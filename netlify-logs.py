#!/usr/bin/env python3
"""
netlify-logs -- pull per-request access logs for a Netlify site into NDJSON.

Modes:
  pull    fetch one time window, append (deduped) to the log file   -- cron
  serve   dashboard with a "Run now" button and a recent-requests view

Auth: install the Netlify CLI and run `netlify login`; the token is read from
~/.config/netlify/config.json. $NETLIFY_AUTH_TOKEN overrides it if set.

The dashboard binds to localhost only. To reach it from elsewhere, front it
with whatever you already use (e.g. `tailscale serve`, an SSH tunnel, a reverse
proxy) -- that stays out of this tool.

Usage:
  NETLIFY_SITE_ID=<your-site-id> ./netlify-logs.py pull --minutes 20
  ./netlify-logs.py serve --site <your-site-id> --port 8800
"""

import argparse
import json
import os
import re
import sys
import time
import fcntl
import concurrent.futures
import urllib.request
import urllib.error
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

API_HOST = os.environ.get("NETLIFY_API_HOST", "https://api.netlify.com")
HERE     = os.path.dirname(os.path.abspath(__file__))
DEFAULT_OUT = os.environ.get("NETLIFY_LOG_OUT", os.path.join(HERE, "logs", "access.ndjson"))
# Production traffic only; no user-agent filter, so all client types are included.
PROD_BRANCH = "__nf-no-branch-production-traffic"
PROD_FILTER = [{"field": "Branch", "op": "=", "value": PROD_BRANCH}]


def resolve_token(token_file=None):
    tok = os.environ.get("NETLIFY_AUTH_TOKEN")
    if tok:
        return tok.strip()
    if token_file:
        with open(token_file) as f:
            return f.read().strip()
    cfg = os.path.expanduser("~/.config/netlify/config.json")
    if os.path.exists(cfg):
        try:
            d = json.load(open(cfg))
            return next(iter(d["users"].values()))["auth"]["token"].strip()
        except Exception:
            pass
    sys.exit("no Netlify auth found. Install the Netlify CLI and run `netlify login`.")


def resolve_site(site=None):
    site = site or os.environ.get("NETLIFY_SITE_ID")
    if not site:
        sys.exit("no site id. Pass --site or set NETLIFY_SITE_ID "
                 "(find it under Site configuration -> Site information).")
    return site.strip()


def fetch_window(token, site_id, from_ms, to_ms, per_page=100, max_pages=200):
    """Paginate the request log for [from_ms, to_ms]. Returns a list of rows."""
    url = (f"{API_HOST}/api/v1/sites/{site_id}/observability/query/lists"
           f"?from_ts={from_ms}&to_ts={to_ms}")
    all_rows, page = [], 1
    while page <= max_pages:
        body = {"data": [{"attributes": {"queries": [
            {"name": "edge_requests_logs", "filters": PROD_FILTER,
             "page": page, "per_page": per_page}]}}]}
        req = urllib.request.Request(
            url, data=json.dumps(body).encode(), method="POST",
            headers={"Authorization": f"Bearer {token}",
                     "Content-Type": "application/json",
                     "User-Agent": "netlify-logs/1.0",  # the default urllib UA is rejected
                     "Accept": "application/json"})
        try:
            with urllib.request.urlopen(req, timeout=30) as r:
                j = json.load(r)
        except urllib.error.HTTPError as e:
            if e.code in (401, 403):
                raise SystemExit(f"auth failed (HTTP {e.code}); re-run `netlify login`.")
            raise SystemExit(f"HTTP {e.code}: {e.read()[:200]!r}")
        rows = (j.get("data") or [{}])[0].get("attributes", {}).get("list", []) or []
        all_rows.extend(rows)
        meta = (j.get("meta", {}).get("responses") or [{}])[0]
        if not meta.get("has_next_page"):
            break
        page += 1
    return all_rows


def fetch_detail(token, site_id, request_id, from_ms, to_ms):
    """Per-request detail: client IP, geo, user agent, size. Returns dict or None.

    The list endpoint omits these fields; do_pull calls this once per new request
    to enrich each row before it is written.
    """
    url = (f"{API_HOST}/api/v1/sites/{site_id}/observability/requests/{request_id}"
           f"?from_ts={from_ms}&to_ts={to_ms}&branch={PROD_BRANCH}")
    req = urllib.request.Request(url, headers={
        "Authorization": f"Bearer {token}",
        "User-Agent": "netlify-logs/1.0", "Accept": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=30) as r:
            return json.load(r)
    except Exception:
        return None


def enrich_rows(token, site_id, rows, from_ms, to_ms, workers=8):
    """Add client IP / geo / user agent to each row in place (one detail call per row)."""
    if not rows:
        return

    def one(row):
        d = fetch_detail(token, site_id, row.get("request_id", ""), from_ms, to_ms)
        if not d:
            return
        c = d.get("client") or {}
        row["client_ip"]       = c.get("address")
        row["country"]         = c.get("country")
        row["region"]          = c.get("region")
        row["latitude"]        = c.get("latitude")
        row["longitude"]       = c.get("longitude")
        row["user_agent"]      = c.get("user_agent")
        row["referrer"]        = c.get("referrer")
        row["content_type"]    = d.get("content_type")
        row["response_size_b"] = d.get("response_size_b")

    with concurrent.futures.ThreadPoolExecutor(max_workers=min(workers, len(rows))) as ex:
        list(ex.map(one, rows))


def existing_ids(path):
    seen = set()
    if os.path.exists(path):
        with open(path) as f:
            for line in f:
                try:
                    seen.add(json.loads(line)["request_id"])
                except Exception:
                    pass
    return seen


def do_pull(token, site_id, minutes, out, enrich=True):
    """Fetch one window; append new rows (deduped by request_id) under a lock."""
    to_ms = int(time.time() * 1000)
    from_ms = to_ms - minutes * 60 * 1000
    rows = fetch_window(token, site_id, from_ms, to_ms)

    os.makedirs(os.path.dirname(out) or ".", exist_ok=True)
    with open(out + ".lock", "w") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)          # serialize concurrent pulls
        seen = existing_ids(out)
        fresh = [r for r in rows if r.get("request_id") not in seen]
        fresh.sort(key=lambda r: r.get("timestamp", ""))
        if enrich:                                # add client IP / geo to new rows
            enrich_rows(token, site_id, fresh, from_ms, to_ms)
        with open(out, "a") as f:
            for r in fresh:
                f.write(json.dumps(r, separators=(",", ":")) + "\n")
        fcntl.flock(lock, fcntl.LOCK_UN)

    return {"window_min": minutes, "seen": len(rows), "new": len(fresh),
            "at": time.strftime("%Y-%m-%dT%H:%M:%S%z"), "out": out}


def tail_rows(path, n):
    """Last n rows, newest first, reading only the tail of the file."""
    if not os.path.exists(path):
        return []
    with open(path, "rb") as f:
        f.seek(0, os.SEEK_END)
        size = f.tell()
        f.seek(size - min(size, 256 * 1024))
        data = f.read().decode("utf-8", "replace")
    lines = [ln for ln in data.splitlines() if ln.strip()]
    out = []
    for ln in lines[-n:]:
        try:
            out.append(json.loads(ln))
        except Exception:
            pass
    out.reverse()
    return out


def cmd_pull(args):
    summary = do_pull(resolve_token(args.token_file), resolve_site(args.site),
                      args.minutes, args.out, enrich=not args.no_ip)
    if args.quiet and not summary["new"]:
        return
    print(f"{summary['at']}  window={summary['window_min']}m  "
          f"seen={summary['seen']}  new={summary['new']}  -> {summary['out']}")
    if summary["new"]:
        rows = tail_rows(args.out, summary["new"])
        by_status, by_path = {}, {}
        for r in rows:
            by_status[r.get("status_code")] = by_status.get(r.get("status_code"), 0) + 1
            path = re.sub(r"^https?://[^/]+", "", r.get("url", "")) or "/"
            by_path[path] = by_path.get(path, 0) + 1
        print("  status  " + "  ".join(f"{k}:{v}" for k, v in sorted(by_status.items(), key=lambda x: -x[1])))
        for path, c in sorted(by_path.items(), key=lambda x: -x[1])[:8]:
            print(f"  {c:>4}  {path[:90]}")


PAGE = """<!doctype html><html><head><meta charset="utf-8">
<title>access log</title>
<style>
 body{font:14px/1.4 ui-monospace,Menlo,monospace;margin:2rem;background:#0b0e14;color:#cdd6f4}
 h1{font-size:1.1rem}button{font:inherit;padding:.5rem 1rem;background:#89b4fa;color:#0b0e14;border:0;border-radius:6px;cursor:pointer}
 button:disabled{opacity:.5}#msg{margin-left:1rem;color:#a6e3a1}
 table{border-collapse:collapse;margin-top:1rem;width:100%}td,th{padding:.2rem .6rem;text-align:left;border-bottom:1px solid #1e2430;white-space:nowrap}
 td.url{white-space:normal;word-break:break-all;color:#94e2d5}.s2{color:#a6e3a1}.s3{color:#f9e2af}.s4{color:#fab387}.s5{color:#f38ba8}
 .meta{color:#6c7086}
 #filterbar{display:flex;flex-wrap:wrap;gap:.4rem;align-items:center;margin:1rem 0 .3rem}
 #filters{display:flex;flex-wrap:wrap;gap:.4rem}
 .chip{padding:.15rem .55rem;border:1px solid #45475a;border-radius:999px;cursor:pointer;color:#bac2de;user-select:none;font-size:.85rem;white-space:nowrap}
 .chip.only{border-color:#a6e3a1;color:#a6e3a1}
 .chip.hide{border-color:#f38ba8;color:#f38ba8;text-decoration:line-through}
 .chip .x{margin-left:.4rem;opacity:.55}.chip .x:hover{opacity:1}
 #addf{font:inherit;background:#11151f;border:1px solid #45475a;border-radius:999px;color:#cdd6f4;padding:.15rem .6rem;width:13rem}
 tbody tr{cursor:pointer}tbody tr:hover{background:#11151f}
 #detail{position:fixed;top:0;right:0;height:100%;width:min(480px,92vw);background:#11151f;border-left:1px solid #1e2430;padding:1rem 1.2rem;overflow:auto;transform:translateX(105%);transition:transform .15s;box-shadow:-8px 0 24px #0008}
 #detail.open{transform:none}
 #detail h2{font-size:.85rem;color:#89b4fa;margin:1rem 0 .3rem;text-transform:uppercase;letter-spacing:.05em}
 #detail dl{display:grid;grid-template-columns:auto 1fr;gap:.15rem .8rem;margin:0}
 #detail dt{color:#6c7086}#detail dd{margin:0;word-break:break-all}
 #detail pre{background:#0b0e14;padding:.6rem;border-radius:6px;overflow:auto;white-space:pre-wrap;word-break:break-all}
 .cls{background:#45475a;color:#cdd6f4;margin-bottom:.5rem}
 #mapwrap{margin:1rem 0;border:1px solid #1e2430;border-radius:8px;background:#0a1020;overflow:hidden}
 #maphdr{padding:.4rem .7rem;font-size:.8rem;border-bottom:1px solid #1e2430}
 #fit{padding:.05rem .55rem;font-size:.75rem;margin-left:.4rem;background:#313a4f;color:#cdd6f4}
 #map{display:block;width:100%;height:auto;background:#0a1020;cursor:grab;touch-action:none}
 #map .land{fill:#1e2430;stroke:#313a4f;stroke-width:.12}
 #map .pin{fill:#89b4fa;fill-opacity:.65;stroke:#0b0e14;stroke-width:.12;cursor:pointer}
 #map .pin:hover{fill:#f9e2af;fill-opacity:.95}
</style></head><body>
<h1>access log<span id="host" class="meta"></span></h1>
<button id="run" onclick="runNow()">Run pull now</button>
<span id="msg" class="meta">loading&hellip;</span>
<div id="filterbar">
  <span id="filters"></span>
  <input id="addf" placeholder="+ filter, e.g. /blog* &#8629;">
</div>
<div class="meta" style="font-size:.8rem">click a chip to cycle off &rarr; only &rarr; hide &middot; click a row for full detail</div>
<div id="mapwrap"><div id="maphdr" class="meta">map &middot; <span id="mapcount">0 locations</span> &middot; scroll to zoom, drag to pan<button id="fit">fit</button></div><svg id="map" preserveAspectRatio="xMidYMid meet"></svg></div>
<table id="t"><thead><tr><th id="th-time">time</th><th>st</th><th>method</th><th>url</th><th>ip</th><th>ms</th><th>cache</th></tr></thead><tbody></tbody></table>
<div id="detail"></div>
<script>
// Works at the site root or under any mount path (e.g. a reverse-proxy /logs).
const BASE=location.pathname.replace(/\\/+$/,'');
const esc=s=>(''+s).replace(/[<>&"']/g,c=>({'<':'&lt;','>':'&gt;','&':'&amp;','"':'&quot;',"'":'&#39;'}[c]));
const pathOf=u=>(u||'').replace(/^https?:\\/\\/[^/]+/,'')||'/';
const TZ=Intl.DateTimeFormat().resolvedOptions().timeZone||'local';
function fmtLocal(ts){            // UTC ISO -> local wall-clock, no tz math by hand
  if(!ts) return '';
  const d=new Date(ts);
  if(isNaN(d)) return (''+ts).replace('T',' ').slice(0,23);
  const p=n=>(''+n).padStart(2,'0');
  return d.getFullYear()+'-'+p(d.getMonth()+1)+'-'+p(d.getDate())+' '+
         p(d.getHours())+':'+p(d.getMinutes())+':'+p(d.getSeconds())+'.'+
         (''+d.getMilliseconds()).padStart(3,'0');
}
function globToRe(p){          // substring glob: * -> .*, metachars escaped
  let s='';
  for(const ch of p){
    if(ch==='*') s+='.*';
    else if('.+?^${}()|[]\\\\'.includes(ch)) s+='\\\\'+ch;
    else s+=ch;
  }
  return new RegExp(s,'i');
}
const DEF=[];   // no seeded filters; add your own via the box (saved per browser)
let filters;
try{filters=JSON.parse(localStorage.getItem('nf_filters'))||DEF;}catch(e){filters=DEF;}
const saveF=()=>{try{localStorage.setItem('nf_filters',JSON.stringify(filters));}catch(e){}};
let ROWS=[];

function renderChips(){
  const c=document.querySelector('#filters');c.innerHTML='';
  filters.forEach((f,i)=>{
    const el=document.createElement('span');
    el.className='chip '+f.m;el.textContent=f.p;
    el.title='click: off \\u2192 only \\u2192 hide';
    el.onclick=()=>{f.m={off:'only',only:'hide',hide:'off'}[f.m];saveF();renderChips();render();};
    const x=document.createElement('b');x.textContent='\\u00d7';x.className='x';
    x.onclick=e=>{e.stopPropagation();filters.splice(i,1);saveF();renderChips();render();};
    el.appendChild(x);c.appendChild(el);
  });
}
const matchAny=(list,path)=>list.some(f=>globToRe(f.p).test(path));
let WORLD=null, AGG=[], VIEW={s:1,tx:0,ty:0};   // VIEW persists across redraws
function applyView(){
  const g=document.querySelector('#vp');
  if(g) g.setAttribute('transform',`translate(${VIEW.tx} ${VIEW.ty}) scale(${VIEW.s})`);
}
function resetView(){VIEW={s:1,tx:0,ty:0};applyView();}   // "fit": back to whole world
function svgPt(svg,ev){          // client px -> viewBox coords (ignores the group transform)
  const p=svg.createSVGPoint(); p.x=ev.clientX; p.y=ev.clientY;
  return p.matrixTransform(svg.getScreenCTM().inverse());
}
function drawMap(rows){          // pins by lat/lon on the vendored equirectangular land
  const svg=document.querySelector('#map'); if(!svg||!WORLD) return;
  const m=new Map();             // aggregate by rounded location so clusters don't overdraw
  for(const x of rows){
    if(x.latitude==null||x.longitude==null) continue;
    const k=(+x.latitude).toFixed(1)+','+(+x.longitude).toFixed(1);
    let a=m.get(k);
    if(!a){a={lat:+x.latitude,lon:+x.longitude,n:0,row:x,
              where:[x.region,x.country].filter(Boolean).join(', ')};m.set(k,a);}
    a.n++;                       // rows are newest-first, so the first seen stays as sample
  }
  AGG=[...m.values()];
  svg.setAttribute('viewBox',WORLD.viewBox);
  let h='<g id="vp"><rect x="0" y="0" width="360" height="180" fill="#0a1020"/><path class="land" d="'+WORLD.d+'"/>';
  AGG.forEach((a,i)=>{
    const cx=a.lon+180, cy=90-a.lat, r=Math.min(0.8+Math.sqrt(a.n),5);
    h+=`<circle class="pin" data-i="${i}" cx="${cx.toFixed(1)}" cy="${cy.toFixed(1)}" r="${r.toFixed(1)}">`+
       `<title>${esc(a.where||'?')} \\u00b7 ${a.n} req</title></circle>`;
  });
  svg.innerHTML=h+'</g>';
  applyView();                   // re-apply current zoom/pan after the rebuild
  document.querySelector('#mapcount').textContent=AGG.length+' location'+(AGG.length===1?'':'s');
}
function render(){
  const onlies=filters.filter(f=>f.m==='only'),hides=filters.filter(f=>f.m==='hide');
  const vis=ROWS.filter(x=>{
    const path=pathOf(x.url);
    if(onlies.length && !matchAny(onlies,path)) return false;
    if(hides.length && matchAny(hides,path)) return false;
    return true;
  });
  const tb=document.querySelector('#t tbody');tb.innerHTML='';
  for(const x of vis){
    const st=(''+x.status_code),cls='s'+st[0],ms=x.duration_ns?Math.round(x.duration_ns/1e6):'';
    const tr=document.createElement('tr');
    tr.innerHTML=`<td class="meta">${fmtLocal(x.timestamp)}</td>`+
      `<td class="${cls}">${st}</td><td>${esc(x.method||'')}</td>`+
      `<td class="url">${esc(pathOf(x.url))}</td>`+
      `<td class="meta">${esc(x.client_ip||'')}${x.country?' '+esc(x.country):''}</td>`+
      `<td>${ms}</td><td class="meta">${esc(x.edge_cache_result||'')}</td>`;
    tr.onclick=()=>showDetail(x);
    tb.appendChild(tr);
  }
  drawMap(vis);
  const f=onlies.length||hides.length?' (filtered)':'';
  document.querySelector('#msg').textContent=vis.length+' / '+ROWS.length+' requests'+f;
}
function showDetail(x){
  const ms=x.duration_ns?(x.duration_ns/1e6).toFixed(1)+' ms':'';
  const rows=[['time ('+TZ+')',fmtLocal(x.timestamp)],['time (UTC)',x.timestamp],
    ['method',x.method],['status',x.status_code],
    ['client ip',x.client_ip],['country',x.country],['region',x.region],
    ['user agent',x.user_agent],['referrer',x.referrer],
    ['url',x.url],['duration',ms],['edge cache',x.edge_cache_result],
    ['durable cache',x.durable_cache_result],['request id',x.request_id],
    ['function',x.has_function_invocation],['edge function',x.has_edge_function_invocation],
    ['image cdn',x.has_image_cdn_invocation],['ai gateway',x.has_ai_gateway_invocation]];
  let h='<button class="cls" onclick="hideDetail()">close \\u00d7</button><h2>request</h2><dl>';
  for(const [k,v] of rows) h+='<dt>'+k+'</dt><dd>'+esc(v==null?'':v)+'</dd>';
  h+='</dl>';
  if(x.logs&&x.logs.length) h+='<h2>logs</h2><pre>'+esc(JSON.stringify(x.logs,null,2))+'</pre>';
  h+='<h2>raw</h2><pre>'+esc(JSON.stringify(x,null,2))+'</pre>';
  const d=document.querySelector('#detail');d.innerHTML=h;d.classList.add('open');
}
const hideDetail=()=>document.querySelector('#detail').classList.remove('open');

function siteHost(){             // derive the site host from the data (no hardcoded domain)
  for(const x of ROWS){ if(x.url){ try{return new URL(x.url).host;}catch(e){} } }
  return '';
}
async function load(){
  const r=await fetch(BASE+'/recent?n=500');ROWS=await r.json();
  const host=siteHost();
  document.querySelector('#host').textContent=host?' \\u00b7 '+host:'';
  if(host) document.title='access log \\u00b7 '+host;
  render();
}
async function runNow(){
  const b=document.querySelector('#run'),m=document.querySelector('#msg');
  b.disabled=true;m.textContent='pulling\\u2026';
  try{const r=await fetch(BASE+'/run',{method:'POST'});const s=await r.json();
    await load();
    m.textContent=`+${s.new} new (saw ${s.seen} in last ${s.window_min}m) @ ${s.at}`;
  }catch(e){m.textContent='error: '+e;}
  b.disabled=false;
}
document.querySelector('#addf').addEventListener('keydown',e=>{
  if(e.key==='Enter'&&e.target.value.trim()){
    filters.push({p:e.target.value.trim(),m:'only'});e.target.value='';saveF();renderChips();render();
  }
});
document.addEventListener('keydown',e=>{if(e.key==='Escape')hideDetail();});

// --- map zoom (wheel, toward cursor) + pan (drag); "fit" resets ---
const mapSvg=document.querySelector('#map');
let drag=null, moved=false;
mapSvg.addEventListener('wheel',e=>{
  e.preventDefault();
  const m=svgPt(mapSvg,e);
  const cp={x:(m.x-VIEW.tx)/VIEW.s, y:(m.y-VIEW.ty)/VIEW.s};   // point under cursor, pre-zoom
  const s=Math.min(Math.max(VIEW.s*(e.deltaY<0?1.2:1/1.2),1),40);
  VIEW.tx=m.x-cp.x*s; VIEW.ty=m.y-cp.y*s; VIEW.s=s;            // keep that point fixed
  applyView();
},{passive:false});
mapSvg.addEventListener('mousedown',e=>{drag=svgPt(mapSvg,e);moved=false;mapSvg.style.cursor='grabbing';});
window.addEventListener('mousemove',e=>{
  if(!drag) return;
  const m=svgPt(mapSvg,e);
  if(Math.abs(m.x-drag.x)+Math.abs(m.y-drag.y)>0.3) moved=true;
  VIEW.tx+=m.x-drag.x; VIEW.ty+=m.y-drag.y; drag=m; applyView();
});
window.addEventListener('mouseup',()=>{drag=null;mapSvg.style.cursor='';});
mapSvg.addEventListener('click',e=>{
  if(moved){moved=false;return;}                              // a pan-drag is not a pin click
  const c=e.target.closest('.pin'); if(!c) return;
  const a=AGG[+c.dataset.i]; if(a) showDetail(a.row);
});
document.querySelector('#fit').onclick=resetView;
document.querySelector('#th-time').textContent='time ('+TZ+')';
fetch(BASE+'/world').then(r=>r.json()).then(w=>{WORLD=w;render();}).catch(()=>{});
renderChips();load();setInterval(load,30000);
</script></body></html>"""


def make_handler(args, token, site_id):
    # DNS-rebinding / CSRF guard: only answer requests addressed to us. A page
    # in your browser can reach 127.0.0.1, but not with one of these Hosts.
    allowed_hosts = {"localhost", "127.0.0.1", "::1", args.host}
    allowed_hosts.update(h.strip() for h in args.allow_host)
    any_host = args.host in ("0.0.0.0", "::")    # bound to everything on purpose

    class H(BaseHTTPRequestHandler):
        def _host_ok(self):
            if any_host:
                return True
            host = self.headers.get("Host") or ""
            if host.startswith("["):             # [::1]:8800
                host = host[1:].split("]")[0]
            else:
                host = host.rsplit(":", 1)[0]
            return host in allowed_hosts

        def _send(self, code, body, ctype="application/json"):
            b = body.encode() if isinstance(body, str) else body
            self.send_response(code)
            self.send_header("Content-Type", ctype)
            self.send_header("Content-Length", str(len(b)))
            self.end_headers()
            self.wfile.write(b)

        def log_message(self, *a):
            pass

        def do_GET(self):
            # Match by suffix so we work at the root or under any mount path,
            # whether or not the proxy strips its prefix.
            path = self.path.split("?")[0].rstrip("/")
            if path.endswith("/recent"):
                m = re.search(r"[?&]n=(\d+)", self.path)
                n = min(int(m.group(1)) if m else 150, 1000)
                return self._send(200, json.dumps(tail_rows(args.out, n)))
            if path.endswith("/world"):     # vendored equirectangular land outline
                wp = os.path.join(HERE, "world.json")
                if os.path.exists(wp):
                    with open(wp, "rb") as wf:
                        return self._send(200, wf.read())
                return self._send(404, "{}")
            return self._send(200, PAGE, "text/html; charset=utf-8")

        def do_POST(self):
            if not self.path.split("?")[0].rstrip("/").endswith("/run"):
                return self._send(404, "{}")
            if args.run_secret and self.headers.get("X-Run-Secret") != args.run_secret:
                return self._send(403, json.dumps({"error": "forbidden"}))
            try:
                self._send(200, json.dumps(do_pull(token, site_id, args.minutes,
                                                    args.out, enrich=not args.no_ip)))
            except SystemExit as e:
                self._send(500, json.dumps({"error": str(e)}))
    return H


def cmd_serve(args):
    token = resolve_token(args.token_file)
    site_id = resolve_site(args.site)
    httpd = ThreadingHTTPServer((args.host, args.port), make_handler(args, token, site_id))
    print(f"serving on http://{args.host}:{args.port}  "
          f"(window {args.minutes}m, out {args.out})", flush=True)
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        pass


def main():
    ap = argparse.ArgumentParser(description="Pull Netlify per-request access logs.")
    sub = ap.add_subparsers(dest="cmd", required=True)

    p = sub.add_parser("pull", help="fetch one window, append deduped NDJSON")
    p.add_argument("--site", help="Netlify site id (or set NETLIFY_SITE_ID)")
    p.add_argument("--minutes", type=int, default=20)
    p.add_argument("--out", default=DEFAULT_OUT)
    p.add_argument("--token-file")
    p.add_argument("--quiet", action="store_true")
    p.add_argument("--no-ip", action="store_true", help="skip per-request IP/geo enrichment")
    p.set_defaults(func=cmd_pull)

    s = sub.add_parser("serve", help="dashboard with a Run-now button")
    s.add_argument("--site", help="Netlify site id (or set NETLIFY_SITE_ID)")
    s.add_argument("--host", default="127.0.0.1", help="bind address")
    s.add_argument("--port", type=int, default=8800)
    s.add_argument("--minutes", type=int, default=20)
    s.add_argument("--out", default=DEFAULT_OUT)
    s.add_argument("--token-file")
    s.add_argument("--run-secret", default=os.environ.get("RUN_SECRET"))
    s.add_argument("--allow-host", action="append", default=[], metavar="NAME",
                   help="extra Host header to accept (repeatable), for proxies "
                        "that keep their own hostname, e.g. tailscale serve")
    s.add_argument("--no-ip", action="store_true", help="skip per-request IP/geo enrichment")
    s.set_defaults(func=cmd_serve)

    args = ap.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
