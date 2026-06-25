#!/usr/bin/env python3
"""
netlify-logs -- pull per-request access logs for a Netlify site into SQLite.

Modes:
  pull    fetch one time window, insert (deduped) into the database   -- cron
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
import calendar
import hmac
import json
import os
import re
import sys
import time
import sqlite3
import concurrent.futures
import urllib.request
import urllib.error
import urllib.parse
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

API_HOST = os.environ.get("NETLIFY_API_HOST", "https://api.netlify.com")
HERE     = os.path.dirname(os.path.abspath(__file__))
DEFAULT_DB = os.environ.get("NETLIFY_DB", os.path.join(HERE, "logs", "access.db"))
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
        except (OSError, ValueError, KeyError, StopIteration):
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
    except (OSError, ValueError):     # network error or bad JSON: skip enrichment
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


def parse_ts_ms(ts):
    """ISO-8601 UTC timestamp (possibly nanosecond precision) -> epoch milliseconds."""
    m = re.match(r"(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2}):(\d{2})(?:\.(\d+))?", ts or "")
    if not m:
        return 0
    y, mo, d, h, mi, s = (int(m.group(i)) for i in range(1, 7))
    ms = int((m.group(7) or "")[:3].ljust(3, "0")) if m.group(7) else 0
    return calendar.timegm((y, mo, d, h, mi, s, 0, 0, 0)) * 1000 + ms


def db_connect(path):
    """Open (creating if needed) the SQLite store in WAL mode, schema ensured."""
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    conn = sqlite3.connect(path, timeout=30)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=30000")
    conn.execute("CREATE TABLE IF NOT EXISTS requests("
                 "request_id TEXT PRIMARY KEY, ts_ms INTEGER NOT NULL, raw TEXT NOT NULL)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_requests_ts ON requests(ts_ms)")
    conn.execute("CREATE TABLE IF NOT EXISTS settings(key TEXT PRIMARY KEY, value TEXT)")
    return conn


def db_get_setting(conn, key, default=None):
    row = conn.execute("SELECT value FROM settings WHERE key=?", (key,)).fetchone()
    return row[0] if row else default


def db_set_setting(conn, key, value):
    conn.execute("INSERT OR REPLACE INTO settings(key, value) VALUES(?,?)", (key, value))
    conn.commit()


def db_insert(conn, rows):
    """Insert rows, deduped by request_id PK. Returns the number actually inserted."""
    before = conn.total_changes
    conn.executemany(
        "INSERT OR IGNORE INTO requests(request_id, ts_ms, raw) VALUES(?,?,?)",
        [(r["request_id"], parse_ts_ms(r.get("timestamp")),
          json.dumps(r, separators=(",", ":")))
         for r in rows if r.get("request_id")])
    conn.commit()
    return conn.total_changes - before


def db_existing(conn, ids):
    """Subset of ids already in the DB (chunked under SQLite's variable limit)."""
    have, ids = set(), [i for i in ids if i]
    for i in range(0, len(ids), 500):
        chunk = ids[i:i + 500]
        q = "SELECT request_id FROM requests WHERE request_id IN (%s)" % ",".join("?" * len(chunk))
        have.update(r[0] for r in conn.execute(q, chunk).fetchall())
    return have


def do_pull(token, site_id, minutes, db_path, enrich=True):
    """Fetch one window; insert new rows (deduped by request_id) into SQLite."""
    to_ms = int(time.time() * 1000)
    from_ms = to_ms - minutes * 60 * 1000
    rows = fetch_window(token, site_id, from_ms, to_ms)
    rows.sort(key=lambda r: r.get("timestamp", ""))

    conn = db_connect(db_path)
    if enrich:                                    # enrich only rows we don't already have
        have = db_existing(conn, [r["request_id"] for r in rows if r.get("request_id")])
        enrich_rows(token, site_id,
                    [r for r in rows if r.get("request_id") not in have], from_ms, to_ms)
    new = db_insert(conn, rows)
    conn.close()

    return {"window_min": minutes, "seen": len(rows), "new": new,
            "at": time.strftime("%Y-%m-%dT%H:%M:%S%z"), "out": db_path}


def query_rows(db_path, from_ms=None, to_ms=None, limit=2000):
    """Rows in [from_ms, to_ms] (each bound optional), newest first, capped at
    limit. Returns (rows, total_in_range)."""
    if not os.path.exists(db_path):
        return [], 0
    conn = db_connect(db_path)
    where, params = [], []
    if from_ms is not None:
        where.append("ts_ms >= ?"); params.append(int(from_ms))
    if to_ms is not None:
        where.append("ts_ms <= ?"); params.append(int(to_ms))
    clause = ("WHERE " + " AND ".join(where)) if where else ""
    total = conn.execute(f"SELECT COUNT(*) FROM requests {clause}", params).fetchone()[0]
    cur = conn.execute(f"SELECT raw FROM requests {clause} ORDER BY ts_ms DESC LIMIT ?",
                       params + [int(limit)])
    rows = [json.loads(r[0]) for r in cur.fetchall()]
    conn.close()
    return rows, total


def cmd_pull(args):
    summary = do_pull(resolve_token(args.token_file), resolve_site(args.site),
                      args.minutes, args.db, enrich=not args.no_ip)
    if args.quiet and not summary["new"]:
        return
    print(f"{summary['at']}  window={summary['window_min']}m  "
          f"seen={summary['seen']}  new={summary['new']}  -> {summary['out']}")
    if summary["new"]:
        rows, _ = query_rows(args.db, limit=summary["new"])
        by_status, by_path = {}, {}
        for r in rows:
            by_status[r.get("status_code")] = by_status.get(r.get("status_code"), 0) + 1
            path = re.sub(r"^https?://[^/]+", "", r.get("url", "")) or "/"
            by_path[path] = by_path.get(path, 0) + 1
        print("  status  " + "  ".join(f"{k}:{v}" for k, v in sorted(by_status.items(), key=lambda x: -x[1])))
        for path, c in sorted(by_path.items(), key=lambda x: -x[1])[:8]:
            print(f"  {c:>4}  {path[:90]}")


PAGE = """<!doctype html><html><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">

<title>access log</title>
<style>
 html{scrollbar-gutter:stable}      /* always reserve the scrollbar's space so content never shifts */
 body{font:14px/1.4 ui-monospace,Menlo,monospace;margin:0;padding:0 2rem 2rem;background:#0b0e14;color:#cdd6f4}
 #top{position:sticky;top:0;z-index:5;background:#0b0e14;padding:1rem 0 .5rem;border-bottom:1px solid #1e2430}
 #controls{display:flex;flex-wrap:wrap;gap:.4rem .9rem;align-items:center;margin:.55rem 0 .65rem}
 h1{font-size:1.1rem;margin:0}button{font:inherit;padding:.3rem .8rem;background:#89b4fa;color:#0b0e14;border:0;border-radius:6px;cursor:pointer}
 button:disabled{opacity:.5}#msg{color:#a6e3a1;margin-left:auto;text-align:right}
 table{border-collapse:collapse;margin-top:1rem;width:100%;table-layout:fixed}td,th{padding:.2rem .6rem;text-align:left;border-bottom:1px solid #1e2430;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
 th:nth-child(1),td:nth-child(1){width:13.5rem}th:nth-child(2),td:nth-child(2){width:3rem}
 th:nth-child(3),td:nth-child(3){width:4.5rem}th:nth-child(5),td:nth-child(5){width:9.5rem}
 th:nth-child(6),td:nth-child(6){width:4rem}th:nth-child(7),td:nth-child(7){width:8rem}
 th:nth-child(8),td:nth-child(8){width:4rem}th:nth-child(9),td:nth-child(9){width:5rem}
 thead th{position:sticky;top:var(--toph,0);background:#0b0e14;z-index:3;box-shadow:inset 0 -1px 0 #1e2430}
 #toast{position:fixed;bottom:1rem;right:1rem;background:#11151f;border:1px solid #313a4f;color:#a6e3a1;padding:.4rem .8rem;border-radius:8px;font-size:.85rem;opacity:0;transform:translateY(8px);transition:.2s;pointer-events:none;z-index:7}
 #toast.show{opacity:1;transform:none}
 td.url{color:#94e2d5}.s2{color:#a6e3a1}.s3{color:#f9e2af}.s4{color:#fab387}.s5{color:#f38ba8}
 .meta{color:#6c7086}
 #filterbar{display:flex;flex-wrap:wrap;gap:.4rem;align-items:center;margin:0 0 .8rem}
 #filters{display:contents}
 .chip .gr{cursor:grab;opacity:.4;margin-right:.3rem;flex:none;font-size:.8rem}.chip .gr:hover{opacity:.9}
 .chip.dragging{opacity:.15;pointer-events:none}
 .chip .so{width:.85rem;height:.85rem;box-sizing:border-box;border:2px solid currentColor;border-radius:50%;margin-right:.4rem;flex:none;opacity:.55;cursor:pointer;position:relative}
 .chip .so::after{content:"";position:absolute;inset:.18rem;border-radius:50%;background:currentColor}
 .chip .so:hover{opacity:1}
 .chip{padding:.15rem .55rem;border:1px solid #45475a;border-radius:999px;cursor:pointer;color:#bac2de;user-select:none;font-size:.85rem;white-space:nowrap;display:inline-flex;align-items:center}
 .chip:not(.on){border-style:dashed}
 .chip.on{box-shadow:inset 0 0 0 1px currentColor}
 .chip .sw{width:.48rem;height:.48rem;border-radius:50%;margin-left:.4rem;flex:none;cursor:pointer;opacity:.55}.chip .sw:hover{opacity:1}
 .chip .sw:hover{box-shadow:0 0 0 2px #ffffff66}
 .chip .lbl.editing{outline:none;border-bottom:1px dashed currentColor;cursor:text;min-width:1ch}
 .chip .ed{margin-left:.35rem;opacity:.45;cursor:pointer;font-size:.78rem}.chip .ed:hover{opacity:1}
 .chip .x{margin-left:.3rem;opacity:.5;cursor:pointer}.chip .x:hover{opacity:1}
 #addf{font:inherit;background:#11151f;border:1px solid #45475a;border-radius:999px;color:#cdd6f4;padding:.15rem .6rem;width:13rem}
 #tempbar{display:none;flex-wrap:wrap;gap:.4rem;align-items:center;margin:0 0 .6rem}
 .tpill{padding:.12rem .5rem;border:1px dashed #6c7086;border-radius:6px;color:#bac2de;font-size:.82rem;white-space:nowrap}
 .tpill .tx{margin-left:.4rem;opacity:.6;cursor:pointer}.tpill .tx:hover{opacity:1}
 td.fcell{cursor:pointer}td.fcell:hover{background:#313a4f;color:#cdd6f4}
 tbody tr{cursor:pointer}tbody tr:hover{background:#11151f}
 #detail{position:fixed;top:0;right:0;height:100%;width:min(480px,92vw);background:#11151f;border-left:1px solid #1e2430;padding:1rem 1.2rem;overflow:auto;transform:translateX(105%);transition:transform .15s;box-shadow:-8px 0 24px #0008;z-index:6}
 #detail.open{transform:none}
 #detail h2{font-size:.85rem;color:#89b4fa;margin:1rem 0 .3rem;text-transform:uppercase;letter-spacing:.05em}
 #detail dl{display:grid;grid-template-columns:auto 1fr;gap:.15rem .8rem;margin:0}
 #detail dt{color:#6c7086}#detail dd{margin:0;word-break:break-all}
 #detail pre{background:#0b0e14;padding:.6rem;border-radius:6px;overflow:auto;white-space:pre-wrap;word-break:break-all}
 .cls{background:#45475a;color:#cdd6f4;margin-bottom:.5rem}
 #detail .note{background:#0b0e14;border:1px solid #1e2430;border-radius:6px;padding:.5rem .6rem;margin:.2rem 0 .6rem;color:#bac2de;font-size:.82rem;line-height:1.5}
 #mapwrap{margin:0;border:1px solid #1e2430;border-radius:8px;background:#0a1020;overflow:hidden}
 #mapgrip{height:7px;cursor:ns-resize;background:#1e2430;border-top:1px solid #0b0e14}
 #mapgrip:hover{background:#313a4f}
 #scrim{position:fixed;inset:0;background:#0007;display:none;z-index:4}
 #maphdr{padding:.4rem .7rem;font-size:.8rem;border-bottom:1px solid #1e2430;cursor:pointer;user-select:none}
 #maphdr:hover{background:#11151f}
 #mapcaret{cursor:pointer;user-select:none;display:inline-block;width:1rem;text-align:center}
 #cpick{position:absolute;left:-9999px;width:0;height:0;opacity:0}
 #pager{display:none;gap:.7rem;align-items:center;margin:.6rem 0;font-size:.85rem;color:#6c7086}
 #pager button{font:inherit;font-size:.8rem;padding:.1rem .55rem;background:#313a4f;color:#cdd6f4;border:0;border-radius:6px;cursor:pointer}
 #pager button:disabled{opacity:.4;cursor:default}
 #datebar{margin:0;font-size:.85rem;display:inline-flex;flex-wrap:wrap;gap:.4rem;align-items:center}
 #datebar select,#datebar input,#datebar button{font:inherit;font-size:.8rem;background:#11151f;border:1px solid #45475a;border-radius:6px;color:#cdd6f4;padding:.12rem .45rem}
 #datebar #apply{background:#313a4f;cursor:pointer}
 #map{display:block;width:100%;height:340px;background:#0a1020;cursor:grab;touch-action:none}
 #map .land{fill:#1e2430;stroke:#313a4f;stroke-width:.12}
 #map .pin{fill-opacity:.75;stroke:#0b0e14;stroke-width:.5;vector-effect:non-scaling-stroke;cursor:pointer;transition:fill-opacity .18s ease-out,stroke .18s ease-out,stroke-width .18s ease-out}
 #map .pin:hover,#map .pin.ping{fill-opacity:.4;stroke:#fff;stroke-width:.5}   /* keep own color, translucent + outlined, brought to front */
 #map .pin.sel{fill-opacity:.5;stroke:#fff;stroke-width:.85}
 @media (max-width:640px){
  body{padding:0 .6rem}
  #top{position:static;padding:.6rem 0 .4rem}              /* don't pin a tall header on a small screen */
  h1{font-size:1rem}
  #controls{gap:.35rem .6rem}
  #msg{margin-left:0;flex-basis:100%;text-align:left}      /* count on its own line, stable width */
  #twrap{overflow-x:auto;-webkit-overflow-scrolling:touch} /* swipe the table sideways on small screens */
  #t{min-width:760px}
  thead th{position:static}                                /* header sits under the map, not floating */
  #detail{width:94vw}
 }
</style></head><body>
<div id="top">
 <h1>access log<span id="host" class="meta"></span></h1>
 <div id="controls">
  <button id="run" onclick="runNow()">refresh</button>
  <span id="datebar" class="meta">range
   <select id="preset">
    <option value="1">last 1h</option>
    <option value="24" selected>last 24h</option>
    <option value="168">last 7d</option>
    <option value="720">last 30d</option>
    <option value="0">custom</option>
    <option value="-1">all</option>
   </select>
   <input type="datetime-local" id="from"><span id="dash">&ndash;</span><input type="datetime-local" id="to">
   <button id="apply">apply</button>
   <span id="rangemsg"></span>
  </span>
  <span id="msg" class="meta">loading&hellip;</span>
 </div>
 <div id="filterbar"><span id="filters"></span><input id="addf" placeholder="+ url filter, e.g. /blog* &#8629;"></div>
 <div id="tempbar"></div>
 <div id="mapwrap">
  <div id="maphdr" class="meta"><span id="mapcaret">&#9662;</span> map &middot; <span id="mapcount">0 locations</span></div>
  <svg id="map" preserveAspectRatio="xMidYMid meet"></svg>
  <div id="mapgrip" title="drag to resize the map"></div>
 </div>
 <div id="pager"></div>
</div>
<input type="color" id="cpick">
<div id="twrap"><table id="t"><thead><tr><th id="th-time">time</th><th>st</th><th>method</th><th>url</th><th>ip</th><th>country</th><th>region</th><th>ms</th><th>cache</th></tr></thead><tbody></tbody></table></div>
<div id="scrim"></div>
<div id="toast"></div>
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
const PALETTE=['#f38ba8','#fab387','#f9e2af','#a6e3a1','#94e2d5','#89b4fa','#cba6f7','#f5c2e7','#eba0ac','#74c7ec'];
const pickColor=()=>PALETTE[Math.floor(Math.random()*PALETTE.length)];
// url filters live in the DB (global). localStorage is just a fast-paint cache.
let filters=[];
try{const ls=JSON.parse(localStorage.getItem('nf_filters')); if(Array.isArray(ls)) filters=ls;}catch(e){/* no storage */}
let saveT;
function saveF(){                    // cache locally + debounce-save to the server (last write wins)
  try{localStorage.setItem('nf_filters',JSON.stringify(filters));}catch(e){/* no storage */}
  clearTimeout(saveT);
  saveT=setTimeout(()=>fetch(BASE+'/filters',{method:'POST',headers:{'Content-Type':'application/json'},
    body:JSON.stringify(filters)}).catch(()=>{}),300);
}
function normalizeFilters(){         // default fields the server blob may lack
  filters.forEach(f=>{ if(f.on===undefined) f.on=false; if(!f.c) f.c=pickColor(); });
}
normalizeFilters();
async function syncFilters(){        // the server is authoritative; migrate local set up if server is empty
  try{
    const j=await (await fetch(BASE+'/filters')).json();
    if(Array.isArray(j) && j.length) filters=j;
    else if(filters.length) saveF();
  }catch(e){}
  normalizeFilters(); renderChips(); render();
}
let ROWS=[];
let TEMP={};                         // transient facet filters: {field: value} (AND across fields, not saved)

function paintChip(i,el){            // toggle one chip in place (no full re-render, so swipe keeps working)
  filters[i].on=!filters[i].on; PAGE_N=0; SOLOED=-1; saveF();   // manual toggle clears solo memory
  el.classList.toggle('on',filters[i].on);
  el.style.background=filters[i].on?filters[i].c+'3a':'transparent';
  render();
}
function soloChip(i){                // solo = only this on; clicking solo again restores the pre-solo states
  if(SOLOED===i){
    if(PRESOLO) filters.forEach((f,j)=>{ if(j<PRESOLO.length) f.on=PRESOLO[j]; });
    SOLOED=-1; PRESOLO=null;
  } else {
    PRESOLO=filters.map(f=>f.on);   // remember exactly which were on/off before soloing
    filters.forEach((f,j)=>f.on=(j===i));
    SOLOED=i;
  }
  PAGE_N=0; saveF(); renderChips(); render();
}
function reorder(from,to){
  if(from<0||from===to)return;
  const [m]=filters.splice(from,1); filters.splice(to,0,m);
  SOLOED=-1; saveF(); renderChips(); render();
}
function renderChips(){
  PAGE_N=0;                          // any chip change resets to the first page
  const c=document.querySelector('#filters');c.innerHTML='';
  filters.forEach((f,i)=>{
    if(!f.c) f.c=pickColor();
    const el=document.createElement('span');
    el.className='chip'+(f.on?' on':'');
    el.style.borderColor=f.c; el.style.color=f.c;                  // the chip's own color
    el.style.background = f.on ? f.c+'3a' : 'transparent';         // filled when on; dashed border when off
    el.title='click / swipe across to toggle';
    const ctl=(cls,ch,tip)=>{const b=document.createElement('b');b.className=cls;b.textContent=ch;b.title=tip;b.onmousedown=e=>e.stopPropagation();return b;};
    const gr=ctl('gr','\\u2630','drag to reorder');
    gr.onmousedown=e=>{e.stopPropagation();el.draggable=true;};     // only the handle starts a drag
    const sw=document.createElement('b');sw.className='sw';sw.style.background=f.c;sw.title='change color';
    sw.onmousedown=e=>e.stopPropagation(); sw.onclick=e=>{e.stopPropagation();editColor(i);};
    const lbl=document.createElement('span');lbl.className='lbl';lbl.textContent=f.p;
    lbl.onclick=e=>{if(lbl.isContentEditable)e.stopPropagation();};
    const so=ctl('so','','solo (only this)'); so.onclick=e=>{e.stopPropagation();soloChip(i);};   // CSS-drawn ring
    const ed=ctl('ed','\\u270e','rename');           ed.onclick=e=>{e.stopPropagation();editText(i,lbl);};
    const x =ctl('x','\\u00d7','remove');            x.onclick=e=>{e.stopPropagation();filters.splice(i,1);SOLOED=-1;saveF();renderChips();render();};
    el.onmousedown=e=>{if(e.button===0){SWIPE=true;paintChip(i,el);}};   // tap or start of a swipe
    el.onmouseenter=()=>{if(SWIPE)paintChip(i,el);else pingChip(i,true);};  // swipe-paint, or ping its pins
    el.onmouseleave=()=>pingChip(i,false);
    el.onmouseup=()=>{el.draggable=false;};                              // never leave it draggable
    el.draggable=false;
    el.ondragstart=e=>{e.dataTransfer.effectAllowed='move';e.dataTransfer.setData('text','');DRAGI=i;el.classList.add('dragging');};
    el.ondragend=()=>{el.draggable=false;el.classList.remove('dragging');DRAGI=-1;};
    el.ondragover=e=>e.preventDefault();
    el.ondrop=e=>{e.preventDefault();reorder(DRAGI,i);};
    el.append(gr,so,lbl,sw,ed,x);c.appendChild(el);   // solo up front (bigger), color dot after label
  });
}
function editColor(i){             // native picker for one chip; live-preview, then persist
  const inp=document.querySelector('#cpick');
  inp.value=filters[i].c;
  inp.oninput =e=>{filters[i].c=e.target.value;renderChips();};
  inp.onchange=e=>{filters[i].c=e.target.value;saveF();renderChips();render();};
  inp.click();
}
function editText(i,lbl){          // rename the pattern in place (underlined while editing)
  lbl.contentEditable='true'; lbl.classList.add('editing'); lbl.focus();
  const r=document.createRange(); r.selectNodeContents(lbl);
  const s=getSelection(); s.removeAllRanges(); s.addRange(r);
  lbl.onkeydown=e=>{
    if(e.key==='Enter'){e.preventDefault();lbl.blur();}
    if(e.key==='Escape'){lbl.textContent=filters[i].p;lbl.blur();}
  };
  lbl.onblur=()=>{
    const v=lbl.textContent.trim();
    lbl.contentEditable='false'; lbl.classList.remove('editing');
    if(v && v!==filters[i].p){filters[i].p=v;saveF();renderChips();render();}
    else lbl.textContent=filters[i].p;
  };
}
const matchAny=(list,path)=>list.some(f=>globToRe(f.p).test(path));
const PAGE_SIZE=100; let PAGE_N=0;              // table pagination (map/filters still see all)
let SWIPE=false, DRAGI=-1;                      // chip swipe-toggle + drag-reorder state
let SOLOED=-1, PRESOLO=null;                    // solo memory: which chip is soloed + states to restore
let mapH=340; try{const v=+localStorage.getItem('nf_map_h'); if(v)mapH=v;}catch(e){/* no storage */}
let WORLD=null, AGG=[], VIEW={s:1,tx:0,ty:0};   // VIEW persists across redraws
const PIN_R=5.1;                  // base pin radius (counter-scaled by zoom)
let _pinS=0;
function applyView(){
  const g=document.querySelector('#vp');
  if(g) g.setAttribute('transform',`translate(${VIEW.tx} ${VIEW.ty}) scale(${VIEW.s})`);
  if(VIEW.s!==_pinS){           // counter-scale pin radius so dots stay constant on-screen (shrink as you zoom in)
    _pinS=VIEW.s; const r=(PIN_R/VIEW.s).toFixed(2);
    document.querySelectorAll('#map .pin').forEach(p=>p.setAttribute('r',r));
  }
}
function resetView(){VIEW={s:1,tx:0,ty:0};applyView();}   // "fit": back to whole world
function svgPt(svg,ev){          // client px -> viewBox coords (ignores the group transform)
  const p=svg.createSVGPoint(); p.x=ev.clientX; p.y=ev.clientY;
  return p.matrixTransform(svg.getScreenCTM().inverse());
}
function drawMap(rows){          // pins by lat/lon on the vendored equirectangular land
  const svg=document.querySelector('#map'); if(!svg||!WORLD) return;
  const tags=filters.filter(f=>f.on&&f.p).map(f=>({rx:globToRe(f.p),c:f.c}));   // active chip colors
  const colorOf=path=>{for(const t of tags){if(t.rx.test(path))return t.c;}return '';};
  const m=new Map();             // aggregate by rounded location so clusters don't overdraw
  for(const x of rows){
    if(x.latitude==null||x.longitude==null) continue;
    const k=(+x.latitude).toFixed(1)+','+(+x.longitude).toFixed(1);
    let a=m.get(k);
    if(!a){a={key:k,lat:+x.latitude,lon:+x.longitude,n:0,row:x,color:'',
              where:[x.region,x.country].filter(Boolean).join(', ')};m.set(k,a);}
    a.n++;                       // rows are newest-first, so the first seen stays as sample
    if(!a.color){const cc=colorOf(pathOf(x.url)); if(cc)a.color=cc;}   // tint by matching chip
  }
  AGG=[...m.values()];
  svg.setAttribute('viewBox',WORLD.viewBox);
  let h='<g id="vp"><rect x="0" y="0" width="360" height="180" fill="#0a1020"/><path class="land" d="'+WORLD.d+'"/>';
  AGG.forEach((a,i)=>{
    const cx=a.lon+180, cy=90-a.lat, r=PIN_R/VIEW.s;  // uniform, counter-scaled to the zoom; count is in the hover title
    h+=`<circle class="pin" data-i="${i}" data-key="${a.key}" fill="${a.color||'#89b4fa'}" cx="${cx.toFixed(1)}" cy="${cy.toFixed(1)}" r="${r.toFixed(2)}">`+
       `<title>${esc(a.where||'?')} \\u00b7 ${a.n} req</title></circle>`;
  });
  svg.innerHTML=h+'</g>';
  applyView();                   // re-apply current zoom/pan after the rebuild
  document.querySelector('#mapcount').textContent=AGG.length+' location'+(AGG.length===1?'':'s');
}
let SEL='';
const pinByKey=k=>k?document.querySelector('#map .pin[data-key="'+k+'"]'):null;
const toFront=p=>{if(p&&p.parentNode)p.parentNode.appendChild(p);};   // SVG paint order: last = on top
function pingPin(k,on){const p=pinByKey(k);if(!p)return;p.classList.toggle('ping',on);if(on)toFront(p);}
function pingChip(i,on){                           // hovering a chip pings all its matching locations
  if(!filters[i]||!filters[i].p)return;
  const rx=globToRe(filters[i].p), seen=new Set();
  for(const x of ROWS){ if(x.latitude!=null && rx.test(pathOf(x.url))){ const k=locKey(x); if(k&&!seen.has(k)){seen.add(k);pingPin(k,on);} } }
}
function selectPin(k){                            // mark one location selected (persists across redraws)
  SEL = (k===undefined) ? SEL : k;
  document.querySelectorAll('#map .pin.sel').forEach(p=>p.classList.remove('sel'));
  const p=pinByKey(SEL); if(p){p.classList.add('sel');toFront(p);}
}
const locKey=x=>(x.latitude!=null&&x.longitude!=null)?((+x.latitude).toFixed(1)+','+(+x.longitude).toFixed(1)):'';
const FACET={status:x=>''+x.status_code, method:x=>x.method||'', ip:x=>x.client_ip||'',
             country:x=>x.country||'', region:x=>x.region||'', cache:x=>x.edge_cache_result||''};
function toggleFacet(field,val){     // transient click-to-filter; click same value again to clear it
  if(val==='') return;
  if(TEMP[field]===val) delete TEMP[field]; else TEMP[field]=val;   // one value per field, AND across fields
  PAGE_N=0; renderTemp(); render();
}
function renderTemp(){
  const c=document.querySelector('#tempbar'); c.innerHTML='';
  const keys=Object.keys(TEMP);
  c.style.display = keys.length ? 'flex' : 'none';
  keys.forEach(k=>{
    const el=document.createElement('span'); el.className='tpill'; el.append(k+': '+TEMP[k]);
    const x=document.createElement('b'); x.className='tx'; x.textContent='\\u00d7';
    x.onclick=()=>{delete TEMP[k]; PAGE_N=0; renderTemp(); render();};
    el.appendChild(x); c.appendChild(el);
  });
}
function render(){
  const active=filters.filter(f=>f.on && f.p);                 // on url-chips show only their matches (OR)
  const tags=active.map(f=>({rx:globToRe(f.p),c:f.c}));        // on url-chips color the url
  const colorFor=path=>{for(const t of tags){if(t.rx.test(path))return t.c;}return '';};
  const tkeys=Object.keys(TEMP);                               // transient facets AND-narrow on top
  const vis = ROWS.filter(x=>
    (!active.length || matchAny(active,pathOf(x.url))) &&
    (!tkeys.length || tkeys.every(k=>FACET[k](x)===TEMP[k])));
  drawMap(vis); selectPin();                      // map + selection use the FULL filtered set
  const pages=Math.max(1,Math.ceil(vis.length/PAGE_SIZE));
  PAGE_N=Math.min(Math.max(PAGE_N,0),pages-1);    // clamp (data may have shrunk)
  const start=PAGE_N*PAGE_SIZE, pageRows=vis.slice(start,start+PAGE_SIZE);
  const tb=document.querySelector('#t tbody');tb.innerHTML='';
  for(const x of pageRows){                       // render only the current page
    const st=(''+x.status_code),cls='s'+st[0],ms=x.duration_ns?Math.round(x.duration_ns/1e6):'';
    const p=pathOf(x.url), cc=colorFor(p), key=locKey(x), me=esc(x.method||''),
          ipv=esc(x.client_ip||''), co=esc(x.country||''), rg=esc(x.region||''), ca=esc(x.edge_cache_result||'');
    const tr=document.createElement('tr');
    tr.innerHTML=`<td class="meta">${fmtLocal(x.timestamp)}</td>`+
      `<td class="${cls} fcell" data-f="status" data-v="${st}">${st}</td>`+
      `<td class="fcell" data-f="method" data-v="${me}">${me}</td>`+
      `<td class="url"${cc?' style="color:'+cc+'"':''} title="${esc(p)}">${esc(p)}</td>`+
      `<td class="meta fcell" data-f="ip" data-v="${ipv}">${ipv}</td>`+
      `<td class="meta fcell" data-f="country" data-v="${co}">${co}</td>`+
      `<td class="meta fcell" data-f="region" data-v="${rg}" title="${rg}">${rg}</td>`+
      `<td>${ms}</td>`+
      `<td class="meta fcell" data-f="cache" data-v="${ca}">${ca}</td>`;
    if(key) tr.dataset.key=key;
    tr.onmouseenter=()=>pingPin(key,true);
    tr.onmouseleave=()=>pingPin(key,false);
    tr.onclick=e=>{const fc=e.target.closest('.fcell'); if(fc){toggleFacet(fc.dataset.f,fc.dataset.v);} else {showDetail(x);selectPin(key);}};
    tb.appendChild(tr);
  }
  renderPager(vis.length,pages,start);
  document.querySelector('#msg').textContent=vis.length+' / '+ROWS.length+' requests';
}
function setPage(n){PAGE_N=n;render();}
function renderPager(total,pages,start){
  const pg=document.querySelector('#pager');
  if(total<=PAGE_SIZE){pg.style.display='none';return;}
  pg.style.display='flex';pg.innerHTML='';
  const mk=(t,fn,dis)=>{const b=document.createElement('button');b.textContent=t;b.disabled=dis;b.onclick=fn;return b;};
  const lbl=document.createElement('span');
  lbl.textContent=`page ${PAGE_N+1} / ${pages} \\u00b7 rows ${start+1}\\u2013${Math.min(start+PAGE_SIZE,total)} of ${total}`;
  pg.append(mk('\\u2039 prev',()=>setPage(PAGE_N-1),PAGE_N===0), lbl, mk('next \\u203a',()=>setPage(PAGE_N+1),PAGE_N>=pages-1));
}
function showDetail(x,agg){
  const ms=x.duration_ns?(x.duration_ns/1e6).toFixed(1)+' ms':'';
  const rows=[['time ('+TZ+')',fmtLocal(x.timestamp)],['time (UTC)',x.timestamp],
    ['method',x.method],['status',x.status_code],
    ['client ip',x.client_ip],['country',x.country],['region',x.region],
    ['user agent',x.user_agent],['referrer',x.referrer],
    ['url',x.url],['duration',ms],['edge cache',x.edge_cache_result],
    ['durable cache',x.durable_cache_result],['request id',x.request_id],
    ['function',x.has_function_invocation],['edge function',x.has_edge_function_invocation],
    ['image cdn',x.has_image_cdn_invocation],['ai gateway',x.has_ai_gateway_invocation]];
  let h='<button class="cls" onclick="hideDetail()">close \\u00d7</button>';
  if(agg && agg.n>1)                              // opened from a map pin that groups several requests
    h+='<div class="note">most recent of '+agg.n+' requests grouped at this location '+
       '('+esc(agg.where||'')+'), bucketed by lat/lon \\u2248 11&nbsp;km. Showing the latest, from '+esc(x.client_ip||'?')+'.</div>';
  h+='<h2>request</h2><dl>';
  for(const [k,v] of rows) h+='<dt>'+k+'</dt><dd>'+esc(v==null?'':v)+'</dd>';
  h+='</dl>';
  if(x.logs&&x.logs.length) h+='<h2>logs</h2><pre>'+esc(JSON.stringify(x.logs,null,2))+'</pre>';
  h+='<h2>raw</h2><pre>'+esc(JSON.stringify(x,null,2))+'</pre>';
  const d=document.querySelector('#detail');d.innerHTML=h;d.classList.add('open');
  document.querySelector('#scrim').style.display='block';
}
function hideDetail(){
  document.querySelector('#detail').classList.remove('open');
  document.querySelector('#scrim').style.display='none';
  selectPin('');                                  // closing the detail de-highlights the pin
}

function siteHost(){             // derive the site host from the data (no hardcoded domain)
  for(const x of ROWS){ if(x.url){ try{return new URL(x.url).host;}catch(e){/* malformed url, try next */} } }
  return '';
}
function rangeMs(){              // -> {from,to} epoch ms from the preset / custom inputs
  const p=document.querySelector('#preset').value;
  if(p==='-1') return {from:null,to:null};                 // all time
  if(p==='0'){                                             // custom (datetime-local is local tz)
    const f=document.querySelector('#from').value, t=document.querySelector('#to').value;
    return {from:f?new Date(f).getTime():null, to:t?new Date(t).getTime():null};
  }
  return {from:Date.now()-(+p)*3600*1000, to:null};        // last N hours
}
function syncDateInputs(){       // custom inputs only matter for the "custom" preset
  const custom=document.querySelector('#preset').value==='0';
  ['#from','#dash','#to','#apply'].forEach(s=>document.querySelector(s).style.display=custom?'':'none');
}
async function load(){
  const {from,to}=rangeMs();
  const qs=new URLSearchParams({limit:5000});
  if(from!=null) qs.set('from',Math.floor(from));
  if(to!=null)   qs.set('to',Math.floor(to));
  const j=await (await fetch(BASE+'/query?'+qs)).json();
  ROWS=j.rows||[];
  const host=siteHost();
  document.querySelector('#host').textContent=host?' \\u00b7 '+host:'';
  if(host) document.title='access log \\u00b7 '+host;
  document.querySelector('#rangemsg').textContent =
    (j.total>ROWS.length) ? ('\\u00b7 newest '+ROWS.length+' of '+j.total+' \\u2014 narrow the range for more') : '';
  render();
}
let toastT;
function toast(msg){                              // transient note that never reflows the header
  const t=document.querySelector('#toast');t.textContent=msg;t.classList.add('show');
  clearTimeout(toastT);toastT=setTimeout(()=>t.classList.remove('show'),4000);
}
async function runNow(){
  const b=document.querySelector('#run');
  b.disabled=true;toast('pulling\\u2026');
  try{const r=await fetch(BASE+'/run',{method:'POST'});const s=await r.json();
    await load();                                 // load() refreshes the #msg count
    toast(`+${s.new} new \\u00b7 saw ${s.seen} in last ${s.window_min}m`);
  }catch(e){toast('error: '+e);}
  b.disabled=false;
}
document.querySelector('#addf').addEventListener('keydown',e=>{
  if(e.key==='Enter'&&e.target.value.trim()){
    filters.push({p:e.target.value.trim(),on:true,c:pickColor()});e.target.value='';SOLOED=-1;saveF();renderChips();render();
  }
});
document.addEventListener('keydown',e=>{if(e.key==='Escape')hideDetail();});

// --- map zoom (wheel, toward cursor) + pan (drag); "fit" resets ---
const mapSvg=document.querySelector('#map');
let drag=null, moved=false;
function clampView(){            // keep the single world in view -- no panning into empty space, no tiling
  const lx=360*(VIEW.s-1), ly=180*(VIEW.s-1);
  VIEW.tx=Math.min(0,Math.max(-lx,VIEW.tx));
  VIEW.ty=Math.min(0,Math.max(-ly,VIEW.ty));
}
mapSvg.addEventListener('wheel',e=>{
  e.preventDefault();
  const m=svgPt(mapSvg,e);
  const cp={x:(m.x-VIEW.tx)/VIEW.s, y:(m.y-VIEW.ty)/VIEW.s};   // point under cursor, pre-zoom
  const s=Math.min(Math.max(VIEW.s*(e.deltaY<0?1.2:1/1.2),1),40);
  VIEW.tx=m.x-cp.x*s; VIEW.ty=m.y-cp.y*s; VIEW.s=s; clampView(); // keep that point fixed
  applyView();
},{passive:false});
mapSvg.addEventListener('mousedown',e=>{e.preventDefault();drag=svgPt(mapSvg,e);moved=false;mapSvg.style.cursor='grabbing';document.body.style.userSelect='none';});
window.addEventListener('mousemove',e=>{
  if(!drag) return;
  const m=svgPt(mapSvg,e);
  if(Math.abs(m.x-drag.x)+Math.abs(m.y-drag.y)>0.3) moved=true;
  VIEW.tx+=m.x-drag.x; VIEW.ty+=m.y-drag.y; clampView(); drag=m; applyView();
});
window.addEventListener('mouseup',()=>{drag=null;mapSvg.style.cursor='';SWIPE=false;document.body.style.userSelect='';});
mapSvg.addEventListener('click',e=>{
  if(moved){moved=false;return;}                              // a pan-drag is not a pin click
  const c=e.target.closest('.pin'); if(!c) return;
  const a=AGG[+c.dataset.i]; if(a){showDetail(a.row,a);selectPin(a.key);}
});
mapSvg.addEventListener('dblclick',resetView);                // double-click resets zoom/pan
const applyMapH=()=>{mapSvg.style.height=Math.min(mapH,Math.round(innerHeight*0.45))+'px';};  // cap at ~45vh so the map never eats a small screen
(function(){const grip=document.querySelector('#mapgrip');let gy=0,gh=0,on=false;
  grip.addEventListener('mousedown',e=>{e.preventDefault();on=true;gy=e.clientY;gh=mapH;});
  window.addEventListener('mousemove',e=>{if(!on)return;mapH=Math.max(120,Math.min(800,gh+e.clientY-gy));applyMapH();});
  window.addEventListener('mouseup',()=>{if(on){on=false;try{localStorage.setItem('nf_map_h',mapH);}catch(e){/* no storage */}}});
})();
let mapHidden=false; try{mapHidden=localStorage.getItem('nf_map_hidden')==='1';}catch(e){/* no storage */}
function applyMap(){
  const show=!mapHidden;
  mapSvg.style.display=show?'':'none';
  document.querySelector('#mapgrip').style.display=show?'':'none';
  if(show) applyMapH();
  document.querySelector('#mapcaret').textContent=mapHidden?'\\u25b8':'\\u25be';   // > or v
}
document.querySelector('#maphdr').onclick=()=>{mapHidden=!mapHidden;try{localStorage.setItem('nf_map_hidden',mapHidden?'1':'0');}catch(e){/* no storage */}applyMap();};
applyMap();
document.querySelector('#scrim').onclick=hideDetail;          // click outside the detail closes it
// keep the table's sticky column header docked just under the sticky top bar
const topEl=document.querySelector('#top');
const syncTopH=()=>document.documentElement.style.setProperty('--toph',topEl.offsetHeight+'px');
new ResizeObserver(syncTopH).observe(topEl); syncTopH();
document.querySelector('#th-time').textContent='time ('+TZ+')';
document.querySelector('#preset').addEventListener('change',()=>{
  PAGE_N=0; syncDateInputs(); if(document.querySelector('#preset').value!=='0') load();
});
document.querySelector('#apply').addEventListener('click',()=>{PAGE_N=0;load();});
syncDateInputs();
fetch(BASE+'/world').then(r=>r.json()).then(w=>{WORLD=w;render();}).catch(()=>{});
renderChips();renderTemp();syncFilters();load();setInterval(load,30000);
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
            if not self._host_ok():
                return self._send(403, json.dumps(
                    {"error": "unexpected Host header; start with --allow-host <name> if you front this with a proxy"}))
            # Match by suffix so we work at the root or under any mount path,
            # whether or not the proxy strips its prefix.
            path = self.path.split("?")[0].rstrip("/")
            if path.endswith("/query"):
                qs = urllib.parse.parse_qs(self.path.split("?", 1)[1] if "?" in self.path else "")
                def g(k):
                    v = (qs.get(k) or [None])[0]
                    return int(v) if v not in (None, "") else None
                try:
                    limit = min(g("limit") or 2000, 20000)
                    from_ms, to_ms = g("from"), g("to")
                except ValueError:
                    return self._send(400, json.dumps({"error": "from/to/limit must be integers"}))
                rows, total = query_rows(args.db, from_ms, to_ms, limit)
                return self._send(200, json.dumps({"rows": rows, "total": total, "limit": limit}))
            if path.endswith("/filters"):   # the saved url-filter list (global, JSON blob)
                conn = db_connect(args.db)
                val = db_get_setting(conn, "url_filters", "[]")
                conn.close()
                return self._send(200, val)
            if path.endswith("/world"):     # vendored equirectangular land outline
                wp = os.path.join(HERE, "world.json")
                if os.path.exists(wp):
                    with open(wp, "rb") as wf:
                        return self._send(200, wf.read())
                return self._send(404, "{}")
            return self._send(200, PAGE, "text/html; charset=utf-8")

        def do_POST(self):
            if not self._host_ok():
                return self._send(403, json.dumps(
                    {"error": "unexpected Host header; start with --allow-host <name> if you front this with a proxy"}))
            p = self.path.split("?")[0].rstrip("/")
            if p.endswith("/filters"):      # replace the saved url-filter list (last write wins)
                try:
                    length = int(self.headers.get("Content-Length") or 0)
                except ValueError:
                    length = 0
                if length > 1_000_000:
                    return self._send(413, json.dumps({"error": "body too large"}))
                body = self.rfile.read(length).decode("utf-8", "replace") if length else "[]"
                try:
                    json.loads(body)
                except Exception:
                    return self._send(400, json.dumps({"error": "invalid json"}))
                conn = db_connect(args.db); db_set_setting(conn, "url_filters", body); conn.close()
                return self._send(200, json.dumps({"ok": True}))
            if not p.endswith("/run"):
                return self._send(404, "{}")
            if args.run_secret and not hmac.compare_digest(
                    self.headers.get("X-Run-Secret") or "", args.run_secret):
                return self._send(403, json.dumps({"error": "forbidden"}))
            try:
                self._send(200, json.dumps(do_pull(token, site_id, args.minutes,
                                                    args.db, enrich=not args.no_ip)))
            except SystemExit as e:
                self._send(500, json.dumps({"error": str(e)}))
    return H


def cmd_serve(args):
    token = resolve_token(args.token_file)
    site_id = resolve_site(args.site)
    db_connect(args.db).close()          # ensure the store exists
    httpd = ThreadingHTTPServer((args.host, args.port), make_handler(args, token, site_id))
    print(f"serving on http://{args.host}:{args.port}  "
          f"(window {args.minutes}m, db {args.db})", flush=True)
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        pass


def main():
    ap = argparse.ArgumentParser(description="Pull Netlify per-request access logs.")
    sub = ap.add_subparsers(dest="cmd", required=True)

    p = sub.add_parser("pull", help="fetch one window, insert deduped into SQLite")
    p.add_argument("--site", help="Netlify site id (or set NETLIFY_SITE_ID)")
    p.add_argument("--minutes", type=int, default=20)
    p.add_argument("--db", default=DEFAULT_DB)
    p.add_argument("--token-file")
    p.add_argument("--quiet", action="store_true")
    p.add_argument("--no-ip", action="store_true", help="skip per-request IP/geo enrichment")
    p.set_defaults(func=cmd_pull)

    s = sub.add_parser("serve", help="dashboard with a Run-now button")
    s.add_argument("--site", help="Netlify site id (or set NETLIFY_SITE_ID)")
    s.add_argument("--host", default="127.0.0.1", help="bind address")
    s.add_argument("--port", type=int, default=8800)
    s.add_argument("--minutes", type=int, default=20)
    s.add_argument("--db", default=DEFAULT_DB)
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
