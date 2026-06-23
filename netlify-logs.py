#!/usr/bin/env python3
"""
netlify-logs -- pull per-request access logs for a Netlify site into NDJSON.

Modes:
  pull    fetch one time window, append (deduped) to the log file   -- cron
  serve   dashboard with a "Run now" button and a recent-requests view

Auth: install the Netlify CLI and run `netlify login`; the token is read from
~/.config/netlify/config.json. $NETLIFY_AUTH_TOKEN overrides it if set.

Usage:
  ./netlify-logs.py pull --minutes 20
  ./netlify-logs.py serve --host tailscale --port 8800
"""

import argparse
import json
import os
import re
import sys
import time
import fcntl
import subprocess
import urllib.request
import urllib.error
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

SITE_ID  = os.environ.get("NETLIFY_SITE_ID", "00000000-0000-0000-0000-000000000000")
API_HOST = os.environ.get("NETLIFY_API_HOST", "https://api.netlify.com")
HERE     = os.path.dirname(os.path.abspath(__file__))
DEFAULT_OUT = os.environ.get("NETLIFY_LOG_OUT", os.path.join(HERE, "logs", "access.ndjson"))
# Production traffic only; no user-agent filter, so all client types are included.
PROD_FILTER = [{"field": "Branch", "op": "=", "value": "__nf-no-branch-production-traffic"}]


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


def fetch_window(token, from_ms, to_ms, per_page=100, max_pages=200):
    """Paginate the request log for [from_ms, to_ms]. Returns a list of rows."""
    url = (f"{API_HOST}/api/v1/sites/{SITE_ID}/observability/query/lists"
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


def do_pull(token, minutes, out):
    """Fetch one window; append new rows (deduped by request_id) under a lock."""
    to_ms = int(time.time() * 1000)
    from_ms = to_ms - minutes * 60 * 1000
    rows = fetch_window(token, from_ms, to_ms)

    os.makedirs(os.path.dirname(out) or ".", exist_ok=True)
    with open(out + ".lock", "w") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)          # serialize concurrent pulls
        seen = existing_ids(out)
        fresh = [r for r in rows if r.get("request_id") not in seen]
        fresh.sort(key=lambda r: r.get("timestamp", ""))
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
    summary = do_pull(resolve_token(args.token_file), args.minutes, args.out)
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
<title>example.com access log</title>
<style>
 body{font:14px/1.4 ui-monospace,Menlo,monospace;margin:2rem;background:#0b0e14;color:#cdd6f4}
 h1{font-size:1.1rem}button{font:inherit;padding:.5rem 1rem;background:#89b4fa;color:#0b0e14;border:0;border-radius:6px;cursor:pointer}
 button:disabled{opacity:.5}#msg{margin-left:1rem;color:#a6e3a1}
 table{border-collapse:collapse;margin-top:1rem;width:100%}td,th{padding:.2rem .6rem;text-align:left;border-bottom:1px solid #1e2430;white-space:nowrap}
 td.url{white-space:normal;word-break:break-all;color:#94e2d5}.s2{color:#a6e3a1}.s3{color:#f9e2af}.s4{color:#fab387}.s5{color:#f38ba8}
 .meta{color:#6c7086}
</style></head><body>
<h1>example.com &middot; access log</h1>
<button id="run" onclick="runNow()">Run pull now</button>
<span id="msg" class="meta">loading&hellip;</span>
<table id="t"><thead><tr><th>time (UTC)</th><th>st</th><th>method</th><th>url</th><th>ms</th><th>cache</th></tr></thead><tbody></tbody></table>
<script>
async function load(){
  const r=await fetch('recent?n=150'); const rows=await r.json();
  const tb=document.querySelector('#t tbody'); tb.innerHTML='';
  for(const x of rows){
    const st=(''+x.status_code); const cls='s'+st[0];
    const ms=x.duration_ns?Math.round(x.duration_ns/1e6):'';
    const tr=document.createElement('tr');
    tr.innerHTML=`<td class="meta">${(x.timestamp||'').replace('T',' ').slice(0,23)}</td>`+
      `<td class="${cls}">${st}</td><td>${x.method||''}</td>`+
      `<td class="url">${(x.url||'').replace(/[<>&]/g,c=>({'<':'&lt;','>':'&gt;','&':'&amp;'}[c]))}</td>`+
      `<td>${ms}</td><td class="meta">${x.edge_cache_result||''}</td>`;
    tb.appendChild(tr);
  }
  document.querySelector('#msg').textContent=rows.length+' most recent requests';
}
async function runNow(){
  const b=document.querySelector('#run'),m=document.querySelector('#msg');
  b.disabled=true;m.textContent='pulling\\u2026';
  try{const r=await fetch('run',{method:'POST'});const s=await r.json();
    m.textContent=`+${s.new} new (saw ${s.seen} in last ${s.window_min}m) @ ${s.at}`;
    await load();
  }catch(e){m.textContent='error: '+e;}
  b.disabled=false;
}
load(); setInterval(load, 30000);
</script></body></html>"""


def make_handler(args, token):
    class H(BaseHTTPRequestHandler):
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
            path = self.path.split("?")[0]
            if path in ("/", "/index.html"):
                return self._send(200, PAGE, "text/html; charset=utf-8")
            if path == "/recent":
                m = re.search(r"[?&]n=(\d+)", self.path)
                n = min(int(m.group(1)) if m else 150, 1000)
                return self._send(200, json.dumps(tail_rows(args.out, n)))
            self._send(404, "{}")

        def do_POST(self):
            if self.path.split("?")[0] != "/run":
                return self._send(404, "{}")
            if args.run_secret and self.headers.get("X-Run-Secret") != args.run_secret:
                return self._send(403, json.dumps({"error": "forbidden"}))
            try:
                self._send(200, json.dumps(do_pull(token, args.minutes, args.out)))
            except SystemExit as e:
                self._send(500, json.dumps({"error": str(e)}))
    return H


def resolve_host(host):
    """`--host tailscale` -> bind to the tailscale0 IPv4."""
    if host != "tailscale":
        return host
    try:
        out = subprocess.run(["tailscale", "ip", "-4"], capture_output=True, text=True, timeout=5)
        ip = (out.stdout or "").strip().splitlines()
        if ip and ip[0]:
            return ip[0].strip()
    except Exception:
        pass
    try:
        out = subprocess.run(["ip", "-4", "-o", "addr", "show", "tailscale0"],
                             capture_output=True, text=True, timeout=5)
        m = re.search(r"inet\s+([\d.]+)", out.stdout)
        if m:
            return m.group(1)
    except Exception:
        pass
    sys.exit("--host tailscale: no Tailscale IP found. Is Tailscale up?")


def cmd_serve(args):
    token = resolve_token(args.token_file)
    args.host = resolve_host(args.host)
    httpd = ThreadingHTTPServer((args.host, args.port), make_handler(args, token))
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
    p.add_argument("--minutes", type=int, default=20)
    p.add_argument("--out", default=DEFAULT_OUT)
    p.add_argument("--token-file")
    p.add_argument("--quiet", action="store_true")
    p.set_defaults(func=cmd_pull)

    s = sub.add_parser("serve", help="dashboard with a Run-now button")
    s.add_argument("--host", default="0.0.0.0", help="bind address, or 'tailscale'")
    s.add_argument("--port", type=int, default=8800)
    s.add_argument("--minutes", type=int, default=20)
    s.add_argument("--out", default=DEFAULT_OUT)
    s.add_argument("--token-file")
    s.add_argument("--run-secret", default=os.environ.get("RUN_SECRET"))
    s.add_argument("--allow-host", action="append", default=[], metavar="NAME",
                   help="extra Host header to accept (repeatable), for proxies "
                        "that keep their own hostname, e.g. tailscale serve")
    s.set_defaults(func=cmd_serve)

    args = ap.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
