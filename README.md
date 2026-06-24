# netlify-logs

**Pay Netlify.** Their observability, log drains, and analytics are the real
product — if you depend on this, give them money.

This repo is *not* meant to replace any of that. It's a small, stdlib-only thing
to pull your own per-request access logs and keep them locally as NDJSON, plus a
dashboard to look at them. That's it. Run `./netlify-logs.py -h` (and
`pull -h` / `serve -h`) for the full set of options.

## Setup

```sh
npm install -g netlify-cli   # or use: npx netlify-cli
netlify login
export NETLIFY_SITE_ID=<your-site-id>   # Site configuration -> Site information
./install.sh
```

`install.sh` adds two user systemd units: a pull every 15 minutes, and the
dashboard on `127.0.0.1:8800`. Logs land in `logs/access.ndjson` — one request
per line, append-only, deduped by `request_id`.

## Dashboard

- **World map** of request origins (zoom, pan, `fit`).
- **Entry detail** drawer — click any row or pin for the full record.
- **Filter chips** — click to cycle off → only → hide; add your own URL globs.
- **Run now** — pull on demand and refresh.
