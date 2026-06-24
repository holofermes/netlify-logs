# netlify-logs

A small script to pull your own Netlify access logs and keep them locally as NDJSON, with a tiny dashboard to look at them. Run `./netlify-logs.py -h` (and
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
