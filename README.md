# netlify-logs

Pull per-request access logs for a Netlify site into NDJSON — on a timer and on demand.

## Setup

```sh
npm install -g netlify-cli   # or use: npx netlify-cli
netlify login
./install.sh
```

`install.sh` adds two user systemd units:

- `netlify-logs-pull.timer` — runs a pull every 15 minutes.
- `netlify-logs.service` — serves the dashboard on the box's Tailscale IP (port 8800).

## Use

```sh
./netlify-logs.py pull --minutes 20      # one pull
./netlify-logs.py pull --minutes 1440    # last 24h
./netlify-logs.py serve --host tailscale # dashboard + "Run now" button
```

Output: `logs/access.ndjson` — one request per line, append-only, deduped by
`request_id`. The dashboard's **Run now** button triggers a pull and shows the
most recent requests.

## Config (environment)

| var | default | purpose |
|-----|---------|---------|
| `NETLIFY_AUTH_TOKEN` | from `~/.config/netlify/config.json` | auth override |
| `NETLIFY_SITE_ID` | example.com site | target site |
| `NETLIFY_LOG_OUT` | `logs/access.ndjson` | output file |
| `NETLIFY_LOGS_PORT` | `8800` | dashboard port |
| `NETLIFY_LOGS_WINDOW` | `20` | pull lookback (minutes) |

Requirements: Python 3 (stdlib only), systemd, Tailscale.
