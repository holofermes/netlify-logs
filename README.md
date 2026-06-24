# netlify-logs

Pull per-request access logs for a Netlify site into NDJSON — on a timer and on
demand — and browse them on a self-contained dashboard with a world map, a
detail view, and live filters. Stdlib Python, no runtime dependencies. The
dashboard binds to localhost; how you reach it from elsewhere is your call
(see below).

Each new request is enriched (via Netlify's per-request detail endpoint) with
client IP, country, region, latitude/longitude, and user agent, so the map and
detail view have something to show. Pass `--no-ip` to skip enrichment (saves one
GET per new request).

## Setup

```sh
npm install -g netlify-cli   # or use: npx netlify-cli
netlify login
export NETLIFY_SITE_ID=<your-site-id>   # Site configuration -> Site information
./install.sh
```

`install.sh` adds two user systemd units:

- `netlify-logs-pull.timer` — runs a pull every 15 minutes.
- `netlify-logs.service` — serves the dashboard on `127.0.0.1:8800`.

## Use

```sh
./netlify-logs.py pull --site <id> --minutes 20      # one pull
./netlify-logs.py pull --site <id> --minutes 1440    # last 24h
./netlify-logs.py serve --site <id>                  # dashboard + "Run now" button
```

`--site` is optional if `NETLIFY_SITE_ID` is set in the environment.

Output: `logs/access.ndjson` — one request per line, append-only, deduped by
`request_id`.

## Dashboard

- **World map** of request origins (pins by lat/lon, sized by volume, clustered
  by location). Drawn from a vendored equirectangular land outline
  (`world.json`) served by the app — no tiles, no CDN, no map library.
- **Click any row** (or a pin) for a detail drawer: full URL, timestamps (local
  + UTC), IP, country/region, user agent, referrer, cache results, raw JSON.
- **Filter chips** with three states — click to cycle off → only → hide — plus a
  box to add your own URL globs (`*` wildcard). Your set is saved per browser.
  The map and table both honor the active filters.
- **Run now** triggers a pull and refreshes the view.

## Reaching the dashboard remotely

The dashboard listens on localhost only. Expose it with whatever you already
run — this tool stays out of it. The page is mount-path aware, so it works at
the root or behind a reverse proxy on a sub-path. For example:

```sh
tailscale serve --bg --set-path /logs http://127.0.0.1:8800   # -> <host>/logs
tailscale serve --bg --https=8801    http://127.0.0.1:8800    # -> <host>:8801/
ssh -L 8800:127.0.0.1:8800 <this-host>                        # ad-hoc tunnel
```

## Config (environment)

| var | default | purpose |
|-----|---------|---------|
| `NETLIFY_SITE_ID` | *(required)* | target site (or pass `--site`) |
| `NETLIFY_AUTH_TOKEN` | from `~/.config/netlify/config.json` | auth override |
| `NETLIFY_LOG_OUT` | `logs/access.ndjson` | output file |
| `NETLIFY_LOGS_HOST` | `127.0.0.1` | dashboard bind address |
| `NETLIFY_LOGS_PORT` | `8800` | dashboard port |
| `NETLIFY_LOGS_WINDOW` | `20` | pull lookback (minutes) |

Requirements: Python 3 (stdlib only), systemd. The map outline ships with the
repo as `world.json` next to the script — no other assets needed.
