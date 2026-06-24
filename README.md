# netlify-logs

A small script to pull your own Netlify access logs and keep them locally in SQLite, with a tiny dashboard to look at them. Run `./netlify-logs.py -h`, `pull -h`, or `serve -h` for the full set of options.

![dashboard](docs/01-dashboard.png)

## Setup

```sh
npm install -g netlify-cli   # or use: npx netlify-cli
netlify login
export NETLIFY_SITE_ID=<your-site-id>   # Site configuration -> Site information
./install.sh
```

`install.sh` adds two user systemd units: a pull every 15 minutes, and the
dashboard on `127.0.0.1:8800`. Requests are stored in `logs/access.db`, a
stdlib SQLite database, deduped by `request_id`.

## Dashboard

- **Date range.** Last 1h / 24h / 7d / 30d, custom, or all.
- **World map** of request origins. Zoom, pan, fit.
- **Entry-detail drawer.** Click any row or pin for the full record.
- **Filter chips.** Toggle URL-glob filters on or off; recolor or rename each in place.
- **Run now.** Pull on demand and refresh.
