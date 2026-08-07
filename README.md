# Airband + Tetrapol Monitor

Standalone Node.js monitor that aggregates WebSocket feeds from RTLSDR-Airband and Tetrapol-kit, serves the web UI, and proxies HTTP APIs/streams.

## Architecture

- **Airband** (`:8081`) — analog channels, PulseAudio exclusive
- **Tetrapol** (`:8080`) — Tetrapol groups/channels/cells
- **Icecast** (`:8000`) — audio mounts under `/stream/*.mp3`
- **This monitor** (`:8082`) — unified WS `/ws/monitor`, static UI, API/stream proxy, **Pulse arbiter**

## Quick start

```bash
cd ~/monitor
npm install
npm start
```

Open http://localhost:8082/monitor.html

## Configuration (environment)

| Variable | Default | Description |
|----------|---------|-------------|
| `MONITOR_PORT` | `8082` | Listen port |
| `AIRBAND_BASE` | `http://127.0.0.1:8081` | RTLSDR-Airband HTTP |
| `TETRAPOL_BASE` | `http://127.0.0.1:8080` | Tetrapol-kit HTTP |
| `ICECAST_BASE` | `http://127.0.0.1:8000` | Icecast HTTP (server-side proxy target) |
| `ICECAST_PUBLIC_BASE` | _(empty)_ | Browser-facing Icecast URL (direct streams; e.g. `http://192.168.0.12:8000`) |
| `STREAM_PROXY` | `1` | `0` disables monitor `/stream` proxy when using direct Icecast |
| `AIRBAND_WS_URL` | `ws://127.0.0.1:8081/ws/monitor` | Airband WebSocket |
| `TETRAPOL_WS_URL` | `ws://127.0.0.1:8080/ws/monitor` | Tetrapol WebSocket |
| `API_PROXY_TIMEOUT_MS` | `15000` | Timeout for `/api/*` proxies (streams unlimited) |
| `PULSE_ARBITER` | on | Set to `0` to disable Pulse authority + heartbeat |

### Projects (Airband / Tetrapol)

| Variable | Default | Description |
|----------|---------|-------------|
| `MONITOR_BASE` | `http://127.0.0.1:8082` | Monitor URL for claim/release (required when exclusive + arbiter on) |

Airband also accepts `monitor.monitor_base` in its libconfig.

## API proxy

- `/stream` → Icecast (`ICECAST_BASE`) — optional; prefer `ICECAST_PUBLIC_BASE` so browsers bypass the proxy
- `/api/monitor/config` → `{ icecast_public_base, stream_proxy }`
- `/health` — liveness + endpoints
- `GET|POST /api/airband/*` → Airband API (toggle, pulse-exclusive, channels…)
- `GET|POST /api/tetrapol/*` → Tetrapol API

## Pulse exclusive (monitor = authority)

When Pulse **exclusive** is on, the monitor is the FIFO arbiter across Airband and Tetrapol:

1. **Heartbeat** (~1 Hz): monitor POSTs `{ monitor_heartbeat, lease_ms: 3000 }` to each exclusive backend (presence only — does **not** force `manual_hold`).
2. **Claim**: projects do **not** auto-promote; they `POST /api/pulse/claim` when a channel is ready.
3. **Commands**: monitor grants play via `active_group_id` / `yield_active` / `clear_buffer`.
4. **Release**: `POST /api/pulse/release` when a com ends → next waiter plays (any project).
5. **Fallback**: if no heartbeat for ~3 s (`monitor_alive=false`), each project resumes local arbitration.

Rules: first claim plays; FIFO pause/resume; ★ preempts non-★ when active unread &lt; 80 %; **Bascule avant fin tampon** force-plays a waiter at ≥ 92 % fill when the active is quiet and unread &lt; 80 % (then rotate to the next saturated channel the same way).

### Direct Icecast streams (recommended)

Two access modes can run **at the same time**:

| Accès | URL page | Streams audio |
|-------|----------|---------------|
| LAN direct monitor | `http://<ip-monitor>:8082` | same-origin `/stream/...` (monitor proxy) by default |
| Domaine / Apache | `https://domaine.externe.com/...` | same-origin `/stream/...` (Apache → Icecast, bypass Node) |

Default is **same-origin `/stream`** so `IcecastMetadataPlayer` works without CORS. Direct browser→Icecast (`ICECAST_PUBLIC_BASE`) is opt-in (`ICECAST_BROWSER_DIRECT=1` or `?directStreams=1`) and needs Icecast CORS — otherwise you get `net::ERR_EMPTY_RESPONSE`.

```bash
export ICECAST_PUBLIC_BASE=http://192.168.0.12:8000   # optional, for direct mode
# export ICECAST_BROWSER_DIRECT=1   # only if Icecast sends Access-Control-Allow-Origin
npm start
```

Apache path map (see `apache-monitor.conf.example`):

```
domaine.externe.com/           → monitor:8082
domaine.externe.com/api/*      → monitor:8082
domaine.externe.com/ws/monitor → monitor:8082
domaine.externe.com/stream/*   → icecast:8000/stream/*   ← bypass Node
```

Also accepted from Airband `monitor.icecast_public_base`. For direct mode Icecast needs CORS (`Access-Control-Allow-Origin: *`).

`GET /api/monitor/config` → `{ icecast_public_base, icecast_browser_direct, stream_proxy }`.

### Monitor Pulse routes

- `GET /api/pulse/arbiter` — snapshot `{ active, queue, monitor_alive, … }`
- `POST /api/pulse/claim` — `{ source, id, priority?, … }` → `{ granted, action: play|wait }`
- `POST /api/pulse/release` — `{ source, id, reason? }`
- `POST /api/pulse/clear` — `{ source, id }`
- `POST /api/pulse/play` — UI force-play `{ source, id }`
- `POST /api/pulse/stop` — UI stop (no auto-promote)
- `POST /api/pulse/auto` — clear holds + promote next if idle

WS `update` includes `pulse_arbiter: { active, queue, monitor_alive }` and both `pulse_exclusive` (Airband) + `tetrapol.pulse_exclusive`.

Disable with `PULSE_ARBITER=0`.

## Apache reverse proxy

If the UI is behind Apache and some XHR stay « pending », use flush + short API timeouts. See [`apache-monitor.conf.example`](apache-monitor.conf.example).

Essentials:

- `mod_proxy`, `mod_proxy_http`, `mod_proxy_wstunnel`, `mod_headers`
- `/api/*` → `timeout=30`, no buffering
- `/stream` → `flushpackets=on`, large/no idle timeout
- `/ws/monitor` → `ws://127.0.0.1:8082/ws/monitor`

## WebSocket

Browser connects to `ws://localhost:8082/ws/monitor`. The bridge merges Airband + Tetrapol state and:

- sends a full `type: "update"` snapshot on connect and about every 15 s (`fullSyncIntervalMs`);
- otherwise sends `type: "patch"` with only changed entities (channel/group upserts & removes, pulse, sources…);
- relays Tetrapol `event` frames (`cell_info` always forwarded as liveness; `cch_sync` deduplicated when identical).
