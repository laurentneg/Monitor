'use strict';

const { WebSocketServer, WebSocket } = require('ws');
const { PulseArbiter } = require('./pulse-arbiter');

const DEFAULT_CONFIG = {
    airbandWsUrl: 'ws://127.0.0.1:8081/ws/monitor',
    tetrapolWsUrl: 'ws://127.0.0.1:8080/ws/monitor',
    airbandBase: 'http://127.0.0.1:8081',
    tetrapolBase: 'http://127.0.0.1:8080',
    icecastPublicBase: '',
    broadcastIntervalMs: 250,
    /** Mark Airband/Tetrapol dead if no WS update arrives within this window. */
    sourceStaleMs: 5000,
    pulseArbiter: true,
};

function emptyState() {
    return {
        channels: { channels: [] },
        groups: { groups: [] },
        tetrapol: {
            connected: false,
            channels: { channels: [] },
            groups: { groups: [] },
            cells: [],
            pulse_exclusive: {},
        },
        config: {},
        pulse_exclusive: {},
        NETWORKS: { networks: [] },
        icecast: { icestats: { source: [] } },
    };
}

function deepMerge(target, source) {
    if (!source || typeof source !== 'object') return target;
    for (const [key, value] of Object.entries(source)) {
        if (value && typeof value === 'object' && !Array.isArray(value)) {
            target[key] = deepMerge(target[key] && typeof target[key] === 'object' ? { ...target[key] } : {}, value);
        } else {
            target[key] = value;
        }
    }
    return target;
}

function parseUpdateMessage(raw) {
    try {
        const msg = JSON.parse(raw);
        if (msg.type === 'update') return msg;
        if (msg.event) return { _event: msg };
    } catch (_) {
        /* ignore malformed payloads */
    }
    return null;
}

function pulseModeKey(pe) {
    if (!pe || typeof pe !== 'object') return null;
    if (!Object.prototype.hasOwnProperty.call(pe, 'exclusive')
        && !Object.prototype.hasOwnProperty.call(pe, 'force_before_full')) {
        return null;
    }
    return `${pe.exclusive ? 1 : 0}|${pe.force_before_full ? 1 : 0}`;
}

function pulseModePayload(pe) {
    return {
        exclusive: !!pe.exclusive,
        force_before_full: !!pe.force_before_full,
    };
}

class WsBridge {
    constructor(config = {}) {
        this.config = { ...DEFAULT_CONFIG, ...config };
        this.state = emptyState();
        this.clients = new Set();
        this.sources = {
            airband: { ws: null, connected: false, retries: 0, timer: null, lastMsgAt: 0 },
            tetrapol: { ws: null, connected: false, retries: 0, timer: null, lastMsgAt: 0 },
        };
        this.broadcastTimer = null;
        this.dirty = false;
        this.wss = null;
        this.arbiter = this.config.pulseArbiter === false
            ? null
            : new PulseArbiter({
                airbandBase: this.config.airbandBase,
                tetrapolBase: this.config.tetrapolBase,
                onChange: () => {
                    this.dirty = true;
                    this.flushBroadcast();
                },
            });
        this._pulseModeSeen = { airband: null, tetrapol: null };
        this._mirroringPulseMode = false;
        this._pulseModeQueue = [];
        this._pulseModeHoldoff = null;
    }

    attach(server) {
        this.wss = new WebSocketServer({ noServer: true });
        this.wss.on('connection', (socket) => {
            this.clients.add(socket);
            socket.on('close', () => this.clients.delete(socket));
            socket.on('error', () => this.clients.delete(socket));
            this.send(socket, this.buildUpdate());
        });
        this.connectSource('airband', this.config.airbandWsUrl, (msg) => this.onAirbandMessage(msg));
        this.connectSource('tetrapol', this.config.tetrapolWsUrl, (msg) => this.onTetrapolMessage(msg));
        this.broadcastTimer = setInterval(() => {
            this.checkSourceFreshness();
            this.flushBroadcast();
        }, this.config.broadcastIntervalMs);
        if (this.arbiter) this.arbiter.start();
    }

    handleUpgrade(request, socket, head) {
        if (!this.wss) {
            socket.destroy();
            return;
        }
        this.wss.handleUpgrade(request, socket, head, (ws) => {
            this.wss.emit('connection', ws, request);
        });
    }

    connectSource(name, url, onMessage) {
        const src = this.sources[name];
        if (!url) {
            src.connected = false;
            return;
        }
        if (src.ws && (src.ws.readyState === WebSocket.OPEN || src.ws.readyState === WebSocket.CONNECTING)) {
            return;
        }
        const ws = new WebSocket(url);
        src.ws = ws;
        ws.on('open', () => {
            src.connected = true;
            src.retries = 0;
            src.lastMsgAt = Date.now();
            if (name === 'tetrapol') {
                this.state.tetrapol.connected = true;
            }
            this.dirty = true;
            this.flushBroadcast();
        });
        ws.on('message', (data) => {
            src.lastMsgAt = Date.now();
            if (!src.connected) {
                src.connected = true;
                if (name === 'tetrapol') this.state.tetrapol.connected = true;
                this.dirty = true;
            }
            const parsed = parseUpdateMessage(String(data));
            if (!parsed) return;
            if (parsed._event) {
                this.broadcastEvent(parsed._event);
                return;
            }
            onMessage(parsed);
        });
        ws.on('close', () => {
            src.connected = false;
            src.ws = null;
            src.lastMsgAt = 0;
            this.onSourceDisconnected(name);
            this.scheduleReconnect(name, url, onMessage);
        });
        ws.on('error', () => {
            /* close handler schedules reconnect */
        });
    }

    onSourceDisconnected(name) {
        if (name === 'airband') {
            this.clearAirbandState();
        } else if (name === 'tetrapol') {
            this.state.tetrapol.connected = false;
            // Keep last tetrapol rows but UI marks them unsynced via connected=false.
        }
        this.dirty = true;
        this.tickArbiter();
        this.flushBroadcast();
    }

    clearAirbandState() {
        this.state.channels = { channels: [] };
        this.state.groups = { groups: [] };
        this.state.pulse_exclusive = {};
        this.state.NETWORKS = { networks: [] };
        this.state.icecast = { icestats: { source: [] } };
        this._pulseModeSeen.airband = null;
    }

    checkSourceFreshness() {
        const staleMs = Math.max(2000, Number(this.config.sourceStaleMs) || 5000);
        const now = Date.now();
        for (const [name, src] of Object.entries(this.sources)) {
            if (!src.connected || !src.lastMsgAt) continue;
            if (now - src.lastMsgAt < staleMs) continue;
            // Half-open / frozen upstream: force close so UI drops stale green state.
            console.warn(`[ws-bridge] ${name} stale (${now - src.lastMsgAt}ms) — closing`);
            try {
                if (src.ws) src.ws.terminate();
            } catch (_) {
                /* ignore */
            }
            src.connected = false;
            src.ws = null;
            src.lastMsgAt = 0;
            this.onSourceDisconnected(name);
            const url = name === 'airband' ? this.config.airbandWsUrl : this.config.tetrapolWsUrl;
            const onMessage = name === 'airband'
                ? (msg) => this.onAirbandMessage(msg)
                : (msg) => this.onTetrapolMessage(msg);
            this.scheduleReconnect(name, url, onMessage);
        }
    }

    scheduleReconnect(name, url, onMessage) {
        const src = this.sources[name];
        if (src.timer) return;
        src.retries += 1;
        const delay = Math.min(30000, 1000 * src.retries);
        src.timer = setTimeout(() => {
            src.timer = null;
            this.connectSource(name, url, onMessage);
        }, delay);
    }

    onAirbandMessage(msg) {
        if (msg.channels) this.state.channels = msg.channels;
        if (msg.groups) this.state.groups = msg.groups;
        if (msg.config) this.state.config = deepMerge(this.state.config, msg.config);
        if (msg.pulse_exclusive) this.state.pulse_exclusive = msg.pulse_exclusive;
        if (msg.icecast) this.state.icecast = msg.icecast;
        if (msg.NETWORKS) this.state.NETWORKS = msg.NETWORKS;
        this.dirty = true;
        this.syncPulseModeFrom('airband');
        this.tickArbiter();
    }

    onTetrapolMessage(msg) {
        this.state.tetrapol.connected = true;
        if (msg.channels) this.state.tetrapol.channels = msg.channels;
        if (msg.groups) this.state.tetrapol.groups = msg.groups;
        if (msg.cells) this.state.tetrapol.cells = msg.cells;
        if (msg.pulse_exclusive) this.state.tetrapol.pulse_exclusive = msg.pulse_exclusive;
        if (msg.icecast && (!this.state.icecast || !this.state.icecast.icestats)) {
            this.state.icecast = msg.icecast;
        }
        if (msg.NETWORKS && (!this.state.NETWORKS.networks || !this.state.NETWORKS.networks.length)) {
            this.state.NETWORKS = msg.NETWORKS;
        }
        this.dirty = true;
        this.syncPulseModeFrom('tetrapol');
        this.tickArbiter();
    }

    syncPulseModeFrom(source) {
        if (!this.arbiter) return;
        const ab = this.state.pulse_exclusive || {};
        const tp = (this.state.tetrapol && this.state.tetrapol.pulse_exclusive) || {};
        const abKey = pulseModeKey(ab);
        const tpKey = pulseModeKey(tp);
        if (!abKey || !tpKey) return;

        const now = Date.now();
        if (this._pulseModeHoldoff && now < this._pulseModeHoldoff.until) {
            const want = this._pulseModeHoldoff.key;
            const payload = this._pulseModeHoldoff.payload;
            if (abKey !== want) this.pushPulseMode('airband', payload);
            if (tpKey !== want) this.pushPulseMode('tetrapol', payload);
            this._pulseModeSeen.airband = want;
            this._pulseModeSeen.tetrapol = want;
            return;
        }
        if (this._pulseModeHoldoff && now >= this._pulseModeHoldoff.until) {
            this._pulseModeHoldoff = null;
        }

        if (this._mirroringPulseMode) return;

        const prev = this._pulseModeSeen[source];
        const cur = source === 'airband' ? abKey : tpKey;

        if (abKey === tpKey) {
            this._pulseModeSeen.airband = abKey;
            this._pulseModeSeen.tetrapol = tpKey;
            return;
        }

        if (prev === null) {
            this._pulseModeSeen[source] = cur;
            if (abKey !== tpKey) {
                this.pushPulseMode('tetrapol', pulseModePayload(ab), { holdoff: true });
            }
            return;
        }

        if (prev === cur) return;

        this._pulseModeSeen[source] = cur;
        const pe = source === 'airband' ? ab : tp;
        const target = source === 'airband' ? 'tetrapol' : 'airband';
        this.pushPulseMode(target, pulseModePayload(pe), { holdoff: true });
    }

    pushPulseMode(target, payload, { holdoff = false } = {}) {
        if (!this.arbiter || !payload) return;
        const key = `${payload.exclusive ? 1 : 0}|${payload.force_before_full ? 1 : 0}`;
        if (holdoff) {
            this._pulseModeHoldoff = {
                key,
                payload: { ...payload },
                until: Date.now() + 2000,
            };
        }
        const body = {
            exclusive: !!payload.exclusive,
            force_before_full: !!payload.force_before_full,
        };
        this._pulseModeQueue.push({ target, body, key });
        this.drainPulseModeQueue();
    }

    drainPulseModeQueue() {
        if (!this.arbiter || this._mirroringPulseMode) return;
        const next = this._pulseModeQueue.shift();
        if (!next) return;
        this._mirroringPulseMode = true;
        this.arbiter.post(next.target, next.body)
            .then(() => {
                this._pulseModeSeen[next.target] = next.key;
            })
            .catch((err) => {
                console.warn('[pulse-mode-sync]', err && err.message ? err.message : err);
            })
            .finally(() => {
                this._mirroringPulseMode = false;
                this.drainPulseModeQueue();
            });
    }

    tickArbiter() {
        if (!this.arbiter) return;
        this.arbiter.onConnectivity({
            airband: this.sources.airband.connected,
            tetrapol: this.sources.tetrapol.connected,
        });
        this.arbiter.onStates(
            this.state.pulse_exclusive,
            this.state.tetrapol && this.state.tetrapol.pulse_exclusive
        );
        this.dirty = true;
    }

    buildUpdate() {
        const publicBase = this.config.icecastPublicBase
            || (this.state.config && this.state.config.icecast_public_base)
            || '';
        return {
            type: 'update',
            channels: this.state.channels,
            groups: this.state.groups,
            tetrapol: this.state.tetrapol,
            config: {
                ...this.state.config,
                airband_bridged: this.sources.airband.connected,
                tetrapol_bridged: this.sources.tetrapol.connected,
                http_port: this.config.listenPort,
                // Prefer monitor env ICECAST_PUBLIC_BASE; else Airband's icecast_public_base.
                icecast_public_base: publicBase || null,
            },
            sources: {
                airband: this.sources.airband.connected,
                tetrapol: this.sources.tetrapol.connected,
            },
            pulse_exclusive: this.state.pulse_exclusive,
            pulse_arbiter: this.arbiter ? this.arbiter.snapshot() : null,
            NETWORKS: this.state.NETWORKS,
            icecast: this.state.icecast,
        };
    }

    send(socket, payload) {
        if (socket.readyState !== WebSocket.OPEN) return;
        socket.send(JSON.stringify(payload));
    }

    broadcastEvent(event) {
        const raw = JSON.stringify(event);
        for (const client of this.clients) {
            if (client.readyState === WebSocket.OPEN) client.send(raw);
        }
    }

    flushBroadcast() {
        if (!this.dirty && this.clients.size === 0) return;
        this.dirty = false;
        const payload = JSON.stringify(this.buildUpdate());
        for (const client of this.clients) {
            if (client.readyState !== WebSocket.OPEN) continue;
            client.send(payload);
        }
    }

    close() {
        if (this.broadcastTimer) clearInterval(this.broadcastTimer);
        if (this.arbiter) this.arbiter.stop();
        for (const src of Object.values(this.sources)) {
            if (src.timer) clearTimeout(src.timer);
            if (src.ws) src.ws.close();
        }
        if (this.wss) this.wss.close();
        this.clients.clear();
    }
}

module.exports = { WsBridge, DEFAULT_CONFIG };
