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
    /** Full snapshot resync to clients (patches in between). */
    fullSyncIntervalMs: 30000,
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

function cloneJson(value) {
    return value == null ? value : JSON.parse(JSON.stringify(value));
}

function stableStringify(value) {
    return JSON.stringify(value);
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

function listFromSection(section, arrayKey) {
    if (!section) return [];
    if (Array.isArray(section)) return section;
    if (arrayKey && Array.isArray(section[arrayKey])) return section[arrayKey];
    return [];
}

function entityId(item, keys) {
    if (!item || typeof item !== 'object') return null;
    for (const key of keys) {
        if (item[key] != null && item[key] !== '') return String(item[key]);
    }
    return null;
}

/** Diff two entity lists; return { upsert, remove } or null if unchanged. */
function diffEntityList(currList, prevList, idKeys) {
    const currMap = new Map();
    const prevMap = new Map();
    for (const item of currList || []) {
        const id = entityId(item, idKeys);
        if (id != null) currMap.set(id, item);
    }
    for (const item of prevList || []) {
        const id = entityId(item, idKeys);
        if (id != null) prevMap.set(id, item);
    }
    const upsert = [];
    const remove = [];
    for (const [id, item] of currMap) {
        const prev = prevMap.get(id);
        if (!prev || stableStringify(prev) !== stableStringify(item)) {
            upsert.push(item);
        }
    }
    for (const id of prevMap.keys()) {
        if (!currMap.has(id)) remove.push(id);
    }
    if (!upsert.length && !remove.length) return null;
    const out = {};
    if (upsert.length) out.upsert = upsert;
    if (remove.length) out.remove = remove;
    return out;
}

function diffPlainObject(curr, prev) {
    if (stableStringify(curr) === stableStringify(prev)) return null;
    return cloneJson(curr);
}

/** Quantize noisy pulse telemetry so tiny fill/buffer jitter does not emit patches. */
function normalizePulseExclusive(pe) {
    if (!pe || typeof pe !== 'object') return pe || {};
    const out = { ...pe };
    const qFill = (v) => Math.floor((Number(v) || 0) * 100) / 100; // 1% floor buckets
    const qBuf = (v) => Math.floor((Number(v) || 0) * 2) / 2; // 0.5 s
    if (Array.isArray(out.listen_groups)) {
        out.listen_groups = out.listen_groups.map((g) => {
            if (!g || typeof g !== 'object') return g;
            return {
                ...g,
                fill_ratio: qFill(g.fill_ratio),
                buffer_seconds: qBuf(g.buffer_seconds),
            };
        });
    }
    if (out.fill_ratios && typeof out.fill_ratios === 'object') {
        const fr = {};
        for (const [k, v] of Object.entries(out.fill_ratios)) {
            fr[k] = qFill(v);
        }
        out.fill_ratios = fr;
    }
    return out;
}

function diffPulseExclusive(curr, prev) {
    const a = normalizePulseExclusive(curr || {});
    const b = normalizePulseExclusive(prev || {});
    if (stableStringify(a) === stableStringify(b)) return null;
    return cloneJson(a);
}

function diffConfigFields(curr, prev) {
    const out = {};
    let changed = false;
    const keys = new Set([
        ...Object.keys(curr || {}),
        ...Object.keys(prev || {}),
    ]);
    for (const key of keys) {
        const a = curr ? curr[key] : undefined;
        const b = prev ? prev[key] : undefined;
        if (stableStringify(a) !== stableStringify(b)) {
            out[key] = a === undefined ? null : a;
            changed = true;
        }
    }
    return changed ? out : null;
}

/**
 * Build a patch message from current vs last-sent snapshots.
 * Returns null when nothing changed.
 */
function computePatch(current, lastSent) {
    if (!lastSent) return null;
    const patch = { type: 'patch' };
    let any = false;

    const chDiff = diffEntityList(
        listFromSection(current.channels, 'channels'),
        listFromSection(lastSent.channels, 'channels'),
        ['stream_id', 'id']
    );
    if (chDiff) {
        patch.channels = chDiff;
        any = true;
    }

    const grDiff = diffEntityList(
        listFromSection(current.groups, 'groups'),
        listFromSection(lastSent.groups, 'groups'),
        ['group_id', 'id', 'stream_id']
    );
    if (grDiff) {
        patch.groups = grDiff;
        any = true;
    }

    const currTp = current.tetrapol || {};
    const prevTp = lastSent.tetrapol || {};
    const tp = {};
    let tpAny = false;

    if (!!currTp.connected !== !!prevTp.connected) {
        tp.connected = !!currTp.connected;
        tpAny = true;
    }

    const tpCh = diffEntityList(
        listFromSection(currTp.channels, 'channels'),
        listFromSection(prevTp.channels, 'channels'),
        ['channel_id', 'id']
    );
    if (tpCh) {
        tp.channels = tpCh;
        tpAny = true;
    }

    const tpGr = diffEntityList(
        listFromSection(currTp.groups, 'groups'),
        listFromSection(prevTp.groups, 'groups'),
        ['group_id', 'id']
    );
    if (tpGr) {
        tp.groups = tpGr;
        tpAny = true;
    }

    const tpCells = diffEntityList(
        listFromSection(currTp.cells, 'cells'),
        listFromSection(prevTp.cells, 'cells'),
        ['channel_id', 'id']
    );
    if (tpCells) {
        tp.cells = tpCells;
        tpAny = true;
    }

    const tpPe = diffPulseExclusive(currTp.pulse_exclusive || {}, prevTp.pulse_exclusive || {});
    if (tpPe) {
        tp.pulse_exclusive = tpPe;
        tpAny = true;
    }

    if (tpAny) {
        patch.tetrapol = tp;
        any = true;
    }

    const sources = diffPlainObject(current.sources, lastSent.sources);
    if (sources) {
        patch.sources = sources;
        any = true;
    }

    const config = diffConfigFields(current.config, lastSent.config);
    if (config) {
        patch.config = config;
        any = true;
    }

    const pe = diffPulseExclusive(current.pulse_exclusive || {}, lastSent.pulse_exclusive || {});
    if (pe) {
        patch.pulse_exclusive = pe;
        any = true;
    }

    const arb = diffPlainObject(current.pulse_arbiter, lastSent.pulse_arbiter);
    if (arb !== null) {
        // null arbiter → explicit null; object when changed
        patch.pulse_arbiter = current.pulse_arbiter == null ? null : arb;
        any = true;
    }

    const networks = diffPlainObject(current.NETWORKS, lastSent.NETWORKS);
    if (networks) {
        patch.NETWORKS = networks;
        any = true;
    }

    const icecast = diffPlainObject(current.icecast, lastSent.icecast);
    if (icecast) {
        patch.icecast = icecast;
        any = true;
    }

    return any ? patch : null;
}

function eventDedupKey(event) {
    if (!event || typeof event !== 'object') return 'unknown';
    const name = event.event != null ? String(event.event) : 'event';
    const ch = event.channel_id != null ? String(event.channel_id) : '';
    return `${name}|${ch}`;
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
        this.fullSyncTimer = null;
        this.dirty = false;
        /** @type {object|null} Last snapshot actually sent to clients (for patch diffs). */
        this.lastSent = null;
        /** Last serialized event payload per dedup key. */
        this._lastEventByKey = new Map();
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
            // Flush pending patches to existing clients before a new snapshot baseline.
            if (this.dirty && this.clients.size > 0) {
                this.flushBroadcast();
            }
            this.clients.add(socket);
            socket.on('close', () => this.clients.delete(socket));
            socket.on('error', () => this.clients.delete(socket));
            const full = this.buildUpdate();
            this.send(socket, full);
            // Align global lastSent so subsequent patches match what clients know.
            this.lastSent = cloneJson(full);
            this.dirty = false;
        });
        this.connectSource('airband', this.config.airbandWsUrl, (msg) => this.onAirbandMessage(msg));
        this.connectSource('tetrapol', this.config.tetrapolWsUrl, (msg) => this.onTetrapolMessage(msg));
        this.broadcastTimer = setInterval(() => {
            this.checkSourceFreshness();
            this.flushBroadcast();
        }, this.config.broadcastIntervalMs);
        // Full resync only if something was dirty recently — avoid 15s spam
        // when the wire is already quiet.
        const fullMs = Math.max(5000, Number(this.config.fullSyncIntervalMs) || 30000);
        this.fullSyncTimer = setInterval(() => {
            if (this.clients.size === 0) return;
            this.flushBroadcast({ forceFull: true });
        }, fullMs);
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
        // Do not force dirty here: arbiter onChange notifies real changes;
        // upstream handlers already set dirty for telemetry sections.
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

    broadcastRaw(raw) {
        for (const client of this.clients) {
            if (client.readyState === WebSocket.OPEN) client.send(raw);
        }
    }

    broadcastEvent(event) {
        const key = eventDedupKey(event);
        const raw = JSON.stringify(event);
        if (this._lastEventByKey.get(key) === raw) return;
        this._lastEventByKey.set(key, raw);
        this.broadcastRaw(raw);
    }

    flushBroadcast({ forceFull = false } = {}) {
        if (this.clients.size === 0) return;
        if (!forceFull && !this.dirty) return;

        const current = this.buildUpdate();

        if (forceFull || !this.lastSent) {
            this.dirty = false;
            this.lastSent = cloneJson(current);
            this.broadcastRaw(JSON.stringify(current));
            return;
        }

        this.dirty = false;
        const patch = computePatch(current, this.lastSent);
        if (!patch) return;
        this.lastSent = cloneJson(current);
        this.broadcastRaw(JSON.stringify(patch));
    }

    close() {
        if (this.broadcastTimer) clearInterval(this.broadcastTimer);
        if (this.fullSyncTimer) clearInterval(this.fullSyncTimer);
        if (this.arbiter) this.arbiter.stop();
        for (const src of Object.values(this.sources)) {
            if (src.timer) clearTimeout(src.timer);
            if (src.ws) src.ws.close();
        }
        if (this.wss) this.wss.close();
        this.clients.clear();
        this._lastEventByKey.clear();
        this.lastSent = null;
    }
}

module.exports = {
    WsBridge,
    DEFAULT_CONFIG,
    computePatch,
    diffEntityList,
};
