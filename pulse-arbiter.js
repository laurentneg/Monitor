'use strict';

const http = require('http');
const https = require('https');
const { URL } = require('url');
const { randomUUID } = require('crypto');

/** Unread fill at/above which a force-promoted active must not be preempted. */
const YIELD_MAX_UNREAD_RATIO = 0.80;
/** Buffer fill at/above which « Bascule avant fin tampon » force-promotes a waiter. */
const FORCE_PROMOTE_RATIO = 0.92;
const HEARTBEAT_INTERVAL_MS = 1000;
const HEARTBEAT_LEASE_MS = 3000;
/** Min time before a non-priority active can be cut by priority (align Airband). */
const PRIORITY_PREEMPT_GRACE_MS = 400;
/** Min time before force-promote may displace the current active (align Tetrapol). */
const PROMOTION_GRACE_MS = 15000;
/** If a source WS stays down while active, force failover. */
const SOURCE_DOWN_FAILOVER_MS = 2500;

function keyOf(source, id) {
    return `${source}:${String(id)}`;
}

function postJson(baseUrl, apiPath, body) {
    return new Promise((resolve, reject) => {
        let url;
        try {
            url = new URL(apiPath, baseUrl);
        } catch (err) {
            reject(err);
            return;
        }
        const payload = JSON.stringify(body);
        const lib = url.protocol === 'https:' ? https : http;
        const req = lib.request(
            {
                protocol: url.protocol,
                hostname: url.hostname,
                port: url.port || (url.protocol === 'https:' ? 443 : 80),
                path: url.pathname + url.search,
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                    'Content-Length': Buffer.byteLength(payload),
                    Connection: 'close',
                },
                timeout: 8000,
            },
            (res) => {
                const chunks = [];
                res.on('data', (c) => chunks.push(c));
                res.on('end', () => {
                    const text = Buffer.concat(chunks).toString('utf8');
                    if (res.statusCode && res.statusCode >= 400) {
                        reject(new Error(`HTTP ${res.statusCode} ${apiPath}: ${text.slice(0, 160)}`));
                        return;
                    }
                    try {
                        resolve(text ? JSON.parse(text) : {});
                    } catch (_) {
                        resolve({ raw: text });
                    }
                });
            }
        );
        req.on('error', reject);
        req.on('timeout', () => {
            req.destroy();
            reject(new Error(`timeout ${apiPath}`));
        });
        req.write(payload);
        req.end();
    });
}

/**
 * Authoritative cross-project Pulse exclusive arbiter.
 *
 * Projects claim/release via HTTP; the monitor heartbeats presence and
 * commands play / yield / clear. No reactive fight with local auto-promote
 * when monitor_alive is set on the projects.
 */
class PulseArbiter {
    constructor(opts = {}) {
        this.airbandBase = opts.airbandBase || 'http://127.0.0.1:8081';
        this.tetrapolBase = opts.tetrapolBase || 'http://127.0.0.1:8080';
        this.enabled = opts.enabled !== false;
        this.monitorId = opts.monitorId || randomUUID().slice(0, 8);
        this.log = opts.log || ((...args) => console.log('[pulse-arbiter]', ...args));
        this.onChange = typeof opts.onChange === 'function' ? opts.onChange : null;

        /** @type {{ source: string, id: string, priority: boolean } | null} */
        this.globalActive = null;
        /** @type {Array<{ source: string, id: string, priority: boolean, fill_ratio?: number, buffer_force_promoted?: boolean, force_before_full?: boolean }>} */
        this.globalQueue = [];

        this._snap = { airband: {}, tetrapol: {} };
        this._busy = false;
        this._cmdQueue = Promise.resolve();
        this._heartbeatTimer = null;
        this._lastHeartbeatAt = 0;
        /** When current globalActive was granted (ms). */
        this._activeSinceMs = 0;
        /** Consecutive drained observations while still pinned active. */
        this._drainedTicks = 0;
        /** WS connectivity as seen by the monitor bridge. */
        this._sourceConnected = { airband: true, tetrapol: true };
        this._sourceDownSinceMs = { airband: 0, tetrapol: 0 };
    }

    start() {
        if (!this.enabled || this._heartbeatTimer) return;
        this._heartbeatTimer = setInterval(() => {
            this.sendHeartbeats().catch((err) => {
                this.log('heartbeat error:', err && err.message ? err.message : err);
            });
        }, HEARTBEAT_INTERVAL_MS);
        if (this._heartbeatTimer.unref) this._heartbeatTimer.unref();
        this.sendHeartbeats().catch(() => {});
    }

    stop() {
        if (this._heartbeatTimer) {
            clearInterval(this._heartbeatTimer);
            this._heartbeatTimer = null;
        }
    }

    snapshot() {
        const qFill = (v) => Math.floor((Number(v) || 0) * 100) / 100;
        // Stable client-facing shape: omit last_heartbeat_at / busy, quantize fills.
        let active = null;
        if (this.globalActive) {
            const a = this.globalActive;
            active = {
                source: a.source,
                id: a.id,
                priority: !!a.priority,
                fill_ratio: qFill(a.fill_ratio),
                buffer_force_promoted: !!a.buffer_force_promoted,
                force_before_full: !!a.force_before_full,
            };
        }
        return {
            enabled: this.enabled,
            monitor_alive: this.enabled,
            monitor_id: this.monitorId,
            lease_ms: HEARTBEAT_LEASE_MS,
            active,
            queue: this.globalQueue.map((s) => ({
                source: s.source,
                id: s.id,
                priority: !!s.priority,
                fill_ratio: qFill(s.fill_ratio),
                buffer_force_promoted: !!s.buffer_force_promoted,
            })),
            sources: { ...this._sourceConnected },
        };
    }

    notifyChange() {
        if (this.onChange) {
            try {
                this.onChange(this.snapshot());
            } catch (_) {
                /* ignore */
            }
        }
    }

    /** Soft telemetry from WS — used for fill/force metadata, not for fighting actives. */
    onStates(airbandPe, tetrapolPe) {
        this._snap.airband = airbandPe && typeof airbandPe === 'object' ? airbandPe : {};
        this._snap.tetrapol = tetrapolPe && typeof tetrapolPe === 'object' ? tetrapolPe : {};
        if (!this.enabled) return;
        this.refreshActiveMeta();
        this.maybeReleaseEndedActive();
        // Rebuild FIFO from project listen_groups when claim HTTP lagged / missed.
        const added = this.syncWaitersFromTelemetry();
        if (this._busy) return;
        if (!this.globalActive) {
            this.adoptLocalActivesIfIdle();
        }
        if (this.globalActive) {
            this.maybePriorityPreempt();
            this.maybeForcePromote();
        } else if (this.globalQueue.length > 0 || added > 0) {
            this.enqueueCommand(async () => {
                if (!this.globalActive && this.globalQueue.length > 0) {
                    await this.promoteNext();
                    this.notifyChange();
                }
            });
        }
    }

    /** Bridge connectivity callback. */
    onConnectivity(status = {}) {
        const now = Date.now();
        for (const source of ['airband', 'tetrapol']) {
            const connected = status[source] !== false;
            this._sourceConnected[source] = connected;
            if (connected) {
                this._sourceDownSinceMs[source] = 0;
            } else if (!this._sourceDownSinceMs[source]) {
                this._sourceDownSinceMs[source] = now;
            }
        }
        this.maybeFailoverFromDownSource();
    }

    maybeFailoverFromDownSource() {
        if (!this.enabled || !this.globalActive || this._busy) return;
        const src = this.globalActive.source;
        if (this._sourceConnected[src] !== false) return;
        const downSince = this._sourceDownSinceMs[src] || 0;
        if (!downSince || (Date.now() - downSince) < SOURCE_DOWN_FAILOVER_MS) return;

        const ended = { ...this.globalActive };
        this.log('source-down failover', this.fmtSlot(ended),
            `down_ms=${Date.now() - downSince} queue=${this.queueSummary()}`);
        this.enqueueCommand(async () => {
            this._busy = true;
            try {
                if (!this.globalActive
                    || keyOf(this.globalActive.source, this.globalActive.id) !== keyOf(ended.source, ended.id)) {
                    return;
                }
                this.removeFromQueue(ended);
                // Do not call yieldSource on a down source (would timeout and stall).
                this.globalActive = null;
                this._activeSinceMs = 0;
                this.syncWaitersFromTelemetry({
                    excludeKeys: new Set([keyOf(ended.source, ended.id)]),
                });
                await this.promoteNext();
                this.notifyChange();
            } finally {
                this._busy = false;
            }
        });
    }

    /** True if telemetry shows real audio (live or residual buffer) — not a hollow queue seat. */
    hasPlayableAudio(g) {
        if (!g) return false;
        if (g.on_air) return true;
        const buf = Number(g.buffer_seconds) || 0;
        if (buf >= 0.05) return true;
        const fill = Number(g.fill_ratio);
        return Number.isFinite(fill) && fill > 0.001;
    }

    /**
     * Drop waiters that no longer have audio (stale claims / emptied rings).
     * Keeps FIFO honest so promoteNext never activates fill=0% ghosts.
     */
    pruneDrainedWaiters() {
        if (!this.globalQueue.length) return 0;
        const kept = [];
        const dropped = [];
        for (const s of this.globalQueue) {
            const meta = this.groupMeta(this._snap[s.source] || {}, s.id);
            if (this.hasPlayableAudio(meta)) {
                s.fill_ratio = meta.fill_ratio;
                s.buffer_force_promoted = meta.buffer_force_promoted;
                s.priority = meta.priority || s.priority;
                kept.push(s);
            } else {
                dropped.push(s);
            }
        }
        if (!dropped.length) return 0;
        this.globalQueue = kept;
        this.log('prune empty waiters', `-${dropped.length}`,
            dropped.map((s) => this.fmtSlot(s)).join(', '),
            `| queue=${this.queueSummary()}`);
        return dropped.length;
    }

    /**
     * Monitor lancé après coup : les projets ont déjà un actif Pulse local.
     * L'adopter au lieu de le traiter comme un waiter (sinon yield + silence).
     * S'il y a déjà une ★ en file, on laisse promoteNext la grant.
     */
    adoptLocalActivesIfIdle() {
        if (this.globalActive) return false;
        if (this.globalQueue.some((s) => s.priority)) return false;
        const locals = [];
        for (const source of ['airband', 'tetrapol']) {
            const pe = this._snap[source] || {};
            if (pe.exclusive === false) continue;
            const id = pe.active_group_id ? String(pe.active_group_id) : '';
            if (!id) continue;
            const meta = this.groupMeta(pe, id);
            locals.push({
                source,
                id,
                priority: !!meta.priority,
                fill_ratio: meta.fill_ratio,
            });
        }
        if (!locals.length) return false;
        locals.sort((a, b) => (b.priority ? 1 : 0) - (a.priority ? 1 : 0));
        const slot = locals[0];
        this.removeFromQueue(slot);
        this.globalActive = {
            source: slot.source,
            id: slot.id,
            priority: !!slot.priority,
            fill_ratio: slot.fill_ratio,
        };
        this._activeSinceMs = Date.now();
        this._drainedTicks = 0;
        this.log('adopt local active (late monitor)', this.fmtSlot(this.globalActive),
            `| queue=${this.queueSummary()}`);
        this.notifyChange();
        return true;
    }

    /**
     * Pull playable waiters from Airband/Tetrapol pulse telemetry into globalQueue.
     * UI "file" often shows local queues while claims never reached the arbiter —
     * this closes that gap. Only seats with real audio are imported (not fill=0%).
     * @param {{ excludeKeys?: Set<string> }} [opts]
     */
    syncWaitersFromTelemetry(opts = {}) {
        this.pruneDrainedWaiters();
        const excludeKeys = opts.excludeKeys || new Set();
        const seen = new Set(this.globalQueue.map((s) => keyOf(s.source, s.id)));
        if (this.globalActive) {
            seen.add(keyOf(this.globalActive.source, this.globalActive.id));
        }
        for (const k of excludeKeys) seen.add(k);
        const candidates = [];

        const consider = (source, id, g, pe) => {
            const gid = String(id);
            if (!gid) return;
            const k = keyOf(source, gid);
            if (seen.has(k)) return;
            if (candidates.some((c) => keyOf(c.source, c.id) === k)) return;
            if (!this.hasPlayableAudio(g)) return;
            // Skip project's current active if it matches our commanded active.
            if (this.globalActive
                && keyOf(this.globalActive.source, this.globalActive.id) === k) {
                return;
            }
            const fill = Number(g.fill_ratio);
            candidates.push({
                source,
                id: gid,
                priority: !!(g.priority || (Array.isArray(pe.priority_groups)
                    && pe.priority_groups.map(String).includes(gid))),
                fill_ratio: Number.isFinite(fill) ? fill : 0,
                buffer_force_promoted: !!g.buffer_force_promoted,
                force_before_full: !!pe.force_before_full,
            });
        };

        for (const source of ['airband', 'tetrapol']) {
            const pe = this._snap[source] || {};
            if (pe.exclusive === false) continue;
            if (this._sourceConnected[source] === false) continue;
            // queue_order first — preserves project FIFO before listen_groups extras.
            const order = Array.isArray(pe.queue_order) ? pe.queue_order : [];
            for (const rawId of order) {
                const meta = this.groupMeta(pe, rawId);
                consider(source, rawId, meta, pe);
            }
            const list = Array.isArray(pe.listen_groups) ? pe.listen_groups : [];
            for (const g of list) {
                if (!g || g.group_id == null) continue;
                consider(source, g.group_id, g, pe);
            }
        }

        if (!candidates.length) {
            this.reorderQueueByPriority();
            return 0;
        }
        for (const c of candidates) {
            this.enqueue(c);
            seen.add(keyOf(c.source, c.id));
        }
        this.reorderQueueByPriority();
        this.log('sync waiters ← telemetry', `+${candidates.length}`,
            candidates.map((c) => this.fmtSlot(c)).join(', '),
            `| queue=${this.queueSummary()}`);
        return candidates.length;
    }

    refreshActiveMeta() {
        if (!this.globalActive) return;
        const pe = this._snap[this.globalActive.source] || {};
        const meta = this.groupMeta(pe, this.globalActive.id);
        const wasForcePromoted = !!this.globalActive.buffer_force_promoted;
        this.globalActive.priority = meta.priority || this.globalActive.priority;
        this.globalActive.fill_ratio = meta.fill_ratio;
        this.globalActive.force_before_full = meta.force_before_full;
        // Sticky force-promote until unread < 80 % (telemetry may clear the flag early).
        if (wasForcePromoted && meta.fill_ratio >= YIELD_MAX_UNREAD_RATIO) {
            this.globalActive.buffer_force_promoted = true;
        } else if (meta.fill_ratio < YIELD_MAX_UNREAD_RATIO) {
            this.globalActive.buffer_force_promoted = !!meta.buffer_force_promoted;
        } else {
            this.globalActive.buffer_force_promoted = !!(meta.buffer_force_promoted || wasForcePromoted);
        }
    }

    /**
     * If the commanded active disappeared on the project (EOF / silence), promote next.
     * Also promote when the project still pins the id but the buffer is drained and quiet
     * (stuck "live" with no com).
     */
    maybeReleaseEndedActive() {
        if (!this.globalActive || this._busy) return;
        const pe = this._snap[this.globalActive.source] || {};
        if (!pe.exclusive) return;
        // Grace after every grant — snap lags behind activate ACK and would
        // otherwise false-end a channel that still has local buffer.
        const age = Date.now() - (this._activeSinceMs || 0);
        if (age < 3000) return;

        const activeId = pe.active_group_id ? String(pe.active_group_id) : '';
        const want = String(this.globalActive.id);
        const meta = this.groupMeta(pe, want);
        const drained = !meta.on_air && meta.buffer_seconds < 0.05 && !meta.suspended;

        // Still commanded active but drained / quiet → force release + promote.
        if (activeId === want) {
            if (!drained) {
                this._drainedTicks = 0;
                return;
            }
            this._drainedTicks = (this._drainedTicks || 0) + 1;
            if (this._drainedTicks < 2) return;
            const ended = { ...this.globalActive };
            const src = ended.source;
            this._drainedTicks = 0;
            this.log('eof-detect (still pinned, drained)', this.fmtSlot(ended),
                `| queue=${this.queueSummary()}`);
            this.enqueueCommand(async () => {
                if (!this.globalActive
                    || keyOf(this.globalActive.source, this.globalActive.id) !== keyOf(ended.source, ended.id)) {
                    return;
                }
                this.removeFromQueue(ended);
                this.globalActive = null;
                this._activeSinceMs = 0;
                await this.yieldSource(src);
                await this.promoteNext();
                this.notifyChange();
            });
            return;
        }
        this._drainedTicks = 0;

        const stillWanted = meta.on_air || meta.buffer_seconds >= 0.05 || meta.suspended
            || meta.fill_ratio > 0.001;
        if (stillWanted && !activeId) return; // activate in flight / held empty briefly
        if (activeId && activeId !== want) {
            // Foreign spontaneous active on our project — re-assert authority.
            const src = this.globalActive.source;
            const expected = { ...this.globalActive };
            this.log('re-assert (foreign active)', `${src}#${activeId}`,
                `want=${this.fmtSlot(expected)}`);
            this.enqueueCommand(async () => {
                if (!this.globalActive
                    || keyOf(this.globalActive.source, this.globalActive.id)
                        !== keyOf(expected.source, expected.id)) {
                    return;
                }
                await this.yieldSource(src);
                await this.activate(this.globalActive);
            });
            return;
        }
        // Active cleared and group quiet → release and promote.
        if (!activeId && !stillWanted) {
            const ended = { ...this.globalActive };
            this.log('eof-detect (active cleared locally)', this.fmtSlot(ended),
                `| queue=${this.queueSummary()}`);
            this.enqueueCommand(async () => {
                if (!this.globalActive
                    || keyOf(this.globalActive.source, this.globalActive.id) !== keyOf(ended.source, ended.id)) {
                    return;
                }
                this.removeFromQueue(ended);
                this.globalActive = null;
                this._activeSinceMs = 0;
                await this.promoteNext();
                this.notifyChange();
            });
        }
    }

    groupMeta(pe, id) {
        const gid = String(id);
        const list = Array.isArray(pe.listen_groups) ? pe.listen_groups : [];
        const g = list.find((x) => String(x.group_id) === gid) || {};
        const fill = Number(g.fill_ratio);
        return {
            priority: !!(g.priority || (Array.isArray(pe.priority_groups) && pe.priority_groups.map(String).includes(gid))),
            fill_ratio: Number.isFinite(fill) ? fill : Number(pe.fill_ratios && pe.fill_ratios[gid]) || 0,
            buffer_seconds: Number(g.buffer_seconds) || 0,
            buffer_force_promoted: !!g.buffer_force_promoted,
            on_air: !!g.on_air,
            suspended: !!g.suspended,
            in_queue: !!g.in_queue,
            force_before_full: !!pe.force_before_full,
        };
    }

    /** Refresh priority flags from telemetry and move ★ waiters ahead of normal ones. */
    reorderQueueByPriority() {
        if (!this.globalQueue.length) return;
        const prio = [];
        const normal = [];
        for (const s of this.globalQueue) {
            const meta = this.groupMeta(this._snap[s.source] || {}, s.id);
            s.priority = !!(s.priority || meta.priority);
            s.fill_ratio = meta.fill_ratio;
            (s.priority ? prio : normal).push(s);
        }
        this.globalQueue = prio.concat(normal);
    }

    /** « Bascule avant fin tampon » enabled on at least one project. */
    isForceBeforeFullEnabled() {
        const ab = this._snap.airband || {};
        const tp = this._snap.tetrapol || {};
        return !!(ab.force_before_full || tp.force_before_full);
    }

    isSaturatedSlot(source, id, meta) {
        const pe = this._snap[source] || {};
        if (!pe.force_before_full) return false;
        const m = meta || this.groupMeta(pe, id);
        const fill = Number(m.fill_ratio) || 0;
        return fill >= FORCE_PROMOTE_RATIO && this.hasPlayableAudio(m);
    }

    /**
     * Can another channel preempt this active? Always uses fresh telemetry fill.
     * Backlog ≥ 80 % → no preemption (★ or saturated waiter).
     * ★ : peut couper une com encore on_air (comme Airband local / cartes Icecast).
     * Force-promote : seulement si l'actif n'est plus on_air.
     */
    canPreemptActive(active, { priorityPreempt = false } = {}) {
        if (!active) return true;
        const pe = this._snap[active.source] || {};
        const meta = this.groupMeta(pe, active.id);

        const age = Date.now() - (this._activeSinceMs || 0);
        const grace = priorityPreempt ? PRIORITY_PREEMPT_GRACE_MS : PROMOTION_GRACE_MS;
        if (age < grace) return false;

        const fill = Number(meta.fill_ratio);
        if (Number.isFinite(fill) && fill >= YIELD_MAX_UNREAD_RATIO) return false;

        if (!priorityPreempt && meta.on_air) return false;

        return true;
    }

    /** Move the first saturated (≥92 %) waiter to the front when idle promote runs. */
    pullSaturatedWaiterForward() {
        if (!this.isForceBeforeFullEnabled()) return;
        const idx = this.globalQueue.findIndex((s) => this.isSaturatedSlot(s.source, s.id));
        if (idx > 0) {
            const [sat] = this.globalQueue.splice(idx, 1);
            this.globalQueue.unshift(sat);
        }
    }

    /** First queued / telemetry waiter that should force-promote (FIFO scan). */
    findForcePromoteCandidate() {
        const seen = new Set();
        const trySlot = (source, id) => {
            const k = keyOf(source, id);
            if (seen.has(k)) return null;
            seen.add(k);
            if (this.globalActive
                && keyOf(this.globalActive.source, this.globalActive.id) === k) {
                return null;
            }
            if (!this.isSaturatedSlot(source, id)) return null;
            const meta = this.groupMeta(this._snap[source] || {}, id);
            return {
                source,
                id: String(id),
                priority: meta.priority,
                fill_ratio: meta.fill_ratio,
                buffer_force_promoted: true,
                force_before_full: true,
            };
        };

        for (const s of this.globalQueue) {
            const c = trySlot(s.source, s.id);
            if (c) return c;
        }
        for (const source of ['airband', 'tetrapol']) {
            const pe = this._snap[source] || {};
            if (pe.exclusive === false || !pe.force_before_full) continue;
            for (const rawId of (pe.queue_order || [])) {
                const c = trySlot(source, rawId);
                if (c) return c;
            }
            for (const g of (pe.listen_groups || [])) {
                if (!g || g.group_id == null) continue;
                const c = trySlot(source, g.group_id);
                if (c) return c;
            }
        }
        return null;
    }

    /**
     * « Bascule avant fin tampon » : play a near-full buffer (≥92 %) over the
     * current active once it is quiet and unread < 80 %.
     */
    maybeForcePromote() {
        if (this._busy || !this.isForceBeforeFullEnabled()) return;
        const candidate = this.findForcePromoteCandidate();
        if (!candidate) return;

        if (!this.globalActive) {
            this.enqueueCommand(async () => {
                this._busy = true;
                try {
                    if (this.globalActive) return;
                    const c = this.findForcePromoteCandidate();
                    if (!c) return;
                    this.removeFromQueue(c);
                    this.globalActive = {
                        source: c.source,
                        id: c.id,
                        priority: !!c.priority,
                        fill_ratio: c.fill_ratio,
                        buffer_force_promoted: true,
                    };
                    this._activeSinceMs = Date.now();
                    this._drainedTicks = 0;
                    this.log('force-promote → GRANT (idle)', this.fmtSlot(c));
                    await this.activate(c);
                    this.notifyChange();
                } finally {
                    this._busy = false;
                }
            });
            return;
        }

        const active = {
            ...this.globalActive,
            ...this.groupMeta(this._snap[this.globalActive.source] || {}, this.globalActive.id),
            force_before_full: !!(this._snap[this.globalActive.source] || {}).force_before_full,
        };
        if (!this.canPreemptActive(active)) return;

        const target = { ...candidate };
        const victim = { ...active };
        this.enqueueCommand(async () => {
            this._busy = true;
            try {
                if (!this.globalActive
                    || keyOf(this.globalActive.source, this.globalActive.id)
                        !== keyOf(victim.source, victim.id)) {
                    return;
                }
                const cur = {
                    ...this.globalActive,
                    ...this.groupMeta(this._snap[this.globalActive.source] || {}, this.globalActive.id),
                    force_before_full: !!(this._snap[this.globalActive.source] || {}).force_before_full,
                };
                if (!this.canPreemptActive(cur)) return;
                const c = this.findForcePromoteCandidate();
                if (!c) return;
                await this.preemptActiveWith(c, { reason: 'force-promote' });
            } finally {
                this._busy = false;
            }
        });
    }

    async preemptActiveWith(slot, { reason = 'preempt' } = {}) {
        const active = {
            ...this.globalActive,
            ...this.groupMeta(this._snap[this.globalActive.source] || {}, this.globalActive.id),
            force_before_full: !!(this._snap[this.globalActive.source] || {}).force_before_full,
        };
        this.log(reason, this.fmtSlot(slot),
            `over ${this.fmtSlot(active)}`, `| queue=${this.queueSummary()}`);
        this.enqueue(active, { front: true });
        this.removeFromQueue(slot);
        this.globalActive = {
            source: slot.source,
            id: slot.id,
            priority: !!slot.priority,
            fill_ratio: slot.fill_ratio,
            buffer_force_promoted: !!slot.buffer_force_promoted,
        };
        this._activeSinceMs = Date.now();
        this._drainedTicks = 0;
        await this.yieldSource(active.source);
        await this.activate(slot);
        this.notifyChange();
    }

    /**
     * Priority cuts in over a non-priority active (cross-project), mirroring
     * Tetrapol/Airband local preempt when monitor owns the FIFO.
     */
    maybePriorityPreempt() {
        if (!this.globalActive || this._busy) return;
        this.reorderQueueByPriority();
        const active = {
            ...this.globalActive,
            ...this.groupMeta(this._snap[this.globalActive.source] || {}, this.globalActive.id),
            force_before_full: !!(this._snap[this.globalActive.source] || {}).force_before_full,
        };
        if (active.priority) return;

        let waiter = null;
        for (const s of this.globalQueue) {
            if (!s.priority) continue;
            const meta = this.groupMeta(this._snap[s.source] || {}, s.id);
            if (!this.hasPlayableAudio(meta)) continue;
            waiter = { ...s, ...meta, priority: true };
            break;
        }
        if (!waiter || !this.canPreemptActive(active, { priorityPreempt: true })) return;

        const target = { ...waiter };
        const victim = { ...active };
        this.enqueueCommand(async () => {
            this._busy = true;
            try {
                if (!this.globalActive
                    || keyOf(this.globalActive.source, this.globalActive.id)
                        !== keyOf(victim.source, victim.id)) {
                    return;
                }
                const cur = {
                    ...this.globalActive,
                    ...this.groupMeta(this._snap[this.globalActive.source] || {}, this.globalActive.id),
                    force_before_full: !!(this._snap[this.globalActive.source] || {}).force_before_full,
                };
                if (cur.priority || !this.canPreemptActive(cur, { priorityPreempt: true })) return;
                const meta = this.groupMeta(this._snap[target.source] || {}, target.id);
                if (!(target.priority || meta.priority)) return;
                if (!this.hasPlayableAudio(meta)) return;

                await this.preemptActiveWith(
                    { ...target, ...meta, priority: true },
                    { reason: 'prio-preempt' },
                );
            } finally {
                this._busy = false;
            }
        });
    }

    enqueue(slot, { front = false } = {}) {
        if (!slot || !slot.id) return;
        const k = keyOf(slot.source, slot.id);
        this.globalQueue = this.globalQueue.filter((s) => keyOf(s.source, s.id) !== k);
        const pe = this._snap[slot.source] || {};
        const meta = this.groupMeta(pe, slot.id);
        const entry = {
            source: slot.source,
            id: String(slot.id),
            priority: !!(slot.priority != null ? slot.priority : meta.priority),
            fill_ratio: slot.fill_ratio != null ? slot.fill_ratio : meta.fill_ratio,
            buffer_force_promoted: !!slot.buffer_force_promoted,
            force_before_full: !!slot.force_before_full,
        };
        if (front) {
            this.globalQueue.unshift(entry);
        } else if (entry.priority) {
            // Same rule as Tetrapol enqueue_locked: ★ before first non-★.
            const idx = this.globalQueue.findIndex((s) => !s.priority);
            if (idx === -1) this.globalQueue.push(entry);
            else this.globalQueue.splice(idx, 0, entry);
        } else {
            this.globalQueue.push(entry);
        }
    }

    removeFromQueue(slot) {
        if (!slot) return;
        const k = keyOf(slot.source, slot.id);
        this.globalQueue = this.globalQueue.filter((s) => keyOf(s.source, s.id) !== k);
    }

    enqueueCommand(fn) {
        const run = this._cmdQueue.then(() => fn());
        this._cmdQueue = run.catch((err) => {
            this.log('cmd error:', err && err.message ? err.message : err);
        });
        return run;
    }

    fmtSlot(slot) {
        if (!slot) return '—';
        const src = slot.source === 'tetrapol' ? 'TP' : 'AB';
        const prio = slot.priority ? '★' : '';
        const fill = slot.fill_ratio != null && Number.isFinite(Number(slot.fill_ratio))
            ? ` fill=${(Number(slot.fill_ratio) * 100).toFixed(0)}%`
            : '';
        return `${src}#${slot.id}${prio}${fill}`;
    }

    queueSummary() {
        if (!this.globalQueue.length) return '[]';
        return '[' + this.globalQueue.map((s) => this.fmtSlot(s)).join(' → ') + ']';
    }

    /**
     * POST to a project. Heartbeats are silent; other commands log at call sites.
     */
    async post(source, body, { silent = false } = {}) {
        const base = source === 'tetrapol' ? this.tetrapolBase : this.airbandBase;
        const isHb = !!(body && body.monitor_heartbeat);
        if (!silent && !isHb) {
            this.log('POST', source, body);
        }
        return postJson(base, '/api/pulse-exclusive', body);
    }

    async yieldSource(source) {
        this.log('yield', source === 'tetrapol' ? 'TP' : 'AB',
            `| active=${this.fmtSlot(this.globalActive)} queue=${this.queueSummary()}`);
        if (source === 'airband') {
            await this.post('airband', { yield_active: true, manual_hold: true }, { silent: true });
        } else {
            await this.post('tetrapol', {
                yield_active: true,
                active_group_id: '',
                manual_hold: true,
            }, { silent: true });
        }
    }

    async activate(slot) {
        if (!slot) return;
        this.log('promote/activate', this.fmtSlot(slot),
            `| queue=${this.queueSummary()}`);
        // Clear peer first so Tetrapol/Airband do not briefly fight, then grant.
        await this.ensureOtherHeldEmpty(slot.source);
        await this.post(slot.source, {
            active_group_id: String(slot.id),
            manual_hold: true,
        }, { silent: true });
    }

    async ensureOtherHeldEmpty(activeSource) {
        const other = activeSource === 'airband' ? 'tetrapol' : 'airband';
        const pe = this._snap[other] || {};
        // Only clear a foreign active — do NOT force manual_hold on the peer.
        if (pe.active_group_id) {
            this.log('peer-busy → yield', other, `#${pe.active_group_id}`);
            await this.yieldSource(other);
            return true;
        }
        return false;
    }

    async sendHeartbeats() {
        if (!this.enabled) return;
        const ab = this._snap.airband || {};
        const tp = this._snap.tetrapol || {};
        const anyExclusive = !!(ab.exclusive || tp.exclusive);
        if (!anyExclusive) return;

        // Presence only — silent (no log spam every 1s).
        const body = {
            monitor_heartbeat: true,
            monitor_id: this.monitorId,
            lease_ms: HEARTBEAT_LEASE_MS,
        };
        this._lastHeartbeatAt = Date.now();
        await Promise.allSettled([
            ab.exclusive ? this.post('airband', body, { silent: true }) : Promise.resolve(),
            tp.exclusive ? this.post('tetrapol', body, { silent: true }) : Promise.resolve(),
        ]);
    }

    async promoteNext() {
        // Refresh from project telemetry, drop hollow fill=0% seats first.
        this.syncWaitersFromTelemetry();
        this.reorderQueueByPriority();
        this.pullSaturatedWaiterForward();
        while (this.globalQueue.length) {
            const next = this.globalQueue.shift();
            if (!next) break;
            const pe = this._snap[next.source] || {};
            if (pe.exclusive === false) {
                this.log('promoteNext skip (exclusive off)', this.fmtSlot(next));
                continue;
            }
            const meta = this.groupMeta(pe, next.id);
            if (!this.hasPlayableAudio(meta)) {
                this.log('promoteNext skip (empty/no audio)', this.fmtSlot({ ...next, ...meta }),
                    `| remaining=${this.queueSummary()}`);
                continue;
            }

            this.globalActive = {
                source: next.source,
                id: next.id,
                priority: !!(next.priority || meta.priority),
                fill_ratio: meta.fill_ratio,
                buffer_force_promoted: !!(next.buffer_force_promoted || meta.buffer_force_promoted
                    || (this.isSaturatedSlot(next.source, next.id, meta) && (this._snap[next.source] || {}).force_before_full)),
            };
            this._activeSinceMs = Date.now();
            this._drainedTicks = 0;
            this.log('promoteNext →', this.fmtSlot(this.globalActive),
                `| remaining=${this.queueSummary()}`);
            await this.activate({ ...next, ...meta });
            return true;
        }
        this.log('promoteNext → idle (queue empty)');
        this.globalActive = null;
        this._activeSinceMs = 0;
        return false;
    }

    /**
     * Project asks to play a channel (exclusive mode + monitor alive).
     */
    async claim(body) {
        if (!this.enabled) {
            return { granted: true, action: 'play', arbiter: this.snapshot(), note: 'arbiter_disabled' };
        }
        const source = body && body.source === 'tetrapol' ? 'tetrapol' : 'airband';
        const id = body && body.id != null ? String(body.id) : '';
        if (!id) {
            return { ok: false, error: 'missing id', granted: false, action: 'wait', arbiter: this.snapshot() };
        }

        const pe = this._snap[source] || {};
        const meta = this.groupMeta(pe, id);
        const reason = body && body.reason ? String(body.reason) : 'com';
        const slot = {
            source,
            id,
            priority: !!(body.priority != null ? body.priority : meta.priority),
            fill_ratio: body.fill_ratio != null ? Number(body.fill_ratio) : meta.fill_ratio,
            buffer_force_promoted: !!(body.buffer_force_promoted != null
                ? body.buffer_force_promoted
                : meta.buffer_force_promoted),
            force_before_full: !!(body.force_before_full != null
                ? body.force_before_full
                : pe.force_before_full),
        };

        this.log('com/claim', this.fmtSlot(slot), `reason=${reason}`,
            `| active=${this.fmtSlot(this.globalActive)} queue=${this.queueSummary()}`);

        return this.enqueueCommand(async () => {
            this._busy = true;
            try {
                // Already the global active.
                if (this.globalActive
                    && keyOf(this.globalActive.source, this.globalActive.id) === keyOf(source, id)) {
                    this.log('claim → re-assert play', this.fmtSlot(slot));
                    await this.activate(slot);
                    return {
                        ok: true,
                        granted: true,
                        action: 'play',
                        active: { ...this.globalActive },
                        arbiter: this.snapshot(),
                    };
                }

                // No active → grant immediately.
                if (!this.globalActive) {
                    this.removeFromQueue(slot);
                    this.globalActive = {
                        source: slot.source,
                        id: slot.id,
                        priority: !!slot.priority,
                    };
                    this._activeSinceMs = Date.now();
                    this._drainedTicks = 0;
                    this.log('claim → GRANT (idle)', this.fmtSlot(slot));
                    await this.activate(slot);
                    this.notifyChange();
                    return {
                        ok: true,
                        granted: true,
                        action: 'play',
                        active: { ...this.globalActive },
                        arbiter: this.snapshot(),
                    };
                }

                const active = {
                    ...this.globalActive,
                    ...this.groupMeta(this._snap[this.globalActive.source] || {}, this.globalActive.id),
                    force_before_full: !!(this._snap[this.globalActive.source] || {}).force_before_full,
                };

                // Priority preempts non-priority if active can yield (< 80 %, quiet).
                if (slot.priority && !active.priority
                    && this.canPreemptActive(active, { priorityPreempt: true })) {
                    this.log('claim → PREEMPT', this.fmtSlot(slot),
                        `over ${this.fmtSlot(active)}`);
                    await this.preemptActiveWith(slot, { reason: 'claim → PREEMPT' });
                    return {
                        ok: true,
                        granted: true,
                        action: 'play',
                        preempted: keyOf(active.source, active.id),
                        active: { ...this.globalActive },
                        arbiter: this.snapshot(),
                    };
                }

                // « Bascule avant fin tampon » : saturated buffer (≥92 %) cuts in when
                // the active is quiet and unread < 80 % (may displace a ★ if it can yield).
                const forcePromote = slot.force_before_full
                    && Number(slot.fill_ratio) >= FORCE_PROMOTE_RATIO;
                if (forcePromote && this.canPreemptActive(active)) {
                    slot.buffer_force_promoted = true;
                    this.log('claim → FORCE-PROMOTE', this.fmtSlot(slot),
                        `over ${this.fmtSlot(active)}`);
                    await this.preemptActiveWith(slot, { reason: 'claim → FORCE-PROMOTE' });
                    return {
                        ok: true,
                        granted: true,
                        action: 'play',
                        preempted: keyOf(active.source, active.id),
                        active: { ...this.globalActive },
                        arbiter: this.snapshot(),
                    };
                }

                if (slot.priority && active.priority) {
                    this.log('claim → wait (active also prio)', this.fmtSlot(slot),
                        `behind ${this.fmtSlot(active)}`);
                } else if (slot.priority && !this.canPreemptActive(active, { priorityPreempt: true })) {
                    this.log('claim → wait (active cannot yield ≥80%)', this.fmtSlot(slot),
                        `behind ${this.fmtSlot(active)}`);
                } else {
                    this.log('claim → WAIT fifo', this.fmtSlot(slot),
                        `behind ${this.fmtSlot(active)}`);
                }

                // Otherwise wait in FIFO.
                this.enqueue(slot, { front: false });
                // Ensure claimant is not spontaneously playing.
                if (String((this._snap[source] || {}).active_group_id || '') === id) {
                    await this.yieldSource(source);
                }
                this.notifyChange();
                return {
                    ok: true,
                    granted: false,
                    action: 'wait',
                    active: this.globalActive ? { ...this.globalActive } : null,
                    arbiter: this.snapshot(),
                };
            } finally {
                this._busy = false;
            }
        });
    }

    async release(body) {
        if (!this.enabled) {
            return { ok: true, arbiter: this.snapshot() };
        }
        const source = body && body.source === 'tetrapol' ? 'tetrapol' : 'airband';
        const id = body && body.id != null ? String(body.id) : '';
        if (!id) {
            return { ok: false, error: 'missing id', arbiter: this.snapshot() };
        }
        const reason = body && body.reason ? String(body.reason) : 'eof';

        return this.enqueueCommand(async () => {
            this._busy = true;
            try {
                const slot = { source, id };
                const isActive = this.globalActive
                    && keyOf(this.globalActive.source, this.globalActive.id) === keyOf(source, id);

                const inQueue = this.globalQueue.some(
                    (s) => keyOf(s.source, s.id) === keyOf(source, id)
                );
                this.log('release', this.fmtSlot(slot), `reason=${reason}`,
                    isActive ? '(was ACTIVE → promoteNext)'
                        : (inQueue ? '(non-active, seat kept)' : '(orphan / not in arbiter queue)'),
                    `| queue=${this.queueSummary()}`);

                const excludeEnded = { excludeKeys: new Set([keyOf(source, id)]) };
                if (isActive) {
                    this.removeFromQueue(slot);
                    this.globalActive = null;
                    this._activeSinceMs = 0;
                    await this.yieldSource(source);
                    // Rebuild FIFO from project telemetry (claims may have lagged).
                    this.syncWaitersFromTelemetry(excludeEnded);
                    await this.promoteNext();
                } else {
                    // Non-active eof: keep any existing waiter seat, but always resync
                    // from local project queues (UI "file" ≠ arbiter globalQueue).
                    this.syncWaitersFromTelemetry(excludeEnded);
                    if (!this.globalActive && this.globalQueue.length) {
                        await this.promoteNext();
                    }
                }
                this.notifyChange();
                return { ok: true, arbiter: this.snapshot() };
            } finally {
                this._busy = false;
            }
        });
    }

    async clearBuffer(body) {
        const source = body && body.source === 'tetrapol' ? 'tetrapol' : 'airband';
        const id = body && body.id != null ? String(body.id) : '';
        if (!id) return { ok: false, error: 'missing id' };
        this.log('clear_buffer', this.fmtSlot({ source, id }));
        await this.post(source, { clear_buffer: id }, { silent: true });
        return { ok: true, arbiter: this.snapshot() };
    }

    /** UI « ▶ » : force play now (preempt current, keep previous in front of queue). */
    async forcePlay(body) {
        if (!this.enabled) {
            return { ok: true, granted: true, action: 'play', note: 'arbiter_disabled', arbiter: this.snapshot() };
        }
        const source = body && body.source === 'tetrapol' ? 'tetrapol' : 'airband';
        const id = body && body.id != null ? String(body.id) : '';
        if (!id) {
            return { ok: false, error: 'missing id', arbiter: this.snapshot() };
        }
        const pe = this._snap[source] || {};
        const meta = this.groupMeta(pe, id);
        const slot = {
            source,
            id,
            priority: !!(body.priority != null ? body.priority : meta.priority),
            fill_ratio: meta.fill_ratio,
            buffer_force_promoted: meta.buffer_force_promoted,
            force_before_full: !!pe.force_before_full,
        };

        return this.enqueueCommand(async () => {
            this._busy = true;
            try {
                const want = keyOf(source, id);
                this.log('forcePlay', this.fmtSlot(slot),
                    `| was=${this.fmtSlot(this.globalActive)}`);
                if (this.globalActive && keyOf(this.globalActive.source, this.globalActive.id) !== want) {
                    this.enqueue(this.globalActive, { front: true });
                    await this.yieldSource(this.globalActive.source);
                }
                this.removeFromQueue(slot);
                this.globalActive = {
                    source,
                    id,
                    priority: !!slot.priority,
                };
                this._activeSinceMs = Date.now();
                this._drainedTicks = 0;
                await this.activate(slot);
                this.notifyChange();
                return {
                    ok: true,
                    granted: true,
                    action: 'play',
                    active: { ...this.globalActive },
                    arbiter: this.snapshot(),
                };
            } finally {
                this._busy = false;
            }
        });
    }

    /** UI « ⏹ Stop » : yield active, do not auto-promote. */
    async stopActive() {
        return this.enqueueCommand(async () => {
            this._busy = true;
            try {
                this.log('stop', this.fmtSlot(this.globalActive), `| queue=${this.queueSummary()}`);
                if (this.globalActive) {
                    const src = this.globalActive.source;
                    this.globalActive = null;
                    await this.yieldSource(src);
                }
                await Promise.allSettled([
                    this.post('airband', { stop_active: true, manual_hold: true }, { silent: true }),
                    this.post('tetrapol', {
                        yield_active: true,
                        active_group_id: '',
                        manual_hold: true,
                    }, { silent: true }),
                ]);
                this.notifyChange();
                return { ok: true, arbiter: this.snapshot() };
            } finally {
                this._busy = false;
            }
        });
    }

    /** UI « ↻ Auto » : clear holds and promote next waiter if nothing active. */
    async resumeAuto() {
        return this.enqueueCommand(async () => {
            this._busy = true;
            try {
                this.log('auto', `| active=${this.fmtSlot(this.globalActive)} queue=${this.queueSummary()}`);
                await Promise.allSettled([
                    this.post('airband', { manual_hold: false }, { silent: true }),
                    this.post('tetrapol', { manual_hold: false }, { silent: true }),
                ]);
                this.syncWaitersFromTelemetry();
                if (!this.globalActive) {
                    await this.promoteNext();
                }
                this.notifyChange();
                return { ok: true, arbiter: this.snapshot() };
            } finally {
                this._busy = false;
            }
        });
    }
}

module.exports = {
    PulseArbiter,
    YIELD_MAX_UNREAD_RATIO,
    FORCE_PROMOTE_RATIO,
    HEARTBEAT_INTERVAL_MS,
    HEARTBEAT_LEASE_MS,
    postJson,
};
