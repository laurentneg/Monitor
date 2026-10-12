'use strict';

const path = require('path');
const http = require('http');
const express = require('express');
const { createProxyMiddleware } = require('http-proxy-middleware');
const { WsBridge } = require('./ws-bridge');

const CONFIG = {
    listenHost: process.env.MONITOR_HOST || '0.0.0.0',
    listenPort: Number(process.env.MONITOR_PORT || 8082),
    airbandBase: process.env.AIRBAND_BASE || 'http://127.0.0.1:8081',
    tetrapolBase: process.env.TETRAPOL_BASE || 'http://127.0.0.1:8080',
    icecastBase: process.env.ICECAST_BASE || 'http://127.0.0.1:8000',
    /**
     * Public Icecast URL for browsers (LAN IP / hostname).
     * When set, UI loads mounts directly from Icecast instead of /stream proxy.
     * Example: http://192.168.0.12:8000
     */
    icecastPublicBase: (process.env.ICECAST_PUBLIC_BASE || '').replace(/\/$/, ''),
    /**
     * When true, LAN :8082 UI may open Icecast directly (needs CORS on Icecast).
     * Default false — use same-origin /stream (monitor or Apache proxy) to avoid
     * net::ERR_EMPTY_RESPONSE from cross-origin IcecastMetadataPlayer fetches.
     */
    icecastBrowserDirect: process.env.ICECAST_BROWSER_DIRECT === '1',
    /** Keep /stream proxy (default on). Set STREAM_PROXY=0 to disable when using direct Icecast. */
    streamProxy: process.env.STREAM_PROXY !== '0',
    airbandWsUrl: process.env.AIRBAND_WS_URL || 'ws://127.0.0.1:8081/ws/monitor',
    tetrapolWsUrl: process.env.TETRAPOL_WS_URL || 'ws://127.0.0.1:8080/ws/monitor',
    /** API proxy timeout (ms). Streams keep 0 = no timeout. */
    apiProxyTimeoutMs: Number(process.env.API_PROXY_TIMEOUT_MS || 15000),
    wwwRoot: path.join(__dirname, 'www'),
};

const app = express();

// Pulse claim/release must be parsed before proxies and static.
app.use('/api/pulse', express.json({ limit: '64kb' }));

app.get('/api/pulse/arbiter', (_req, res) => {
    res.setHeader('Cache-Control', 'no-store');
    if (!bridge || !bridge.arbiter) {
        res.status(503).json({ ok: false, enabled: false });
        return;
    }
    res.json({ ok: true, ...bridge.arbiter.snapshot() });
});

app.post('/api/pulse/claim', async (req, res) => {
    res.setHeader('Cache-Control', 'no-store');
    if (!bridge || !bridge.arbiter) {
        res.status(503).json({ ok: false, granted: true, action: 'play', note: 'arbiter_disabled' });
        return;
    }
    try {
        const result = await bridge.arbiter.claim(req.body || {});
        res.json(result);
    } catch (err) {
        res.status(500).json({ ok: false, error: String(err && err.message ? err.message : err) });
    }
});

app.post('/api/pulse/release', async (req, res) => {
    res.setHeader('Cache-Control', 'no-store');
    if (!bridge || !bridge.arbiter) {
        res.status(503).json({ ok: false, note: 'arbiter_disabled' });
        return;
    }
    try {
        const result = await bridge.arbiter.release(req.body || {});
        res.json(result);
    } catch (err) {
        res.status(500).json({ ok: false, error: String(err && err.message ? err.message : err) });
    }
});

app.post('/api/pulse/clear', async (req, res) => {
    res.setHeader('Cache-Control', 'no-store');
    if (!bridge || !bridge.arbiter) {
        res.status(503).json({ ok: false, note: 'arbiter_disabled' });
        return;
    }
    try {
        const result = await bridge.arbiter.clearBuffer(req.body || {});
        res.json(result);
    } catch (err) {
        res.status(500).json({ ok: false, error: String(err && err.message ? err.message : err) });
    }
});

app.post('/api/pulse/play', async (req, res) => {
    res.setHeader('Cache-Control', 'no-store');
    if (!bridge || !bridge.arbiter) {
        res.status(503).json({ ok: false, note: 'arbiter_disabled' });
        return;
    }
    try {
        const result = await bridge.arbiter.forcePlay(req.body || {});
        res.json(result);
    } catch (err) {
        res.status(500).json({ ok: false, error: String(err && err.message ? err.message : err) });
    }
});

app.post('/api/pulse/stop', async (req, res) => {
    res.setHeader('Cache-Control', 'no-store');
    if (!bridge || !bridge.arbiter) {
        res.status(503).json({ ok: false, note: 'arbiter_disabled' });
        return;
    }
    try {
        const result = await bridge.arbiter.disableActive();
        res.json(result);
    } catch (err) {
        res.status(500).json({ ok: false, error: String(err && err.message ? err.message : err) });
    }
});

app.post('/api/pulse/pause', async (req, res) => {
    res.setHeader('Cache-Control', 'no-store');
    if (!bridge || !bridge.arbiter) {
        res.status(503).json({ ok: false, note: 'arbiter_disabled' });
        return;
    }
    try {
        const result = await bridge.arbiter.pauseActive();
        res.json(result);
    } catch (err) {
        res.status(500).json({ ok: false, error: String(err && err.message ? err.message : err) });
    }
});

app.post('/api/pulse/auto', async (req, res) => {
    res.setHeader('Cache-Control', 'no-store');
    if (!bridge || !bridge.arbiter) {
        res.status(503).json({ ok: false, note: 'arbiter_disabled' });
        return;
    }
    try {
        const result = await bridge.arbiter.resumeAuto();
        res.json(result);
    } catch (err) {
        res.status(500).json({ ok: false, error: String(err && err.message ? err.message : err) });
    }
});

app.use(express.static(CONFIG.wwwRoot));

function disableProxyBuffering(_proxyRes, _req, res) {
    res.setHeader('X-Accel-Buffering', 'no');
    res.setHeader('Cache-Control', 'no-store');
}

const apiProxyCommon = {
    changeOrigin: true,
    ws: false,
    proxyTimeout: CONFIG.apiProxyTimeoutMs,
    timeout: CONFIG.apiProxyTimeoutMs,
    on: { proxyRes: disableProxyBuffering },
};

const streamProxyCommon = {
    changeOrigin: true,
    ws: false,
    proxyTimeout: 0,
    timeout: 0,
    on: {
        proxyRes(proxyRes, _req, res) {
            res.setHeader('X-Accel-Buffering', 'no');
            res.setHeader('Cache-Control', 'no-cache, no-store');
            if (proxyRes.headers['transfer-encoding'] !== 'chunked'
                && !proxyRes.headers['content-length']) {
                res.setHeader('Transfer-Encoding', 'chunked');
            }
        },
    },
};

const airbandApiProxy = createProxyMiddleware({
    ...apiProxyCommon,
    target: CONFIG.airbandBase,
    pathFilter: '/api/airband',
    pathRewrite: { '^/api/airband': '/api' },
});

const tetrapolApiProxy = createProxyMiddleware({
    ...apiProxyCommon,
    target: CONFIG.tetrapolBase,
    pathFilter: '/api/tetrapol',
    pathRewrite: { '^/api/tetrapol': '/api' },
});

const icecastProxy = createProxyMiddleware({
    ...streamProxyCommon,
    target: CONFIG.icecastBase,
    pathFilter: '/stream',
});

/** Icecast coupe le mount entre deux coms → 404/504. On retente avant d'échouer le navigateur. */
const STREAM_RETRY_MS = 400;
const STREAM_RETRY_BUDGET_MS = 12000;

function isRetryableIcecastStatus(code) {
    return code === 404 || code === 502 || code === 503 || code === 504;
}

function proxyIcecastWithRetry(req, res, next) {
    if (req.method !== 'GET') {
        icecastProxy(req, res, next);
        return;
    }
    const started = Date.now();
    let target;
    try {
        target = new URL(req.originalUrl || req.url, CONFIG.icecastBase);
    } catch (_) {
        res.status(502).end('Icecast URL invalide');
        return;
    }
    let upstream = null;
    let done = false;

    const abortUpstream = () => {
        if (upstream) {
            try { upstream.destroy(); } catch (_) {}
            upstream = null;
        }
    };
    req.on('close', () => {
        if (!res.writableEnded) abortUpstream();
        done = true;
    });

    const fail = () => {
        if (done || res.headersSent) return;
        done = true;
        res.writeHead(504, { 'Content-Type': 'text/plain; charset=utf-8', 'Cache-Control': 'no-store' });
        res.end('Icecast indisponible');
    };

    const attempt = () => {
        if (done || req.destroyed || res.writableEnded) return;
        abortUpstream();
        const headers = Object.assign({}, req.headers);
        delete headers.host;
        delete headers.connection;
        delete headers['content-length'];
        const ureq = http.request({
            protocol: target.protocol,
            hostname: target.hostname,
            port: target.port || 80,
            path: target.pathname + target.search,
            method: 'GET',
            headers,
        }, (ures) => {
            if (done) {
                ures.resume();
                return;
            }
            const elapsed = Date.now() - started;
            if (isRetryableIcecastStatus(ures.statusCode) && elapsed < STREAM_RETRY_BUDGET_MS) {
                ures.resume();
                setTimeout(attempt, STREAM_RETRY_MS);
                return;
            }
            done = true;
            const outHeaders = Object.assign({}, ures.headers);
            delete outHeaders.connection;
            outHeaders['x-accel-buffering'] = 'no';
            outHeaders['cache-control'] = 'no-cache, no-store';
            res.writeHead(ures.statusCode, outHeaders);
            ures.pipe(res);
        });
        upstream = ureq;
        ureq.on('error', () => {
            if (done) return;
            if (Date.now() - started < STREAM_RETRY_BUDGET_MS) {
                setTimeout(attempt, STREAM_RETRY_MS);
                return;
            }
            fail();
        });
        ureq.end();
    };
    attempt();
}

app.use(airbandApiProxy);
app.use(tetrapolApiProxy);
if (CONFIG.streamProxy) {
    app.use('/stream', proxyIcecastWithRetry);
}

app.get('/api/monitor/config', (_req, res) => {
    res.setHeader('Cache-Control', 'no-store');
    res.json({
        ok: true,
        icecast_public_base: CONFIG.icecastPublicBase || null,
        icecast_browser_direct: CONFIG.icecastBrowserDirect,
        stream_proxy: CONFIG.streamProxy,
        icecast_base: CONFIG.icecastBase,
    });
});

app.get('/health', (_req, res) => {
    res.json({
        ok: true,
        port: CONFIG.listenPort,
        airband: CONFIG.airbandBase,
        tetrapol: CONFIG.tetrapolBase,
        icecast: CONFIG.icecastBase,
        icecast_public_base: CONFIG.icecastPublicBase || null,
        icecast_browser_direct: CONFIG.icecastBrowserDirect,
        stream_proxy: CONFIG.streamProxy,
        pulse_arbiter: !!(bridge && bridge.arbiter),
    });
});

const server = http.createServer(app);
// Plus long que le keep-alive Apache / Icecast (sinon 504 sur les flux MP3).
server.keepAliveTimeout = 65000;
server.headersTimeout = 70000;
server.timeout = 0;

const bridge = new WsBridge({
    airbandWsUrl: CONFIG.airbandWsUrl,
    tetrapolWsUrl: CONFIG.tetrapolWsUrl,
    airbandBase: CONFIG.airbandBase,
    tetrapolBase: CONFIG.tetrapolBase,
    listenPort: CONFIG.listenPort,
    icecastPublicBase: CONFIG.icecastPublicBase,
    pulseArbiter: process.env.PULSE_ARBITER !== '0',
});
bridge.attach(server);

server.on('upgrade', (request, socket, head) => {
    const url = request.url || '';
    if (url.startsWith('/ws/monitor')) {
        bridge.handleUpgrade(request, socket, head);
        return;
    }
    socket.destroy();
});

server.listen(CONFIG.listenPort, CONFIG.listenHost, () => {
    console.log(`Monitor listening on http://${CONFIG.listenHost}:${CONFIG.listenPort}`);
    console.log(`  Airband:  ${CONFIG.airbandBase}`);
    console.log(`  Tetrapol: ${CONFIG.tetrapolBase}`);
    console.log(`  Icecast:  ${CONFIG.icecastBase}`);
    console.log(`  Icecast public: ${CONFIG.icecastPublicBase || '(proxy /stream)'}`);
    console.log(`  Stream proxy: ${CONFIG.streamProxy ? 'on' : 'off'}`);
    console.log(`  Pulse arbiter: ${process.env.PULSE_ARBITER !== '0' ? 'on' : 'off'}`);
});

let shuttingDown = false;
function shutdown() {
    if (shuttingDown) {
        process.exit(1);
    }
    shuttingDown = true;
    console.log('Shutting down…');
    bridge.close();
    server.close(() => process.exit(0));
    setTimeout(() => process.exit(0), 1500).unref();
}

process.on('SIGINT', shutdown);
process.on('SIGTERM', shutdown);
