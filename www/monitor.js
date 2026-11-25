/**
 * Unified Analogique + Tetrapol monitor
 * - Two listings (enable/disable independently)
 * - One "Activer l'audio"
 * - Shared active cards + shared exclusive queue
 */

(function () {
    'use strict';

    const isAdminPage = document.body && document.body.dataset.adminPage === 'true';
    let isAdminMode = isAdminPage || (typeof location !== 'undefined' && location.hash.includes('admin'));
    // Activer tôt pour que queue-ring-buffer.js voie le flag dès l'ingestion.
    try {
        const h = String(location.hash || '').toLowerCase();
        const q = String(location.search || '').toLowerCase();
        window.__MONITOR_DEBUG__ = h.split(/[#&?/]/).some((p) => p.trim() === 'debug')
            || /(?:^|[?&])debug(?:=|$|&)/.test(q);
    } catch (_) {
        window.__MONITOR_DEBUG__ = false;
    }
    const WS_URL = (location.protocol === 'https:' ? 'wss:' : 'ws:') + '//' + location.host + '/ws/monitor';
    const OFF_AIR_GRACE_MS = 5000; // drop empty exclusive queue items after off-air grace
    // Fin de com : encore blankMarginSec (0,7 s) sous l'ancien StreamTitle, puis hold 7 s.
    const STREAM_END_HOLD_MS = 7000;
    /** Ignorer les parasites : ne pas ouvrir une carte avant 100 ms de com continue. */
    const MIN_ON_AIR_START_MS = 100;
    /** Tetrapol : après on_air↑, délai max sans StreamTitle avant de considérer la com fantôme. */
    const ON_AIR_TITLE_WAIT_MS = 5000;
    const SYNC_DEBOUNCE_MS = 150;
    const RING_FORCE_PROMOTE_RATIO = 0.92;
    const RING_YIELD_MAX_RATIO = 0.80;
    const RING_UI_INTERVAL_MS = 250;
    const CHANNEL_INACTIVE_HIDE_MS = 48 * 60 * 60 * 1000; // connected TCH
    const CHANNEL_NOT_CONNECTED_HIDE_MS = 24 * 60 * 60 * 1000; // TCH not managed by CCH
    const LONG_PRESS_MS = 450;
    /** Thumb jitter on phones — too low cancels long-press priority. */
    const MOVE_TOLERANCE = 36;
    const GESTURE_GUARD_MS = 700;
    /** Soft reconnect — avoids hammering Icecast (ERR_EMPTY_RESPONSE spam). */
    const ICECAST_PLAYER_OPTS = {
        metadataTypes: ['icy'],
        enableLogging: false,
        // Default is 1s — keep as low as practical so audio starts ASAP.
        // 0 can underrun on reconnect; ~0.1s is usually the workable floor for MP3/MSE.
        bufferLength: 0.1,
        retryTimeout: 20,
        retryDelayMin: 1.0,
        retryDelayMax: 5.0,
        retryDelayRate: 0.25
    };

    let icecastPublicBase = '';
    let tetrapolWsUrl = '';
    let tetrapolBridged = false;
    let airbandBridged = false;
    let audioEnabled = false;
    let exclusivePlaybackMode = mseMpegSupported()
        && localStorage.getItem('exclusivePlaybackMode') === 'true';
    let forceBeforeFull = localStorage.getItem('exclusiveForceBeforeBufferFull') !== 'false';
    let disabledAirband = new Set(JSON.parse(localStorage.getItem('disabledAirbandStreams') || '[]').map(String));
    let disabledTetrapol = new Set(JSON.parse(localStorage.getItem('disabledTetrapolGroups') || '[]').map(String));
    /** Projet entier désactivé : 'airband' | 'tetrapol' (écoute Icecast UI uniquement). */
    let disabledSources = new Set(JSON.parse(localStorage.getItem('disabledMonitorSources') || '[]').map(String));
    /** Devices Airband désactivés (index device en string). */
    let disabledDevices = new Set(JSON.parse(localStorage.getItem('disabledAirbandDevices') || '[]').map(String));
    /** Réseaux Tetrapol désactivés (2ᵉ segment RFSI), comme tetrapol-kit. */
    let disabledNetworks = new Set(JSON.parse(localStorage.getItem('disabledNetworks') || '[]').map(String));
    /** network_id → label (depuis WS NETWORKS). */
    let networkLabels = {};
    /** Entités scope déjà vues — ne pas retirer tant que le projet n'est pas désactivé / absent confirmé. */
    let knownScopeProjects = new Set(JSON.parse(localStorage.getItem('knownMonitorProjects') || '[]').map(String));
    let knownScopeDevices = new Map(Object.entries(
        JSON.parse(localStorage.getItem('knownAirbandDevices') || '{}')
    ).map(([k, v]) => [String(k), Number(v) || 0]));
    let knownScopeNetworks = new Map(Object.entries(
        JSON.parse(localStorage.getItem('knownTetrapolNetworks') || '{}')
    ).map(([k, v]) => [String(k), String(v || `Réseau ${k}`)]));
    let lastScopeSig = '';
    /** Groupes Tetrapol vus en clair au moins une fois (gid → label), persisté localement. */
    let knownClearTetrapol = new Map(Object.entries(
        JSON.parse(localStorage.getItem('knownClearTetrapolGroups') || '{}')
    ).map(([k, v]) => [String(k), String(v || k)]));
    // Stream exclusive-queue priority (Icecast cards)
    let priorityKeys = new Set(JSON.parse(localStorage.getItem('monitorPriorityKeys') || '[]'));
    // Pulse exclusive priority (admin green buttons)
    let pulsePriorityKeys = new Set(JSON.parse(localStorage.getItem('monitorPulsePriorityKeys') || '[]'));
    // Ignore Tetrapol priority echoes briefly after a local TP priority edit.
    let tpPulsePrioLocalUntil = 0;
    // Ignore disabled_networks echoes briefly after a local network toggle.
    let disabledNetworksLocalUntil = 0;
    let channelInactiveSince = {}; // channel_id -> ms when went inactive locally
    let channelWasOnAir = {};
    let inactiveRefreshTimer = null;

    let airbandById = new Map();
    let tetrapolGroups = new Map(); // group_id -> group info
    let tetrapolChannels = [];
    let tetrapolCells = new Map(); // channel_id -> cell sync state (from cell_info / cch_sync)
    /** Cellules KO si aucun cell_info frais (aligné tetrapol-kit 25s ; > fullSyncIntervalMs bridge). */
    const CELL_INFO_TIMEOUT_MS = 25000;
    let lastTetrapolCellFeedMs = 0;
    /** @type {Map<string, object>} unified playable entries by key */
    let catalog = new Map();

    let players = new Map(); // key -> player entry
    let exclusiveQueue = [];
    let exclusiveActive = null;
    let offAirSince = new Map();
    let lastStreamTitles = {};
    let lastNonEmptyStreamTitles = {};
    let pendingCardStarts = new Map(); // key -> timeout id (anti-parasite 0.1s)
    let entryWasOnAir = new Map(); // key -> last seen on_air (for blank fallback)
    /** Timers attente StreamTitle après on_air↑ (Tetrapol). */
    let onAirTitleWaitTimers = new Map();
    /** Incrémenté à chaque front montant on_air — ne pas rouvrir une carte fermée sur blanc tant que on_air n'a pas retombé. */
    let onAirGeneration = new Map();
    let blankClosedOnAirGen = new Map();
    let blankTimers = {};
    let blankSince = {};
    let blankDeadlines = {};
    let inactivityStatusInterval = null;
    let ringUiTimer = null;
    let syncPlayersTimer = null;
    let streamErrorLogAt = {};
    let pulseExclusiveState = {};
    let tpPulseExclusiveState = {};
    let pulseArbiterState = null;
    let pulsePanelDelegated = false;
    let pulseControlInFlight = false;
    let pulseLocalEditUntil = 0;
    let lastPulseActionAt = 0;
    let lastPulseActionKey = '';
    let departmentsMap = {};
    let countriesMap = {};
    let globalVolume = parseInt(localStorage.getItem('globalVolume') || '50', 10);
    let playbackRateFast = localStorage.getItem('playbackRateFast') === 'true';
    const PLAYBACK_RATE_FAST = 1.25;
    const PLAYBACK_RATE_NORMAL = 1;

    let lastAirSig = '';
    let lastAirBtnSig = '';
    let lastTpSig = '';
    let lastPulseSig = '';
    let lastPulseFillSig = '';
    let lastReceptionSig = '';
    let tetrapolConnecting = false;
    let tetrapolWs = null; // unused — Tetrapol is bridged server-side
    let ws = null;
    let wsRetries = 0;

    const el = {
        channelsTableBody: document.getElementById('channelsTableBody'),
        groupButtons: document.getElementById('groupButtonsContainer'),
        groupButtonsSection: document.getElementById('groupButtonsSection'),
        streamsSection: document.getElementById('streamsSection'),
        streamsContainer: document.getElementById('streamsContainer'),
        connectionStatus: document.getElementById('connectionStatus'),
        channelsStatus: document.getElementById('channelsStatusIndicator'),
        activateAudio: document.getElementById('activateAudio'),
        exclusiveToggle: document.getElementById('exclusivePlaybackToggle'),
        exclusivePlaybackControl: document.getElementById('exclusivePlaybackControl'),
        exclusiveForceToggle: document.getElementById('exclusiveForceBufferToggle'),
        exclusiveForceControl: document.getElementById('exclusiveForceBufferControl'),
        volumeSlider: document.getElementById('globalVolumeSlider'),
        volumeValue: document.getElementById('globalVolumeValue'),
        playbackRateFastToggle: document.getElementById('playbackRateFastToggle'),
        tetrapolSection: document.getElementById('tetrapolSection'),
        tetrapolBody: document.getElementById('tetrapolChannelsBody'),
        tetrapolStatus: document.getElementById('tetrapolStatusIndicator'),
        tetrapolGroups: document.getElementById('tetrapolGroupButtons'),
        streamsStatus: document.getElementById('streamsStatusIndicator'),
        receptionContainer: document.getElementById('receptionContainer'),
        scopeSection: document.getElementById('scopeButtonsSection'),
        projectButtons: document.getElementById('projectButtonsContainer'),
        deviceButtons: document.getElementById('deviceButtonsContainer'),
        networkButtons: document.getElementById('networkButtonsContainer'),
        deviceButtonsTitle: document.getElementById('deviceButtonsTitle'),
        networkButtonsTitle: document.getElementById('networkButtonsTitle'),
    };

    function setDot(node, ok) {
        if (!node) return;
        node.style.cssText = 'display:inline-block;width:10px;height:10px;border-radius:50%;margin-right:0.4em;background:' + (ok ? '#3c3' : '#c33');
    }

    function esc(id) {
        return (window.CSS && CSS.escape) ? CSS.escape(id) : String(id).replace(/"/g, '\\"');
    }

    function escHtml(s) {
        return String(s == null ? '' : s)
            .replace(/&/g, '&amp;')
            .replace(/</g, '&lt;')
            .replace(/>/g, '&gt;')
            .replace(/"/g, '&quot;');
    }

    function airKey(streamId) { return 'ab:' + streamId; }
    function tpKey(groupId) { return 'tp:' + groupId; }

    function getAirbandChannel(id) {
        if (id == null || id === '') return undefined;
        if (airbandById.has(id)) return airbandById.get(id);
        const s = String(id);
        if (airbandById.has(s)) return airbandById.get(s);
        const n = Number(id);
        if (Number.isFinite(n) && airbandById.has(n)) return airbandById.get(n);
        return undefined;
    }

    /**
     * On-air Pulse = listing / boutons streaming (canaux), pas la télémétrie mixer
     * qui arrive en retard ou reste collée.
     */
    function pulseGroupOnAir(source, gid, entry, pe) {
        const id = gid != null && gid !== '' ? gid : (entry && entry.rawId);
        const src = source || (entry && entry.source);
        if (src === 'tetrapol' && id != null && id !== '') {
            return tetrapolGroupIsOnAir(id);
        }
        if (src === 'airband' && id != null && id !== '') {
            const ch = getAirbandChannel(id);
            if (ch) return !!ch.on_air;
            const cat = catalog.get(airKey(id)) || catalog.get(airKey(String(id)));
            if (cat) return !!cat.on_air;
        }
        if (entry && entry.on_air != null) return !!entry.on_air;
        return !!(pe && pe.on_air);
    }

    function pulseGroupIsPlaying(source, gid, displayActiveId, displayActiveSource) {
        if (!displayActiveId) return false;
        return displayActiveSource === source && String(gid) === String(displayActiveId);
    }

    function saveDisabled() {
        localStorage.setItem('disabledAirbandStreams', JSON.stringify([...disabledAirband]));
        localStorage.setItem('disabledTetrapolGroups', JSON.stringify([...disabledTetrapol]));
        localStorage.setItem('disabledMonitorSources', JSON.stringify([...disabledSources]));
        localStorage.setItem('disabledAirbandDevices', JSON.stringify([...disabledDevices]));
        localStorage.setItem('disabledNetworks', JSON.stringify([...disabledNetworks]));
        localStorage.setItem('knownMonitorProjects', JSON.stringify([...knownScopeProjects]));
        const devObj = {};
        knownScopeDevices.forEach((total, id) => { devObj[id] = total; });
        localStorage.setItem('knownAirbandDevices', JSON.stringify(devObj));
        const netObj = {};
        knownScopeNetworks.forEach((label, id) => { netObj[id] = label; });
        localStorage.setItem('knownTetrapolNetworks', JSON.stringify(netObj));
        localStorage.setItem('monitorPriorityKeys', JSON.stringify([...priorityKeys]));
        localStorage.setItem('monitorPulsePriorityKeys', JSON.stringify([...pulsePriorityKeys]));
    }

    function saveKnownClearTetrapol() {
        const obj = {};
        knownClearTetrapol.forEach((label, gid) => { obj[gid] = label; });
        localStorage.setItem('knownClearTetrapolGroups', JSON.stringify(obj));
    }

    /** Mémoriser un groupe Tetrapol après une com en clair (pas chiffrée seule). */
    function noteTetrapolClearGroup(channel) {
        if (!channel || channel.encrypted) return;
        const gid = String(channel.group_id || '').trim();
        if (!gid) return;
        const label = String(channel.group_label || knownClearTetrapol.get(gid) || gid).trim() || gid;
        const prev = knownClearTetrapol.get(gid);
        if (prev === label && knownClearTetrapol.has(gid)) return;
        knownClearTetrapol.set(gid, label);
        saveKnownClearTetrapol();
        lastTpSig = '';
    }

    function scanTetrapolClearGroups() {
        tetrapolChannels.forEach((c) => {
            if (c.encrypted) return;
            if (c.on_air || shouldDisplayChannelInTable(c)) {
                noteTetrapolClearGroup(c);
            }
        });
    }

    /**
     * on_air d'un groupe = canaux live (comme le tableau et les réseaux).
     * Le flag groupe côté bridge arrive souvent en retard ou reste collé.
     */
    function tetrapolGroupIsOnAir(gid) {
        const id = String(gid);
        let sawChannel = false;
        for (let i = 0; i < tetrapolChannels.length; i++) {
            const c = tetrapolChannels[i];
            if (String(c.group_id) !== id) continue;
            sawChannel = true;
            if (c.on_air) return true;
        }
        if (sawChannel) return false;
        return !!(tetrapolGroups.get(id)?.on_air);
    }

    function syncTetrapolGroupsOnAirFromChannels() {
        const onAirIds = new Set();
        const seenIds = new Set();
        tetrapolChannels.forEach((c) => {
            if (c.group_id == null) return;
            const gid = String(c.group_id);
            seenIds.add(gid);
            if (c.on_air) onAirIds.add(gid);
        });
        let changed = false;
        seenIds.forEach((gid) => {
            const g = tetrapolGroups.get(gid);
            if (!g) return;
            const next = onAirIds.has(gid);
            if (!!g.on_air !== next) {
                g.on_air = next;
                changed = true;
            }
        });
        return changed;
    }

    /** Boutons Tetrapol : uniquement groupes déjà vus en clair (liste locale). */
    function getTetrapolButtonEntries() {
        const entries = [];
        knownClearTetrapol.forEach((savedLabel, gid) => {
            const key = tpKey(gid);
            const fromCatalog = catalog.get(key);
            const onAir = tetrapolGroupIsOnAir(gid);
            if (fromCatalog && !fromCatalog.encrypted) {
                entries.push(Object.assign({}, fromCatalog, { on_air: onAir }));
                return;
            }
            entries.push({
                key,
                source: 'tetrapol',
                rawId: gid,
                label: fromCatalog?.label || savedLabel || gid,
                on_air: onAir,
                encrypted: false,
                has_stream: true,
                streamUrl: buildTetrapolUrl(gid),
                priority: isStreamPriority(key),
                meta: { listen: true, encrypted: false, group_label: savedLabel || gid }
            });
        });
        return entries;
    }

    function logStreamError(key, err) {
        const now = Date.now();
        if ((streamErrorLogAt[key] || 0) + 5000 > now) return;
        streamErrorLogAt[key] = now;
        console.warn('stream error', key, err);
    }

    function isDebugMode() {
        try {
            const hash = String(location.hash || '').toLowerCase();
            if (hash.split(/[#&?/]/).some((p) => p.trim() === 'debug')) return true;
            const q = String(location.search || '').toLowerCase();
            if (/(?:^|[?&])debug(?:=|$|&)/.test(q)) return true;
        } catch (_) { /* ignore */ }
        return !!window.__MONITOR_DEBUG__;
    }

    function refreshDebugMode() {
        const on = isDebugMode();
        window.__MONITOR_DEBUG__ = on;
        let banner = document.getElementById('monitorDebugBanner');
        if (on) {
            if (!banner && document.body) {
                banner = document.createElement('div');
                banner.id = 'monitorDebugBanner';
                banner.className = 'debug-mode-banner';
                banner.textContent = 'DEBUG StreamTitle — logs console actifs ([debug:StreamTitle] / [debug:ring] / [debug:play])';
                document.body.insertBefore(banner, document.body.firstChild);
            }
            if (banner) banner.style.display = 'block';
            console.info('%c[debug] StreamTitle logging ON', 'background:#00838f;color:#fff;padding:2px 8px;border-radius:3px');
        } else if (banner) {
            banner.style.display = 'none';
        }
        return on;
    }

    /** Logs StreamTitle / blank quand #debug est dans l'URL. */
    function debugStreamTitleLog(event, details) {
        if (!window.__MONITOR_DEBUG__ && !isDebugMode()) return;
        window.__MONITOR_DEBUG__ = true;
        const payload = Object.assign({ t: new Date().toISOString().slice(11, 23) }, details || {});
        console.log(
            '%c[debug:StreamTitle]%c ' + event,
            'background:#00838f;color:#fff;padding:1px 6px;border-radius:3px',
            'color:inherit',
            payload
        );
    }

    function createStreamPlayer(url, key) {
        // IcecastMetadataPlayer utilise MSE audio/mpeg — non supporté sous Firefox.
        if (!mseMpegSupported()) return null;
        if (typeof IcecastMetadataPlayer === 'undefined') return null;
        return new IcecastMetadataPlayer(url, {
            ...ICECAST_PLAYER_OPTS,
            onMetadata: (meta) => handleStreamMetadata(key, meta),
            onError: (msg, err) => logStreamError(key, err || msg),
            onRetryTimeout: () => logStreamError(key, 'retry timeout')
        });
    }

    function extractNetworkId(rfsi) {
        if (rfsi == null) return null;
        const parts = String(rfsi).split('-');
        return parts.length >= 2 && parts[1] ? String(parts[1]) : null;
    }

    /**
     * StreamTitle ICY : "Label|130-2-20-123" (nouveau) ou label/RFSI seul (ancien).
     * Compatible avec et sans partie RFSI.
     */
    function parseIcyStreamTitle(raw) {
        const text = String(raw ?? '').trim();
        if (!text) return { label: '', rfsi: '', display: '' };

        const pipe = text.lastIndexOf('|');
        if (pipe >= 0) {
            const maybeRfsi = text.slice(pipe + 1).trim();
            const label = text.slice(0, pipe).trim();
            if (maybeRfsi && extractNetworkId(maybeRfsi)) {
                return {
                    label: label || maybeRfsi,
                    rfsi: maybeRfsi,
                    display: label || maybeRfsi,
                };
            }
        }

        const nid = extractNetworkId(text);
        if (nid) return { label: text, rfsi: text, display: text };
        return { label: text, rfsi: '', display: text };
    }

    /** Dernier réseau stream vu par groupe (RFSI canal ou ICY Label|RFSI). */
    const groupStreamNetworkIds = Object.create(null);

    function rememberGroupStreamNetwork(groupId, text) {
        const gid = String(groupId || '');
        const parsed = parseIcyStreamTitle(text);
        const nid = extractNetworkId(parsed.rfsi || text);
        if (!gid || !nid) return;
        groupStreamNetworkIds[gid] = String(nid);
    }

    /** Réseau Tetrapol d'un groupe (via RFSI des canaux on_air / connus / ICY). */
    function networkIdForGroup(groupId) {
        const gid = String(groupId);
        let fallback = null;
        for (const c of tetrapolChannels) {
            if (String(c.group_id) !== gid || !c.rfsi) continue;
            const nid = extractNetworkId(c.rfsi);
            if (!nid) continue;
            rememberGroupStreamNetwork(gid, c.rfsi);
            if (c.on_air) return nid;
            if (!fallback) fallback = nid;
        }
        if (fallback) return fallback;
        return groupStreamNetworkIds[gid] || null;
    }

    function deviceIdForEntry(entry) {
        if (!entry || entry.source !== 'airband') return null;
        const d = entry.meta && entry.meta.device;
        if (d == null) return null;
        return String(d);
    }

    function isSourceEnabled(source) {
        return !disabledSources.has(source);
    }

    function isDeviceEnabled(deviceId) {
        if (deviceId == null || deviceId === '') return true;
        return !disabledDevices.has(String(deviceId));
    }

    function isNetworkEnabled(networkId) {
        if (networkId == null || networkId === '') return true;
        return !disabledNetworks.has(String(networkId));
    }

    function isEnabled(entry) {
        if (!entry) return false;
        if (!isSourceEnabled(entry.source)) return false;
        if (entry.source === 'airband') {
            const dev = deviceIdForEntry(entry);
            if (dev != null && !isDeviceEnabled(dev)) return false;
            return !disabledAirband.has(String(entry.rawId));
        }
        const nid = networkIdForGroup(entry.rawId);
        if (nid != null && !isNetworkEnabled(nid)) return false;
        return !disabledTetrapol.has(String(entry.rawId));
    }

    function applyNetworksPayload(networksMsg) {
        if (!networksMsg) return false;
        const list = Array.isArray(networksMsg.networks)
            ? networksMsg.networks
            : (Array.isArray(networksMsg) ? networksMsg : null);
        if (!list) return false;
        const next = {};
        list.forEach((n) => {
            if (!n) return;
            const id = n.network_id != null ? String(n.network_id) : null;
            if (!id) return;
            next[id] = String(n.network_label || n.label || `Réseau ${id}`);
            knownScopeNetworks.set(id, next[id]);
        });
        const prev = JSON.stringify(networkLabels);
        // Merge: ne pas perdre les labels connus si le payload est partiel.
        networkLabels = { ...networkLabels, ...next };
        return prev !== JSON.stringify(networkLabels);
    }

    /** Remember devices / networks / projects seen — lists stay until user disables the project. */
    function rememberScopeFromLiveData() {
        if (airbandBridged || airbandById.size) knownScopeProjects.add('airband');
        if (tetrapolBridged || tetrapolWsUrl || tetrapolChannels.length || tetrapolGroups.size) {
            knownScopeProjects.add('tetrapol');
        }
        airbandById.forEach((ch) => {
            if (ch.device == null) return;
            const id = String(ch.device);
            const prev = knownScopeDevices.get(id) || 0;
            // Compter les fréquences courantes si disponibles, sinon garder le max connu.
            let n = 0;
            airbandById.forEach((c) => { if (String(c.device) === id) n += 1; });
            knownScopeDevices.set(id, Math.max(prev, n));
        });
        Object.keys(networkLabels).forEach((id) => {
            knownScopeNetworks.set(id, networkLabels[id] || knownScopeNetworks.get(id) || `Réseau ${id}`);
        });
        tetrapolChannels.forEach((c) => {
            const nid = extractNetworkId(c.rfsi);
            if (!nid) return;
            if (!knownScopeNetworks.has(nid)) {
                knownScopeNetworks.set(nid, networkLabels[nid] || `Réseau ${nid}`);
            }
        });
    }

    function stopStreamsMatching(pred) {
        catalog.forEach((entry) => {
            if (pred(entry)) stopStreamMonitoring(entry);
        });
    }

    function refreshAfterScopeChange() {
        lastAirBtnSig = '';
        lastTpSig = '';
        lastReceptionSig = '';
        lastScopeSig = '';
        saveDisabled();
        updateScopeButtons();
        updateAirbandButtons();
        updateTetrapolListing();
        updateReceptionDisplay();
        if (audioEnabled) syncPlayersFromCatalog();
    }

    /** Entrée éligible à une carte stream (activée, courante en scan, non chiffrée). */
    function shouldMonitorEntry(entry) {
        if (!entry || !entry.has_stream || !entry.streamUrl) return false;
        if (isEncryptedEntry(entry)) return false;
        if (!isEnabled(entry)) return false;
        // Scan : une seule fréquence courante par canal — ignorer les autres.
        if (entry.source === 'airband' && entry.meta && entry.meta.is_current === false) return false;
        return true;
    }

    /** Clé catalogue qui doit lire ce streamUrl (fréquence courante + on_air + activée). */
    function streamUrlOwnerKey(url) {
        if (!url) return null;
        if (mountBlockedByDisabledCurrent(url)) return null;

        let owner = null;
        let currentOwner = null;
        catalog.forEach((entry) => {
            if (entry.streamUrl !== url || !shouldMonitorEntry(entry) || !entry.on_air) return;
            owner = entry.key;
            if (entry.source === 'airband' && !(entry.meta && entry.meta.is_current === false)) {
                currentOwner = entry.key;
            }
        });
        return currentOwner || owner;
    }

    /** Mount airband dont la fréquence courante est on_air mais désactivée par l'utilisateur. */
    function mountBlockedByDisabledCurrent(url) {
        if (!url) return false;
        let blocked = false;
        catalog.forEach((entry) => {
            if (entry.streamUrl !== url || !entry.on_air) return;
            if (entry.source !== 'airband') return;
            if (entry.meta && entry.meta.is_current === false) return;
            if (!isEnabled(entry)) blocked = true;
        });
        return blocked;
    }

    function forEachCatalogKeyWithStreamUrl(url, fn) {
        if (!url) return;
        catalog.forEach((entry) => {
            if (entry.streamUrl === url) fn(entry.key, entry);
        });
    }

    function clearPendingForStreamUrl(url) {
        forEachCatalogKeyWithStreamUrl(url, (key) => clearPendingCardStart(key));
    }

    /** Arrête la carte / file de cette entrée uniquement (pas tout le mount). */
    function stopStreamMonitoring(entry) {
        if (!entry) return;
        clearPendingCardStart(entry.key);
        if (players.has(entry.key) || isExclusiveSegmentKey(entry.key)) {
            stopEntry(entry.key);
        }
        pruneStreamCardsByUrlOwner();
    }

    function getExclusiveSegment(key) {
        if (!key) return null;
        if (exclusiveActive?.key === key) return exclusiveActive;
        return exclusiveQueue.find((s) => s.key === key) || null;
    }

    function pruneStreamCardsByUrlOwner() {
        const keys = new Set([...players.keys()]);
        exclusiveQueue.forEach((s) => keys.add(s.key));
        if (exclusiveActive) keys.add(exclusiveActive.key);
        keys.forEach((key) => {
            const entry = catalog.get(key);
            if (!entry?.streamUrl) return;
            if (!isEnabled(entry)) {
                stopEntry(key);
                return;
            }
            const ownerKey = streamUrlOwnerKey(entry.streamUrl);
            // Autre entrée légitime sur le même mount.
            if (ownerKey && ownerKey !== key) {
                stopEntry(key);
                return;
            }
            // Courant désactivé : ne pas laisser une autre card lire ce mount.
            if (!ownerKey && mountBlockedByDisabledCurrent(entry.streamUrl)) {
                stopEntry(key);
                return;
            }
            // ownerKey null (fin de com) : laisser finir le tampon.
        });
    }

    function deriveTetrapolBases(wsUrl) {
        // Kept for config display; browser no longer opens a remote WS/API.
        void wsUrl;
    }

    /** Prefer same-origin /stream (proxy) unless browser-direct Icecast is enabled. */
    function resolveIcecastUrl(mountPath) {
        const mount = String(mountPath || '').replace(/^\//, '');
        if (!mount) return null;
        const base = String(icecastPublicBase || '').replace(/\/$/, '');
        const port = String(location.port || (location.protocol === 'https:' ? '443' : '80'));
        const viaReverseProxy = port === '80' || port === '443' || port === '';
        // Direct Icecast needs CORS (Access-Control-Allow-Origin). Without it the
        // metadata player fails with net::ERR_EMPTY_RESPONSE / CORS errors.
        // Enable via ICECAST_BROWSER_DIRECT=1 or ?directStreams=1
        const wantDirect = !!(window.__ICECAST_BROWSER_DIRECT__
            || /(?:^|[?&#])directStreams=1(?:&|$|#)/.test(location.search + location.hash));
        if (wantDirect && base && !viaReverseProxy) {
            return base + '/' + mount;
        }
        return '/' + mount;
    }

    function buildAirbandUrl(ch) {
        if (!ch || !ch.has_icecast || !ch.icecast_mountpoint) return null;
        return resolveIcecastUrl(ch.icecast_mountpoint);
    }

    function buildTetrapolUrl(groupId) {
        if (groupId == null || groupId === '') return null;
        return resolveIcecastUrl('stream/' + groupId + '.mp3');
    }

    function catalogEntryFor(keyOrSeg) {
        const key = typeof keyOrSeg === 'string'
            ? keyOrSeg
            : (keyOrSeg?.key || keyOrSeg?.entry?.key || '');
        if (!key) return keyOrSeg?.entry || keyOrSeg || null;
        return catalog.get(key) || keyOrSeg?.entry || null;
    }

    function labelOf(entry) {
        const e = catalogEntryFor(entry) || entry;
        if (!e) return '';
        const direct = String(e.label || '').trim();
        if (direct) return direct;
        if (e.source === 'tetrapol') {
            const gid = String(e.rawId || '').trim();
            if (gid) {
                const g = tetrapolGroups.get(gid);
                const gl = String(g?.group_label || '').trim();
                if (gl) return gl;
                const ch = tetrapolChannels.find((c) => String(c.group_id || '') === gid);
                const chl = String(ch?.group_label || '').trim();
                if (chl) return chl;
                return gid;
            }
        }
        if (e.source === 'airband') {
            const hz = e.meta?.freq_hz;
            if (hz) return (hz / 1e6).toFixed(3) + ' MHz';
        }
        return String(e.rawId || '').trim();
    }

    function sourceLabel(entry) {
        return entry && entry.source === 'tetrapol' ? '📻 Tetrapol' : '✈️ Analogique';
    }

    function freqLabel(entry) {
        const hz = entry && entry.meta && entry.meta.freq_hz;
        if (!hz) return '';
        return (hz / 1e6).toFixed(3) + ' MHz';
    }

    function queueRoleLabel(seg, isActive) {
        if (isActive) return '';
        if (seg && seg.suspendedByUser) return 'pause';
        return 'file';
    }

    function cardHeaderHtml(entry, txTitle, frozenGroupLabel) {
        const src = sourceLabel(entry);
        if (entry && entry.source === 'airband') {
            const title = labelOf(entry);
            const freq = freqLabel(entry);
            return `<div class="stream-card-header stream-card-zone stream-card-zone-name">
                <div class="stream-card-source">${src}</div>
                <div class="stream-card-title">${title}</div>
                ${freq ? `<div class="stream-card-subtitle">${freq}</div>` : ''}
            </div>`;
        }
        const group = tetrapolCardGroupLabel(entry, frozenGroupLabel);
        const parsed = parseIcyStreamTitle(txTitle);
        const streamTitle = parsed.display || '';
        const rfsiLine = (parsed.rfsi && parsed.rfsi !== parsed.label)
            ? `<div class="stream-card-rfsi">${parsed.rfsi}</div>`
            : '';
        return `<div class="stream-card-header stream-card-zone stream-card-zone-name stream-card-header-tetrapol">
            <div class="stream-card-source">${src}</div>
            <div class="stream-card-title">${streamTitle || '\u00a0'}</div>
            ${rfsiLine}
            <div class="stream-card-subtitle stream-card-group-label">${group}</div>
        </div>`;
    }

    function updateCardHeader(card, entry, opts = {}) {
        entry = catalogEntryFor(entry) || entry;
        if (!card || !entry) return;
        let header = card.querySelector('.stream-card-header');
        if (!header) {
            const wrap = document.createElement('div');
            wrap.innerHTML = cardHeaderHtml(entry, opts.txTitle || '', opts.groupLabel || '');
            header = wrap.firstElementChild;
            const oldName = card.querySelector('.stream-name, .stream-card-zone-name');
            if (oldName) oldName.replaceWith(header);
            else card.insertBefore(header, card.firstChild);
        }
        const srcEl = header.querySelector('.stream-card-source');
        const titleEl = header.querySelector('.stream-card-title');
        const subEl = header.querySelector('.stream-card-subtitle');
        const role = (opts.role || '').trim();
        if (srcEl) {
            srcEl.textContent = role ? `${sourceLabel(entry)} · ${role}` : sourceLabel(entry);
        }
        if (entry.source === 'airband') {
            if (titleEl) titleEl.textContent = labelOf(entry);
            const freq = freqLabel(entry);
            if (freq) {
                if (subEl) subEl.textContent = freq;
                else {
                    const s = document.createElement('div');
                    s.className = 'stream-card-subtitle';
                    s.textContent = freq;
                    header.appendChild(s);
                }
            } else if (subEl) subEl.remove();
        } else {
            header.classList.add('stream-card-header-tetrapol');
            const group = tetrapolCardGroupLabel(entry, opts.groupLabel);
            const parsed = parseIcyStreamTitle(opts.txTitle);
            const streamTitle = parsed.display || '';
            if (titleEl) titleEl.textContent = streamTitle || '\u00a0';

            let rfsiEl = header.querySelector('.stream-card-rfsi');
            const showRfsi = !!(parsed.rfsi && parsed.rfsi !== parsed.label);
            if (showRfsi) {
                if (!rfsiEl) {
                    rfsiEl = document.createElement('div');
                    rfsiEl.className = 'stream-card-rfsi';
                    const after = titleEl || header.querySelector('.stream-card-source');
                    if (after && after.nextSibling) header.insertBefore(rfsiEl, after.nextSibling);
                    else header.appendChild(rfsiEl);
                }
                rfsiEl.textContent = parsed.rfsi;
            } else if (rfsiEl) {
                rfsiEl.remove();
            }

            // Re-resolve subtitle after possible RFSI insert.
            const groupEl = header.querySelector('.stream-card-group-label')
                || header.querySelector('.stream-card-subtitle');
            if (groupEl) {
                groupEl.textContent = group;
                groupEl.classList.add('stream-card-subtitle', 'stream-card-group-label');
            } else {
                const s = document.createElement('div');
                s.className = 'stream-card-subtitle stream-card-group-label';
                s.textContent = group;
                header.appendChild(s);
            }

            if (opts.txTitle) {
                const gid = entry && entry.rawId != null ? entry.rawId : null;
                if (gid != null) rememberGroupStreamNetwork(gid, opts.txTitle);
            }
        }
    }

    function isTetrapolChannelEncrypted(channel) {
        if (!channel) return false;
        const v = channel.encrypted;
        if (v === true || v === 1 || v === '1') return true;
        if (typeof v === 'string') {
            const s = v.trim().toLowerCase();
            if (s === 'true' || s === 'crypted' || s === 'encrypted') return true;
        }
        return false;
    }

    function isEncryptedEntry(entry) {
        if (!entry) return false;
        if (entry.encrypted) return true;
        const m = entry.meta || {};
        if (m.encrypted) return true;
        if (entry.source === 'tetrapol') {
            return tetrapolChannels.some(c =>
                String(c.group_id) === String(entry.rawId) && isTetrapolChannelEncrypted(c));
        }
        return false;
    }

    async function apiPulseToggle(streamId, listen) {
        try {
            await fetch('/api/airband/toggle?group=' + encodeURIComponent(streamId) + '&listen=' + (listen ? 'true' : 'false'), { method: 'POST' });
        } catch (e) { console.warn(e); }
    }

    async function apiTetrapolToggle(groupId, listen) {
        // Proxied through Airband → remote Tetrapol mixer (no browser CORS)
        try {
            await fetch('/api/tetrapol/toggle?group=' + encodeURIComponent(groupId) + '&listen=' + (listen ? 'true' : 'false'), {
                method: 'POST'
            });
        } catch (e) {
            console.warn('tetrapol toggle', e);
        }
    }

    function applyLocalPulseListen(source, gid, listen) {
        const id = String(gid || '');
        if (source === 'tetrapol') {
            const g = tetrapolGroups.get(id);
            if (g) g.listen = !!listen;
            const entry = catalog.get(tpKey(id));
            if (entry) {
                if (!entry.meta) entry.meta = {};
                entry.meta.listen = !!listen;
            }
        } else {
            const ch = getAirbandChannel(id);
            if (ch) ch.listen = !!listen;
            const entry = catalog.get(airKey(id));
            if (entry) {
                if (!entry.meta) entry.meta = {};
                entry.meta.listen = !!listen;
            }
        }
    }

    async function apiPulseExclusive(payload) {
        try {
            const r = await fetch('/api/airband/pulse-exclusive', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(payload)
            });
            const text = await r.text();
            try {
                return JSON.parse(text);
            } catch (e) {
                console.warn('pulse-exclusive failed', e, text.slice(0, 200));
                return null;
            }
        } catch (e) {
            console.warn('pulse-exclusive failed', e);
            return null;
        }
    }

    async function apiTetrapolPulseExclusive(payload) {
        try {
            const r = await fetch('/api/tetrapol/pulse-exclusive', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(payload)
            });
            if (!r.ok) console.warn('tetrapol pulse-exclusive HTTP', r.status);
            const text = await r.text();
            try {
                return text ? JSON.parse(text) : null;
            } catch (_) {
                return null;
            }
        } catch (e) {
            console.warn('tetrapol pulse-exclusive failed', e);
            return null;
        }
    }

    async function apiReloadTetrapolLabels() {
        const r = await fetch('/api/tetrapol/update_groupe', { method: 'POST' });
        const text = await r.text();
        let data = null;
        try {
            data = text ? JSON.parse(text) : null;
        } catch (_) {
            data = null;
        }
        if (!r.ok) {
            const err = new Error('HTTP ' + r.status);
            err.status = r.status;
            throw err;
        }
        return data || {};
    }

    function refreshLabelsOnUi() {
        lastTpSig = '';
        lastPulseSig = '';
        lastAirBtnSig = '';
        lastReceptionSig = '';
        exclusiveQueue.forEach((s) => { s.displayName = ''; });
        if (exclusiveActive) exclusiveActive.displayName = '';
        rebuildCatalog();
        updateTetrapolListing();
        updateAirbandButtons();
        refreshExclusiveCards();
        if (isAdminMode) updatePulseAdmin(pulseExclusiveState);
    }

    function wireAdminReloadControls() {
        const btn = document.getElementById('reloadLabelsBtn');
        if (!btn || btn.dataset.bound === '1') return;
        btn.dataset.bound = '1';
        btn.addEventListener('click', async () => {
            const status = document.getElementById('reloadLabelsStatus');
            btn.disabled = true;
            if (status) {
                status.textContent = '⏳ Chargement…';
                status.style.color = '#4af';
            }
            try {
                const data = await apiReloadTetrapolLabels();
                refreshLabelsOnUi();
                if (status) {
                    const rfsi = data.rfsi_labels != null ? data.rfsi_labels : '?';
                    const groups = data.group_labels != null ? data.group_labels : '?';
                    const rfsiAdd = data.rfsi_added != null
                        ? ` (${data.rfsi_added >= 0 ? '+' : ''}${data.rfsi_added})`
                        : '';
                    const gAdd = data.group_added != null
                        ? ` (${data.group_added >= 0 ? '+' : ''}${data.group_added})`
                        : '';
                    status.textContent = `✅ Labels rechargés — RFSI: ${rfsi}${rfsiAdd}, Groupes: ${groups}${gAdd}`;
                    status.style.color = '#2c8';
                }
                setTimeout(() => { if (status) status.textContent = ''; }, 5000);
            } catch (err) {
                console.warn('reload labels', err);
                if (status) {
                    status.textContent = '❌ Erreur lors du rechargement';
                    status.style.color = '#f44';
                }
            } finally {
                btn.disabled = false;
            }
        });
    }

    async function apiPulseArbiter(path, payload) {
        try {
            const r = await fetch('/api/pulse/' + path, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(payload || {})
            });
            const text = await r.text();
            try {
                return text ? JSON.parse(text) : null;
            } catch (_) {
                return null;
            }
        } catch (e) {
            console.warn('pulse arbiter', path, e);
            return null;
        }
    }

    function resolvePulseSource(gid) {
        const id = String(gid || '');
        if (catalog.get(airKey(id))) return 'airband';
        if (catalog.get(tpKey(id))) return 'tetrapol';
        if ((pulseExclusiveState.listen_groups || []).some((g) => String(g.group_id) === id))
            return 'airband';
        if ((tpPulseExclusiveState.listen_groups || []).some((g) => String(g.group_id) === id))
            return 'tetrapol';
        return 'airband';
    }

    function cssEscapeAttr(value) {
        const s = String(value);
        if (typeof CSS !== 'undefined' && CSS.escape) return CSS.escape(s);
        return s.replace(/\\/g, '\\\\').replace(/"/g, '\\"');
    }

    /** Pulse exclusive / force must hit both controllers (shared audio behaviour). */
    async function apiPulseExclusiveBoth(payload) {
        const [ab] = await Promise.all([
            apiPulseExclusive(payload),
            apiTetrapolPulseExclusive(payload),
        ]);
        return ab;
    }

    function rebuildCatalog() {
        catalog.clear();
        airbandById.forEach((ch, id) => {
            const url = buildAirbandUrl(ch);
            catalog.set(airKey(id), {
                key: airKey(id),
                source: 'airband',
                rawId: id,
                label: ch.label || ((ch.freq_hz / 1e6).toFixed(3) + ' MHz'),
                on_air: !!ch.on_air,
                has_stream: !!url,
                streamUrl: url,
                priority: isStreamPriority(airKey(id)) || !!ch.priority,
                meta: ch
            });
        });
        tetrapolGroups.forEach((g, gid) => {
            const url = buildTetrapolUrl(gid);
            let encrypted = !!g.encrypted;
            tetrapolChannels.forEach(c => {
                if (String(c.group_id) === String(gid) && c.encrypted) encrypted = true;
            });
            catalog.set(tpKey(gid), {
                key: tpKey(gid),
                source: 'tetrapol',
                rawId: String(gid),
                label: g.group_label || String(gid),
                on_air: tetrapolGroupIsOnAir(gid),
                encrypted,
                has_stream: !!url && !encrypted,
                streamUrl: encrypted ? null : url,
                priority: isStreamPriority(tpKey(gid)),
                meta: Object.assign({}, g, { listen: g.listen !== false, encrypted })
            });
        });
    }

    /* ---- Réception : cellules Tetrapol + devices Analogique ---- */

    function normalizeCellSyncStatus(status, syncErrs) {
        const s = String(status || '').toLowerCase();
        if (s === 'synced' || s === 'ok' || s === 'sync') return 'synced';
        if (s === 'unsynced' || s === 'timeout' || s === 'unknown') return 'unsynced';
        if (s === 'lost' || s === 'error' || s === 'nosync') return 'lost';
        if (!s && syncErrs > 0) return 'lost';
        if (!s) return 'unsynced';
        return s;
    }

    function markAllCellsUnsynced(reason) {
        let changed = false;
        tetrapolCells.forEach((cell) => {
            if (cell.syncStatus !== 'unsynced' && cell.syncStatus !== 'lost') {
                cell.syncStatus = 'unsynced';
                changed = true;
            }
        });
        if (changed) {
            lastReceptionSig = '';
            updateReceptionDisplay();
        }
        return changed;
    }

    function applyTetrapolCellsSnapshot(list) {
        if (!Array.isArray(list)) return;
        if (!list.length) return; // keep last-known cells; timeout / disconnect mark them KO
        const next = new Map();
        const now = Date.now();
        list.forEach((raw) => {
            const channelId = Number(raw.channel_id);
            if (!Number.isFinite(channelId) || channelId <= 0) return;
            const prev = tetrapolCells.get(channelId);
            const syncErrs = Number(raw.sync_errs) || 0;
            let syncStatus = normalizeCellSyncStatus(raw.sync_status, syncErrs);
            const rawLast = Number(raw.last_cell_info_ms) || 0;
            // Comme tetrapol-kit : seuls cell_info / last_cell_info_ms comptent comme heartbeat.
            // Ne JAMAIS traiter un snapshot « synced » comme un refresh (sinon reste vert à jamais).
            let lastCellInfoTime = rawLast
                || (prev && prev.lastCellInfoTime)
                || 0;
            // Ne pas remonter unsynced → synced via snapshot périmé
            if (prev && prev.syncStatus === 'unsynced' && syncStatus === 'synced') {
                const age = lastCellInfoTime ? (now - lastCellInfoTime) : CELL_INFO_TIMEOUT_MS + 1;
                if (age > CELL_INFO_TIMEOUT_MS) syncStatus = 'unsynced';
            }
            if (lastCellInfoTime && (now - lastCellInfoTime) > CELL_INFO_TIMEOUT_MS
                && syncStatus !== 'lost') {
                syncStatus = 'unsynced';
            }
            next.set(channelId, {
                channelId,
                cellId: String(raw.cell_id || (prev && prev.cellId) || channelId),
                countryCode: raw.country_code != null ? Number(raw.country_code) : (prev ? prev.countryCode : null),
                bnId: raw.bn_id != null ? Number(raw.bn_id) : (prev ? prev.bnId : null),
                syncStatus,
                syncErrs,
                lastCellInfoTime
            });
        });
        // Preserve cells missing from this snapshot (still subject to timeout).
        tetrapolCells.forEach((prev, id) => {
            if (!next.has(id)) next.set(id, prev);
        });
        tetrapolCells = next;
        // Pas de lastTetrapolCellFeedMs ici : seul un vrai cell_info prouve que le CCH vit.
    }

    function handleTetrapolCellEvent(msg) {
        if (!msg || !msg.event) return false;
        if (msg.event === 'cell_info') {
            const channelId = Number(msg.channel_id);
            if (!Number.isFinite(channelId) || channelId <= 0) return true;
            let cell = tetrapolCells.get(channelId);
            if (!cell) {
                cell = { channelId, syncStatus: 'unknown', syncErrs: 0, lastCellInfoTime: null };
                tetrapolCells.set(channelId, cell);
            }
            cell.lastCellInfoTime = Date.now();
            lastTetrapolCellFeedMs = cell.lastCellInfoTime;
            if (cell.syncStatus === 'unsynced' || cell.syncStatus === 'unknown')
                cell.syncStatus = 'synced';
            cell.countryCode = msg.country_code != null ? Number(msg.country_code) : cell.countryCode;
            cell.bnId = msg.bn_id != null ? Number(msg.bn_id) : cell.bnId;
            cell.cellId = msg.cell_id != null ? String(msg.cell_id) : cell.cellId;
            lastReceptionSig = '';
            updateReceptionDisplay();
            return true;
        }
        if (msg.event === 'cch_sync') {
            const channelId = Number(msg.channel_id);
            if (!Number.isFinite(channelId) || channelId <= 0) return true;
            let cell = tetrapolCells.get(channelId);
            if (!cell) {
                // Comme tetrapol-kit : pas de cellule sans cell_info (évite faux vert).
                return true;
            }
            const syncErrs = Number(msg.sync_errs) || 0;
            // cch_sync ne rafraîchit PAS lastCellInfoTime (sinon reste vert sans cell_info).
            if (cell.syncStatus !== 'unsynced') {
                cell.syncStatus = normalizeCellSyncStatus(msg.status, syncErrs);
            }
            cell.syncErrs = syncErrs;
            lastReceptionSig = '';
            updateReceptionDisplay();
            return true;
        }
        return false;
    }

    function checkCellInfoTimeouts() {
        const now = Date.now();
        let changed = false;

        // No Tetrapol feed at all (link down / silence) → all cells KO.
        if (tetrapolCells.size && lastTetrapolCellFeedMs
            && (now - lastTetrapolCellFeedMs) > CELL_INFO_TIMEOUT_MS) {
            tetrapolCells.forEach((cell) => {
                if (cell.syncStatus !== 'unsynced' && cell.syncStatus !== 'lost') {
                    cell.syncStatus = 'unsynced';
                    changed = true;
                }
            });
        }

        tetrapolCells.forEach((cell) => {
            if (!cell.lastCellInfoTime) {
                // First sight without timestamp: start the clock, don't treat as fresh forever.
                cell.lastCellInfoTime = lastTetrapolCellFeedMs || now;
                return;
            }
            if ((now - cell.lastCellInfoTime) > CELL_INFO_TIMEOUT_MS) {
                if (cell.syncStatus !== 'unsynced' && cell.syncStatus !== 'lost') {
                    cell.syncStatus = 'unsynced';
                    changed = true;
                }
            }
        });
        if (!tetrapolBridged && tetrapolCells.size) {
            tetrapolCells.forEach((cell) => {
                if (cell.syncStatus !== 'unsynced' && cell.syncStatus !== 'lost') {
                    cell.syncStatus = 'unsynced';
                    changed = true;
                }
            });
        }
        if (changed) {
            lastReceptionSig = '';
            updateReceptionDisplay();
        }
    }

    function buildTetrapolCellCards() {
        if (tetrapolCells.size > 0) {
            return [...tetrapolCells.values()].sort((a, b) => a.channelId - b.channelId);
        }
        // Fallback only while bridged and waiting for first cell_info.
        if (!tetrapolBridged) return [];
        const byCell = new Map();
        tetrapolChannels.forEach((c) => {
            const cellId = c.is_dir ? 'DIR' : String(c.cell_id || '').trim();
            if (!cellId || cellId === '-' || cellId === 'null') return;
            if (!byCell.has(cellId)) {
                byCell.set(cellId, {
                    channelId: Number(c.channel_id) || 0,
                    cellId,
                    countryCode: null,
                    bnId: null,
                    syncStatus: 'unsynced',
                    syncErrs: 0,
                    lastCellInfoTime: lastTetrapolCellFeedMs || 0,
                    _fallback: true
                });
            }
        });
        return [...byCell.values()].sort((a, b) => String(a.cellId).localeCompare(String(b.cellId)));
    }

    function buildAnalogDeviceCards() {
        if (!airbandBridged) return [];
        const byDev = new Map();
        [...airbandById.values()].forEach((ch) => {
            const d = ch.device;
            if (d == null) return;
            if (!byDev.has(d)) {
                byDev.set(d, {
                    device: d,
                    total: 0,
                    listened: 0,
                    onAir: 0,
                    modes: new Set()
                });
            }
            const info = byDev.get(d);
            info.total += 1;
            const entry = catalog.get(airKey(ch.stream_id));
            if (entry ? isEnabled(entry) : !disabledAirband.has(ch.stream_id)) {
                info.listened += 1;
            }
            if (ch.on_air) info.onAir += 1;
            if (ch.modulation) info.modes.add(String(ch.modulation).toUpperCase());
        });
        return [...byDev.values()].sort((a, b) => a.device - b.device);
    }

    function receptionSyncClass(kind, item) {
        if (kind === 'cell') {
            return normalizeCellSyncStatus(item.syncStatus, item.syncErrs);
        }
        if (item.onAir > 0) return 'synced';
        if (item.listened > 0) return 'synced';
        return 'lost';
    }

    function receptionStatusText(kind, item, sync) {
        if (kind === 'cell') {
            if (sync === 'unsynced') return 'Non synchronisé';
            if (sync === 'synced') return 'Synchronisé';
            if (sync === 'unknown') return 'En attente';
            return 'Perte de sync';
        }
        if (item.onAir > 0) return 'En réception';
        if (item.listened > 0) return 'Écoute active';
        return 'Aucune fréquence activée';
    }

    function updateReceptionDisplay() {
        if (!el.receptionContainer) return;
        const cells = buildTetrapolCellCards();
        const devices = buildAnalogDeviceCards();
        const sig = JSON.stringify({
            c: cells.map(c => [c.channelId, c.cellId, c.syncStatus, c.syncErrs, c.countryCode, c.bnId]),
            d: devices.map(d => [d.device, d.listened, d.total, d.onAir])
        });
        if (sig === lastReceptionSig) return;
        lastReceptionSig = sig;

        if (!cells.length && !devices.length) {
            el.receptionContainer.innerHTML = '<div class="no-data">Aucune réception détectée</div>';
            return;
        }

        const parts = [];
        devices.forEach((dev) => {
            const sync = receptionSyncClass('device', dev);
            const status = receptionStatusText('device', dev, sync);
            const modes = [...dev.modes].join(' / ') || '—';
            parts.push(`
                <div class="cell-card ${sync}" data-kind="analog" data-device="${dev.device}">
                    <div class="cell-name">✈️ Device ${dev.device}</div>
                    <div class="cell-info">${dev.listened} fréquence${dev.listened > 1 ? 's' : ''} écoutée${dev.listened > 1 ? 's' : ''}
                        <span style="opacity:.75"> / ${dev.total} configurée${dev.total > 1 ? 's' : ''}</span>
                    </div>
                    <div class="cell-info">Mode : ${modes}</div>
                    <div class="cell-sync-status">
                        <span class="sync-indicator ${sync}"></span>
                        <span>${status}</span>
                        ${dev.onAir ? `<span>(${dev.onAir} ON AIR)</span>` : ''}
                    </div>
                </div>`);
        });
        cells.forEach((cell) => {
            const sync = receptionSyncClass('cell', cell);
            const status = receptionStatusText('cell', cell, sync);
            const countryName = getCountryName(cell.countryCode);
            const deptName = getDepartmentName(cell.bnId);
            const cellLabel = cell.cellId || String(cell.channelId);
            const title = `${countryName} - ${deptName} - ${cellLabel}`;
            parts.push(`
                <div class="cell-card ${sync}" data-kind="tetrapol" data-channel="${cell.channelId}">
                    <div class="cell-name">${escHtml(title)}</div>
                    <div class="cell-info">Channel: ${cell.channelId}</div>
                    <div class="cell-sync-status">
                        <span class="sync-indicator ${sync}"></span>
                        <span>${status}</span>
                        ${cell.syncErrs != null ? `<span>(${cell.syncErrs} erreur${cell.syncErrs > 1 ? 's' : ''})</span>` : ''}
                    </div>
                </div>`);
        });
        el.receptionContainer.innerHTML = parts.join('');
    }

    function getCountryName(countryCode) {
        if (countryCode == null || countryCode < 0) return 'Pays inconnu';
        return countriesMap[String(countryCode)] || 'Pays inconnu';
    }

    function getDepartmentName(bnId) {
        if (bnId == null || bnId < 0) return 'Département inconnu';
        return departmentsMap[String(bnId)] || 'Département inconnu';
    }

    /** Canal Tetrapol le plus pertinent pour un groupe (on_air récent, sinon dernier actif). */
    function findBestChannelForGroup(groupId) {
        const groupIdStr = String(groupId);
        const groupChannels = tetrapolChannels.filter((ch) => String(ch.group_id) === groupIdStr);
        if (!groupChannels.length) return null;
        const byRecency = (a, b) => (b.last_active_ms || 0) - (a.last_active_ms || 0);
        const onAirChannels = groupChannels.filter((ch) => ch.on_air).sort(byRecency);
        return onAirChannels[0] || [...groupChannels].sort(byRecency)[0];
    }

    /** Département de la cellule d'activation (comme tetrapol-kit). */
    function getGroupCommunicationDetails(groupId) {
        const channel = findBestChannelForGroup(groupId);
        if (!channel) return null;

        let departmentName = null;
        let departmentCode = null;

        const cellByChannelId = (channel.channel_id !== undefined && channel.channel_id !== null)
            ? (tetrapolCells.get(Number(channel.channel_id))
                || [...tetrapolCells.values()].find((c) => c.channelId == channel.channel_id)
                || null)
            : null;

        const cellByCellId = (channel.cell_id !== undefined && channel.cell_id !== null && channel.cell_id !== '-')
            ? ([...tetrapolCells.values()].find((c) => String(c.cellId) === String(channel.cell_id)) || null)
            : null;

        const cell = cellByChannelId || cellByCellId || null;
        if (cell && cell.bnId !== undefined && cell.bnId !== null) {
            departmentCode = String(cell.bnId);
            departmentName = getDepartmentName(cell.bnId);
        }

        return {
            isOnAir: !!channel.on_air,
            rfsi: channel.on_air ? (channel.rfsi || null) : null,
            rfsiLabel: channel.on_air ? (channel.rfsi_label || null) : null,
            departmentName,
            departmentCode
        };
    }

    /** Label groupe + « - {code} {département} » (même format que tetrapol-kit). */
    function getGroupDisplayNameWithDepartment(groupId) {
        const gid = String(groupId || '');
        const entry = catalog.get(tpKey(gid));
        const baseLabel = (entry ? labelOf(entry) : '')
            || String(tetrapolGroups.get(gid)?.group_label || '').trim()
            || gid
            || 'Groupe';
        const details = getGroupCommunicationDetails(gid);
        if (details && details.departmentName && details.departmentCode) {
            return `${baseLabel} - ${details.departmentCode} ${details.departmentName}`;
        }
        if (details && details.departmentName) {
            return `${baseLabel} - ${details.departmentName}`;
        }
        return baseLabel;
    }

    /** Nom de carte Tetrapol : figé sur le segment si présent, sinon calcul live. */
    function tetrapolCardGroupLabel(entry, frozenDisplayName) {
        const frozen = String(frozenDisplayName || '').trim();
        if (frozen) return frozen;
        const gid = entry && entry.rawId != null ? String(entry.rawId) : '';
        if (!gid) return labelOf(entry);
        return getGroupDisplayNameWithDepartment(gid);
    }

    /** Figé à la création du segment (évite le bascule entre départements en lecture). */
    function segmentDisplayName(seg) {
        if (!seg) return '';
        const entry = catalogEntryFor(seg) || seg.entry;
        if (!entry || entry.source !== 'tetrapol') return labelOf(entry);
        if (!seg.displayName) {
            seg.displayName = getGroupDisplayNameWithDepartment(entry.rawId);
        }
        return seg.displayName;
    }

    async function loadDepartments() {
        try {
            const r = await fetch('/departments.json');
            const data = await r.json();
            departmentsMap = data.departments || {};
            countriesMap = data.countries || {};
            lastReceptionSig = '';
            updateReceptionDisplay();
        } catch (e) {
            console.warn('departments.json', e);
        }
    }

    /* ---- Listings (stable) ---- */

    function trackAirbandInactive(ch) {
        const id = String(ch.stream_id || '');
        if (!id) return;
        const key = 'ab:' + id;
        const onAir = !!ch.on_air;
        if (channelWasOnAir[key] && !onAir && !(ch.last_close_ms > 0)) {
            channelInactiveSince[key] = Date.now();
        }
        if (onAir) delete channelInactiveSince[key];
        channelWasOnAir[key] = onAir;
    }

    function getAirbandInactiveSince(ch) {
        // Prefer server squelch-close timestamp; else local edge tracking.
        if (ch.last_close_ms > 0) return ch.last_close_ms;
        const id = String(ch.stream_id || '');
        return channelInactiveSince['ab:' + id] || null;
    }

    function getAirbandStatusText(ch) {
        if (ch.on_air) return 'En cours';
        const since = getAirbandInactiveSince(ch);
        if (!since) return 'Inactif';
        return formatChannelInactiveStatus(Math.max(0, Date.now() - since)) || 'Inactif';
    }

    function updateAirbandTable(channels) {
        if (!el.channelsTableBody) return;
        channels.forEach(trackAirbandInactive);
        const statuses = channels.map(ch => getAirbandStatusText(ch));
        const sig = channels.map((c, i) => [
            c.stream_id, c.on_air ? 1 : 0, c.label || '', c.modulation || '',
            c.last_close_ms || 0, statuses[i]
        ].join(':')).join('|');
        if (sig === lastAirSig) return;
        lastAirSig = sig;
        ensureInactiveRefreshTimer();
        if (!channels.length) {
            el.channelsTableBody.innerHTML = airbandBridged
                ? '<tr><td colspan="6" class="no-data">Aucune fréquence</td></tr>'
                : '<tr><td colspan="6" class="no-data">Analogique déconnecté</td></tr>';
            return;
        }
        el.channelsTableBody.innerHTML = channels.map((ch, i) => `<tr>
            <td data-label="Device">${ch.device}</td>
            <td data-label="Canal">${ch.channel}</td>
            <td data-label="Fréquence">${(ch.freq_hz / 1e6).toFixed(3)}</td>
            <td data-label="Mode">${ch.modulation || ''}</td>
            <td data-label="Label">${ch.label || ''}</td>
            <td data-label="État"><span class="status-indicator ${ch.on_air ? 'on-air' : 'off-air'}"></span>${statuses[i]}</td>
        </tr>`).join('');
    }

    function isStreamPriority(key) {
        return priorityKeys.has(key);
    }

    function isPulsePriority(key) {
        return pulsePriorityKeys.has(key);
    }

    function toggleStreamPriority(key) {
        if (priorityKeys.has(key)) priorityKeys.delete(key);
        else priorityKeys.add(key);
        saveDisabled();
        const entry = catalog.get(key);
        if (entry) entry.priority = priorityKeys.has(key);
        exclusiveQueue.forEach(s => {
            if (s.key === key && s.entry) s.entry.priority = priorityKeys.has(key);
        });
        if (exclusiveActive && exclusiveActive.key === key && exclusiveActive.entry) {
            exclusiveActive.entry.priority = priorityKeys.has(key);
        }
        lastAirBtnSig = '';
        lastTpSig = '';
        updateAirbandButtons();
        updateTetrapolListing();
        if (exclusivePlaybackMode) {
            refreshExclusiveCards();
            // Marking prio on a waiting card must cut in immediately when allowed.
            if (priorityKeys.has(key)) {
                const waiting = exclusiveQueue.find((s) => s.key === key);
                if (waiting) preemptActiveWithPriority(waiting);
                else checkPriorityPreempt();
            }
        }
    }

    async function togglePulsePriority(key) {
        const enabling = !pulsePriorityKeys.has(key);
        if (enabling) pulsePriorityKeys.add(key);
        else pulsePriorityKeys.delete(key);
        saveDisabled();
        const entry = catalog.get(key);
        lastPulseSig = '';
        // Prio indépendante de listen : ne pas activer/désactiver le canal.
        if (entry && entry.source === 'airband') {
            await apiPulseExclusive({
                priority_group: entry.rawId,
                priority: enabling
            });
            if (entry.meta) entry.meta.priority = enabling;
            const ch = airbandById.get(entry.rawId);
            if (ch) ch.priority = enabling;
        } else if (entry && entry.source === 'tetrapol') {
            tpPulsePrioLocalUntil = Date.now() + 1500;
            await syncTetrapolPulsePriorities();
        }
        rebuildCatalog();
        updatePulseAdmin(pulseExclusiveState);
    }

    async function syncTetrapolPulsePriorities() {
        const ids = [...pulsePriorityKeys]
            .filter((k) => k.startsWith('tp:'))
            .map((k) => k.slice(3));
        try {
            await fetch('/api/tetrapol/pulse-exclusive', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ priority_groups: ids })
            });
        } catch (e) {
            console.warn('tetrapol pulse priority sync', e);
        }
    }

    /** Apply priority_groups from Airband pulse_exclusive onto local ab: keys. */
    function applyAirbandPulsePriorities(pe) {
        if (!pe || typeof pe !== 'object') return false;
        const server = new Set();
        if (Array.isArray(pe.priority_groups)) {
            pe.priority_groups.forEach((id) => server.add(String(id)));
        }
        (pe.listen_groups || []).forEach((g) => {
            if (g && g.priority === true && g.group_id != null) server.add(String(g.group_id));
        });
        // Sans priority_groups ni listen_groups, ne pas effacer l'état local.
        if (!Array.isArray(pe.priority_groups) && !Array.isArray(pe.listen_groups)) return false;
        let changed = false;
        [...pulsePriorityKeys].forEach((k) => {
            if (!k.startsWith('ab:')) return;
            const id = k.slice(3);
            if (!server.has(id)) {
                pulsePriorityKeys.delete(k);
                changed = true;
            }
        });
        server.forEach((id) => {
            const k = airKey(id);
            if (!pulsePriorityKeys.has(k)) {
                pulsePriorityKeys.add(k);
                changed = true;
            }
        });
        if (changed) saveDisabled();
        return changed;
    }

    /** Apply priority_groups from Tetrapol pulse_exclusive onto local tp: keys. */
    function applyTetrapolPulsePriorities(pe) {
        if (!pe || typeof pe !== 'object' || !Array.isArray(pe.priority_groups)) return;
        if (Date.now() < tpPulsePrioLocalUntil) return;
        const server = new Set(pe.priority_groups.map(String));
        let changed = false;
        [...pulsePriorityKeys].forEach((k) => {
            if (!k.startsWith('tp:')) return;
            const gid = k.slice(3);
            if (!server.has(gid)) {
                pulsePriorityKeys.delete(k);
                changed = true;
            }
        });
        server.forEach((gid) => {
            const k = tpKey(gid);
            if (!pulsePriorityKeys.has(k)) {
                pulsePriorityKeys.add(k);
                changed = true;
            }
        });
        if (!changed) return;
        saveDisabled();
        lastPulseSig = '';
        lastTpSig = '';
        updateTetrapolListing();
    }

    function bindLongPressPriority(btn, onTogglePriority, onShortClick) {
        let pressStartTime = 0;
        let pressStartPos = null;
        let longPressFired = false;
        let longPressTimer = null;
        let lastPriorityToggleAt = 0;
        let lastTouchGestureAt = 0;

        const resetTouchPress = () => {
            pressStartTime = 0;
            pressStartPos = null;
            if (longPressTimer) {
                clearTimeout(longPressTimer);
                longPressTimer = null;
            }
        };

        const fireLongPress = () => {
            if (longPressFired || !pressStartTime) return;
            longPressFired = true;
            longPressTimer = null;
            lastPriorityToggleAt = Date.now();
            window.getSelection()?.removeAllRanges();
            try {
                onTogglePriority();
            } catch (err) {
                console.warn('priority toggle', err);
            }
            try {
                if (navigator.vibrate) navigator.vibrate(50);
            } catch (_) { /* ignore */ }
        };

        // Desktop: right-click / long-press context menu → priority
        btn.addEventListener('contextmenu', (e) => {
            e.preventDefault();
            e.stopPropagation();
            if (Date.now() - lastPriorityToggleAt < GESTURE_GUARD_MS) return;
            lastPriorityToggleAt = Date.now();
            try {
                onTogglePriority();
            } catch (err) {
                console.warn('priority toggle', err);
            }
        });

        // Desktop mouse: normal click → enable/disable (must not be blocked by touch guards)
        btn.addEventListener('click', (e) => {
            if (Date.now() - lastTouchGestureAt < GESTURE_GUARD_MS) {
                e.preventDefault();
                e.stopPropagation();
                return;
            }
            if (Date.now() - lastPriorityToggleAt < GESTURE_GUARD_MS) {
                e.preventDefault();
                e.stopPropagation();
                return;
            }
            try {
                onShortClick(e);
            } catch (err) {
                console.warn('short click', err);
            }
        });

        // Mobile: touch long-press → priority, short tap → enable/disable
        btn.addEventListener('touchstart', (e) => {
            if (!e.touches || !e.touches[0]) return;
            pressStartTime = Date.now();
            pressStartPos = { x: e.touches[0].clientX, y: e.touches[0].clientY };
            longPressFired = false;
            if (longPressTimer) clearTimeout(longPressTimer);
            longPressTimer = setTimeout(fireLongPress, LONG_PRESS_MS);
        }, { passive: true });

        btn.addEventListener('touchmove', (e) => {
            if (!pressStartPos || !e.touches || !e.touches[0]) return;
            const dx = e.touches[0].clientX - pressStartPos.x;
            const dy = e.touches[0].clientY - pressStartPos.y;
            if ((dx * dx + dy * dy) > MOVE_TOLERANCE * MOVE_TOLERANCE) {
                resetTouchPress();
                longPressFired = false;
            }
        }, { passive: true });

        btn.addEventListener('touchend', (e) => {
            const duration = pressStartTime ? Date.now() - pressStartTime : 0;
            const started = !!pressStartTime;
            lastTouchGestureAt = Date.now();
            if (longPressFired || duration >= LONG_PRESS_MS) {
                e.preventDefault();
                e.stopPropagation();
                if (!longPressFired) fireLongPress();
            } else if (started) {
                e.preventDefault();
                e.stopPropagation();
                try {
                    onShortClick(e);
                } catch (err) {
                    console.warn('short click', err);
                }
            }
            resetTouchPress();
            longPressFired = false;
        }, { passive: false });

        btn.addEventListener('touchcancel', () => {
            resetTouchPress();
            longPressFired = false;
        });
    }

    /* ---- Tetrapol channel table rules (from tetrapol-kit) ---- */

    function trackChannelInactive(channel) {
        const id = String(channel.channel_id || '');
        if (!id) return;
        const onAir = !!channel.on_air;
        if (channelWasOnAir[id] && !onAir && !channel.inactive_since_ms && !channel.last_active_ms) {
            channelInactiveSince[id] = Date.now();
        }
        if (onAir) delete channelInactiveSince[id];
        channelWasOnAir[id] = onAir;
    }

    function getChannelInactiveSince(channel) {
        if (channel.inactive_since_ms > 0) return channel.inactive_since_ms;
        if (channel.last_active_ms > 0) return channel.last_active_ms;
        return channelInactiveSince[String(channel.channel_id)] || null;
    }

    function getChannelHideSince(channel) {
        if (channel.on_air) return null;
        if (channel.tch_connected !== true) {
            return (channel.tch_disconnected_since_ms > 0 ? channel.tch_disconnected_since_ms : null)
                || getChannelInactiveSince(channel)
                || null;
        }
        return getChannelInactiveSince(channel) || null;
    }

    function getChannelHideLimitMs(channel) {
        return channel.tch_connected === true ? CHANNEL_INACTIVE_HIDE_MS : CHANNEL_NOT_CONNECTED_HIDE_MS;
    }

    function shouldDisplayChannelInTable(channel) {
        if (channel.on_air) return true;
        const hideSince = getChannelHideSince(channel);
        if (!hideSince) return true;
        return (Date.now() - hideSince) < getChannelHideLimitMs(channel);
    }

    function formatChannelInactiveStatus(elapsedMs) {
        const oneMinute = 60000;
        const oneHour = 60 * oneMinute;
        const oneDay = 24 * oneHour;
        if (elapsedMs < oneMinute) return 'Inactif';
        if (elapsedMs < oneHour) return `Inactif depuis ${Math.floor(elapsedMs / oneMinute)} min`;
        if (elapsedMs < oneDay) {
            const hours = Math.floor(elapsedMs / oneHour);
            return hours === 1 ? 'Inactif >1 heure' : `Inactif >${hours} heures`;
        }
        if (elapsedMs < CHANNEL_INACTIVE_HIDE_MS) return 'Inactif >1 jour';
        return 'Inactif';
    }

    function getChannelStatusText(channel) {
        if (channel.on_air) return 'En cours';
        const since = getChannelInactiveSince(channel);
        if (!since) return 'Inactif';
        return formatChannelInactiveStatus(Math.max(0, Date.now() - since)) || 'Inactif';
    }

    function ensureInactiveRefreshTimer() {
        if (inactiveRefreshTimer) return;
        inactiveRefreshTimer = setInterval(() => {
            lastTpSig = '';
            lastAirSig = '';
            updateTetrapolListing();
            updateAirbandTable([...airbandById.values()]);
        }, 60000);
    }

    function syncButtonGrid(container, entries, kind) {
        if (!container) return;
        const wanted = new Set(entries.map(e => e.key));
        [...container.querySelectorAll('.group-btn')].forEach(btn => {
            if (!wanted.has(btn.dataset.key)) btn.remove();
        });
        // Re-append in entries order so sort (group number, etc.) is reflected in the DOM.
        entries.forEach(entry => {
            let btn = container.querySelector('.group-btn[data-key="' + esc(entry.key) + '"]');
            if (!btn) {
                btn = document.createElement('button');
                btn.type = 'button';
                btn.className = 'group-btn';
                btn.dataset.key = entry.key;
                btn.dataset.kind = kind;
                const span = document.createElement('span');
                span.className = 'group-btn-label';
                btn.appendChild(span);
                bindLongPressPriority(
                    btn,
                    () => toggleStreamPriority(btn.dataset.key),
                    () => toggleMonitoring(btn.dataset.key)
                );
            }
            container.appendChild(btn);
            const prio = isStreamPriority(entry.key);
            btn.classList.toggle('enabled', isEnabled(entry));
            btn.classList.toggle('on-air', !!entry.on_air);
            btn.classList.toggle('priority', prio);
            const span = btn.querySelector('.group-btn-label');
            if (span) {
                const prefix = entry.source === 'tetrapol' ? '📻 ' : '✈️ ';
                span.textContent = prefix + labelOf(entry);
            }
            btn.title = prio
                ? 'Priorité file stream — clic droit / appui long pour retirer'
                : 'Stream · ' + entry.key + (entry.streamUrl ? '\n' + entry.streamUrl : '');
        });
    }

    function sortEntriesByGroupNumber(entries) {
        return [...entries].sort((a, b) => {
            const numA = parseInt(a.rawId, 10);
            const numB = parseInt(b.rawId, 10);
            if (Number.isFinite(numA) && Number.isFinite(numB) && numA !== numB) return numA - numB;
            return String(a.rawId).localeCompare(String(b.rawId), undefined, { numeric: true });
        });
    }

    function syncScopeButton(container, id, opts) {
        if (!container) return null;
        let btn = container.querySelector('.group-btn[data-scope-id="' + esc(id) + '"]');
        if (!btn) {
            btn = document.createElement('button');
            btn.type = 'button';
            btn.className = 'group-btn';
            btn.dataset.scopeId = id;
            btn.dataset.scopeKind = opts.kind;
            const span = document.createElement('span');
            span.className = 'group-btn-label';
            btn.appendChild(span);
            btn.addEventListener('click', () => {
                if (opts.kind === 'project') toggleProjectMonitoring(id);
                else if (opts.kind === 'device') toggleDeviceMonitoring(id);
                else if (opts.kind === 'network') toggleNetworkMonitoring(id);
            });
            container.appendChild(btn);
        }
        const span = btn.querySelector('.group-btn-label');
        if (span) span.textContent = opts.label;
        btn.classList.toggle('enabled', !!opts.enabled);
        btn.classList.toggle('on-air', !!opts.onAir);
        btn.title = opts.title || opts.label;
        return btn;
    }

    function updateScopeButtons() {
        if (isAdminPage) {
            if (el.scopeSection) el.scopeSection.style.display = 'none';
            return;
        }
        if (!audioEnabled) {
            if (el.scopeSection) el.scopeSection.style.display = 'none';
            lastScopeSig = '';
            return;
        }
        if (el.scopeSection) el.scopeSection.style.display = '';

        rememberScopeFromLiveData();

        // Live on-air / counts overlay sticky known sets — never wipe on empty tick.
        const liveDevices = new Map();
        airbandById.forEach((ch) => {
            if (ch.device == null) return;
            const id = String(ch.device);
            if (!liveDevices.has(id)) liveDevices.set(id, { onAir: false, total: 0 });
            const info = liveDevices.get(id);
            info.total += 1;
            if (ch.on_air) info.onAir = true;
        });
        knownScopeDevices.forEach((total, id) => {
            if (!liveDevices.has(id)) liveDevices.set(id, { onAir: false, total });
            else liveDevices.get(id).total = Math.max(liveDevices.get(id).total, total);
        });

        const networkIds = new Set([
            ...knownScopeNetworks.keys(),
            ...Object.keys(networkLabels)
        ]);
        tetrapolChannels.forEach((c) => {
            const nid = extractNetworkId(c.rfsi);
            if (nid) networkIds.add(nid);
        });
        const networks = [...networkIds].sort((a, b) => (parseInt(a, 10) || 0) - (parseInt(b, 10) || 0));

        const abOnAir = [...airbandById.values()].some((c) => c.on_air);
        const tpOnAir = tetrapolChannels.some((c) => c.on_air)
            || [...tetrapolGroups.values()].some((g) => g.on_air);

        const sig = JSON.stringify({
            ab: [airbandBridged, isSourceEnabled('airband'), abOnAir ? 1 : 0],
            tp: [tetrapolBridged || !!tetrapolWsUrl, isSourceEnabled('tetrapol'), tpOnAir ? 1 : 0],
            d: [...liveDevices.entries()].map(([id, d]) => [id, isDeviceEnabled(id) ? 1 : 0, d.onAir ? 1 : 0, d.total]),
            n: networks.map((id) => [
                id,
                isNetworkEnabled(id) ? 1 : 0,
                tetrapolChannels.some((c) => extractNetworkId(c.rfsi) === id && c.on_air) ? 1 : 0,
                knownScopeNetworks.get(id) || networkLabels[id] || ''
            ]),
            proj: [...knownScopeProjects].sort(),
            src: [...disabledSources].sort(),
            devices: [...disabledDevices].sort(),
            nets: [...disabledNetworks].sort()
        });
        if (sig === lastScopeSig) return;
        lastScopeSig = sig;
        saveDisabled();

        // Projets — sticky once seen (ne pas retirer sur un tick offline).
        if (el.projectButtons) {
            const wanted = new Set(knownScopeProjects);
            if (wanted.has('airband') || airbandBridged || airbandById.size) {
                wanted.add('airband');
                knownScopeProjects.add('airband');
                syncScopeButton(el.projectButtons, 'airband', {
                    kind: 'project',
                    label: '✈️ Analogique',
                    enabled: isSourceEnabled('airband'),
                    onAir: abOnAir,
                    title: 'Activer / désactiver tout le projet Analogique (écoute navigateur)'
                });
            }
            if (wanted.has('tetrapol') || tetrapolBridged || tetrapolWsUrl || tetrapolChannels.length) {
                wanted.add('tetrapol');
                knownScopeProjects.add('tetrapol');
                syncScopeButton(el.projectButtons, 'tetrapol', {
                    kind: 'project',
                    label: '📻 Tetrapol',
                    enabled: isSourceEnabled('tetrapol'),
                    onAir: tpOnAir,
                    title: 'Activer / désactiver tout le projet Tetrapol (écoute navigateur)'
                });
            }
            [...el.projectButtons.querySelectorAll('.group-btn')].forEach((btn) => {
                if (!wanted.has(btn.dataset.scopeId)) btn.remove();
            });
        }

        // Devices — restent affichés même projet désactivé / offline temporaire.
        const showDevices = liveDevices.size > 0;
        if (el.deviceButtonsTitle) el.deviceButtonsTitle.style.display = showDevices ? '' : 'none';
        if (el.deviceButtons && showDevices) {
            const sorted = [...liveDevices.entries()]
                .map(([id, d]) => ({ id, ...d }))
                .sort((a, b) => Number(a.id) - Number(b.id));
            const wanted = new Set(sorted.map((d) => d.id));
            sorted.forEach((d) => {
                const parentOn = isSourceEnabled('airband');
                syncScopeButton(el.deviceButtons, d.id, {
                    kind: 'device',
                    label: `Device ${d.id}`,
                    enabled: parentOn && isDeviceEnabled(d.id),
                    onAir: d.onAir,
                    title: parentOn
                        ? `Device ${d.id} — ${d.total} fréquence(s)`
                        : 'Projet Analogique désactivé — liste conservée'
                });
            });
            [...el.deviceButtons.querySelectorAll('.group-btn')].forEach((btn) => {
                if (!wanted.has(btn.dataset.scopeId)) btn.remove();
            });
        }

        // Réseaux — sticky.
        const showNets = networks.length > 0;
        if (el.networkButtonsTitle) el.networkButtonsTitle.style.display = showNets ? '' : 'none';
        if (el.networkButtons && showNets) {
            const wanted = new Set(networks);
            networks.forEach((id) => {
                const parentOn = isSourceEnabled('tetrapol');
                const onAir = tetrapolChannels.some(
                    (c) => extractNetworkId(c.rfsi) === id && c.on_air
                );
                const label = knownScopeNetworks.get(id) || networkLabels[id] || `Réseau ${id}`;
                syncScopeButton(el.networkButtons, id, {
                    kind: 'network',
                    label,
                    enabled: parentOn && isNetworkEnabled(id),
                    onAir,
                    title: parentOn
                        ? `Réseau ${id}`
                        : 'Projet Tetrapol désactivé — liste conservée'
                });
            });
            [...el.networkButtons.querySelectorAll('.group-btn')].forEach((btn) => {
                if (!wanted.has(btn.dataset.scopeId)) btn.remove();
            });
        }
    }

    function toggleProjectMonitoring(source) {
        if (source !== 'airband' && source !== 'tetrapol') return;
        if (isSourceEnabled(source)) {
            disabledSources.add(source);
            stopStreamsMatching((e) => e.source === source);
        } else {
            disabledSources.delete(source);
        }
        refreshAfterScopeChange();
    }

    function toggleDeviceMonitoring(deviceId) {
        const id = String(deviceId);
        if (isDeviceEnabled(id)) {
            disabledDevices.add(id);
            stopStreamsMatching((e) => deviceIdForEntry(e) === id);
        } else {
            disabledDevices.delete(id);
            // Réactiver le projet si on réactive un device
            disabledSources.delete('airband');
        }
        refreshAfterScopeChange();
    }

    function tetrapolGroupIdsForNetwork(networkId) {
        const id = String(networkId);
        const gids = new Set();
        tetrapolChannels.forEach((c) => {
            if (!c.group_id || !c.rfsi) return;
            if (extractNetworkId(c.rfsi) === id) gids.add(String(c.group_id));
        });
        tetrapolGroups.forEach((_g, gid) => {
            if (networkIdForGroup(gid) === id) gids.add(String(gid));
        });
        Object.keys(groupStreamNetworkIds).forEach((gid) => {
            if (groupStreamNetworkIds[gid] === id) gids.add(String(gid));
        });
        return [...gids];
    }

    /** Aligne disabledNetworks sur le mixer (WS / monitor Python / autre client). */
    function applyDisabledNetworksFromServer(rawList, opts = {}) {
        if (!Array.isArray(rawList)) return false;
        if (!opts.force && Date.now() < disabledNetworksLocalUntil) return false;
        const serverNets = new Set(rawList.map(String).filter(Boolean));
        const same = serverNets.size === disabledNetworks.size
            && [...serverNets].every((id) => disabledNetworks.has(id));
        if (same) {
            disabledNetworksLocalUntil = 0;
            return false;
        }

        const newlyDisabled = [...serverNets].filter((id) => !disabledNetworks.has(id));
        disabledNetworks = serverNets;
        newlyDisabled.forEach((id) => {
            stopStreamsMatching((e) => {
                if (e.source !== 'tetrapol') return false;
                const nid = extractNetworkId(e.rfsi || e.meta && e.meta.rfsi)
                    || networkIdForGroup(e.rawId);
                return String(nid) === id;
            });
        });
        saveDisabled();
        lastScopeSig = '';
        lastPulseSig = '';
        refreshAfterScopeChange();
        updatePulseNetworkButtons();
        updatePulseAdmin();
        return true;
    }

    function toggleNetworkMonitoring(networkId) {
        const id = String(networkId);
        // Réseaux = filtre RFSI uniquement (ne pas toucher listen des groupes Pulse).
        if (isNetworkEnabled(id)) {
            disabledNetworks.add(id);
            stopStreamsMatching((e) => {
                if (e.source !== 'tetrapol') return false;
                const nid = extractNetworkId(e.rfsi || e.meta && e.meta.rfsi)
                    || networkIdForGroup(e.rawId);
                return String(nid) === id;
            });
        } else {
            disabledNetworks.delete(id);
            disabledSources.delete('tetrapol');
        }
        disabledNetworksLocalUntil = Date.now() + 4000;
        saveDisabled();
        syncDisabledNetworksToMixer();
        refreshAfterScopeChange();
        updatePulseNetworkButtons();
        lastPulseSig = '';
        updatePulseAdmin();
    }

    function syncDisabledNetworksToMixer() {
        const nets = [...disabledNetworks].map(String);
        return apiTetrapolPulseExclusive({ disabled_networks: nets });
    }

    /** Boutons réseaux dans le panneau Pulse (admin) — visibles sans « Activer l'audio ». */
    function updatePulseNetworkButtons() {
        const box = document.getElementById('pulseNetworkButtonsContainer');
        const title = document.getElementById('pulseNetworkButtonsTitle');
        const empty = document.getElementById('pulseNetworkButtonsEmpty');
        if (!box) return;
        rememberScopeFromLiveData();
        const networkIds = new Set([
            ...knownScopeNetworks.keys(),
            ...Object.keys(networkLabels)
        ]);
        tetrapolChannels.forEach((c) => {
            const nid = extractNetworkId(c.rfsi);
            if (nid) networkIds.add(nid);
        });
        const networks = [...networkIds].sort(
            (a, b) => (parseInt(a, 10) || 0) - (parseInt(b, 10) || 0)
        );
        if (title) title.style.display = '';
        if (!networks.length) {
            if (empty) {
                empty.style.display = '';
                empty.textContent = 'Aucun réseau reçu (WS Tetrapol / labels NETWORKS)';
            }
            [...box.querySelectorAll('.pulse-btn')].forEach((b) => b.remove());
            return;
        }
        if (empty) empty.style.display = 'none';
        const wanted = new Set(networks);
        networks.forEach((id) => {
            const parentOn = isSourceEnabled('tetrapol');
            const onAir = tetrapolChannels.some(
                (c) => extractNetworkId(c.rfsi) === id && c.on_air
            );
            const label = knownScopeNetworks.get(id) || networkLabels[id] || `Réseau ${id}`;
            syncPulseNetworkButton(box, id, {
                label,
                enabled: parentOn && isNetworkEnabled(id),
                onAir,
                title: parentOn
                    ? `Réseau ${id} — coupe Icecast + Pulse`
                    : 'Projet Tetrapol désactivé'
            });
        });
        [...box.querySelectorAll('.pulse-btn')].forEach((btn) => {
            if (!wanted.has(btn.dataset.scopeId)) btn.remove();
        });
    }

    function syncPulseNetworkButton(container, id, opts) {
        if (!container) return null;
        let btn = container.querySelector('.pulse-btn[data-scope-id="' + esc(id) + '"]');
        if (!btn) {
            btn = document.createElement('button');
            btn.type = 'button';
            btn.className = 'pulse-btn';
            btn.dataset.scopeId = id;
            btn.dataset.scopeKind = 'network';
            const span = document.createElement('span');
            span.className = 'pulse-btn-label';
            btn.appendChild(span);
            btn.addEventListener('click', () => toggleNetworkMonitoring(id));
            container.appendChild(btn);
        }
        const span = btn.querySelector('.pulse-btn-label');
        if (span) span.textContent = opts.label;
        btn.classList.toggle('enabled', !!opts.enabled);
        btn.classList.toggle('on-air', !!opts.onAir);
        btn.title = opts.title || opts.label;
        return btn;
    }

    function updateAirbandButtons() {
        if (isAdminPage) {
            if (el.groupButtons) el.groupButtons.innerHTML = '';
            return;
        }
        if (!audioEnabled) {
            if (el.groupButtonsSection) el.groupButtonsSection.style.display = 'none';
            if (el.groupButtons) el.groupButtons.innerHTML = '';
            lastAirBtnSig = '';
            return;
        }
        if (el.groupButtonsSection) el.groupButtonsSection.style.display = '';
        const entries = [...catalog.values()].filter(e => e.source === 'airband');
        const sorted = sortEntriesByGroupNumber(entries);
        const sig = sorted.map(e => [
            e.key, isEnabled(e) ? 1 : 0, e.on_air ? 1 : 0,
            isStreamPriority(e.key) ? 1 : 0, e.label
        ].join(':')).join('|');
        if (sig === lastAirBtnSig) return;
        lastAirBtnSig = sig;
        syncButtonGrid(el.groupButtons, sorted, 'airband');
    }

    function updateTetrapolListing() {
        const visible = tetrapolChannels.filter(c => {
            trackChannelInactive(c);
            return shouldDisplayChannelInTable(c);
        });
        const sig = JSON.stringify({
            c: visible.map(c => [
                c.channel_id, c.group_id, c.on_air ? 1 : 0, c.rfsi || '',
                c.tch_connected === false ? 0 : 1, isTetrapolChannelEncrypted(c) ? 1 : 0,
                getChannelStatusText(c)
            ]),
            g: [...tetrapolGroups.values()].map(g => [
                g.group_id, g.on_air ? 1 : 0, g.group_label || '',
                isStreamPriority(tpKey(g.group_id)) ? 1 : 0
            ]),
            b: [...knownClearTetrapol.keys()].map((gid) => [
                gid,
                tetrapolGroupIsOnAir(gid) ? 1 : 0,
                disabledTetrapol.has(String(gid)) ? 1 : 0,
                isStreamPriority(tpKey(gid)) ? 1 : 0
            ]),
            labels: visible.map(c => [c.group_id, c.group_label || '', c.rfsi_label || ''])
        });
        if (sig === lastTpSig) return;
        lastTpSig = sig;
        ensureInactiveRefreshTimer();

        if (el.tetrapolBody) {
            if (!visible.length) {
                el.tetrapolBody.innerHTML = '<tr class="no-data-row"><td colspan="7" class="no-data">'
                    + (tetrapolChannels.length ? 'Aucune transmission récente' : 'Aucune transmission')
                    + '</td></tr>';
            } else {
                el.tetrapolBody.innerHTML = visible.map(c => {
                    const encrypted = isTetrapolChannelEncrypted(c);
                    const encIcon = encrypted ? '🔒' : '🔓';
                    const encClass = encrypted ? 'encrypted' : 'clear';
                    const cellId = c.is_dir ? 'DIR' : (c.cell_id || '-');
                    const rowClasses = [encrypted ? 'channel-encrypted' : 'channel-clear'];
                    if (c.tch_connected === false) rowClasses.push('channel-not-connected');
                    const status = getChannelStatusText(c);
                    return `<tr class="${rowClasses.join(' ')}">
                        <td data-label="Cellule">${cellId}</td>
                        <td data-label="Channel">${c.channel_id || '-'}</td>
                        <td data-label="Group"><div class="group-id-cell">
                            <span class="encryption-icon ${encClass}">${encIcon}</span>
                            <span>${c.group_id || '-'}</span>
                        </div></td>
                        <td data-label="Label">${c.group_label || '-'}</td>
                        <td data-label="RFSI">${c.rfsi || '-'}</td>
                        <td data-label="RFSI Label">${c.rfsi_label || '-'}</td>
                        <td data-label="État"><span class="status-indicator ${c.on_air ? 'on-air' : 'off-air'}"></span>${status}</td>
                    </tr>`;
                }).join('');
            }
        }
        if (!isAdminPage) {
            if (!audioEnabled) {
                if (el.groupButtonsSection) el.groupButtonsSection.style.display = 'none';
                if (el.tetrapolGroups) el.tetrapolGroups.innerHTML = '';
            } else {
                if (el.groupButtonsSection) el.groupButtonsSection.style.display = '';
                scanTetrapolClearGroups();
                syncButtonGrid(el.tetrapolGroups, sortEntriesByGroupNumber(getTetrapolButtonEntries()), 'tetrapol');
            }
        }
    }

    async function toggleMonitoring(key) {
        const k = String(key || '');
        let entry = catalog.get(k);
        if (!entry) {
            if (k.startsWith('tp:')) {
                const rawId = k.slice(3);
                if (!rawId) return;
                entry = { key: k, source: 'tetrapol', rawId, on_air: tetrapolGroupIsOnAir(rawId) };
            } else if (k.startsWith('ab:')) {
                const rawId = k.slice(3);
                if (!rawId) return;
                entry = { key: k, source: 'airband', rawId };
            } else {
                return;
            }
        }
        lastAirBtnSig = '';
        lastTpSig = '';
        lastReceptionSig = '';
        // Stream cards only — never touch Pulse / remote mixer listen here
        if (entry.source === 'airband') {
            const rawId = String(entry.rawId);
            if (disabledAirband.has(rawId)) {
                disabledAirband.delete(rawId);
                saveDisabled();
                updateAirbandButtons();
                updateReceptionDisplay();
                if (audioEnabled && shouldMonitorEntry(entry) && entry.on_air) requestStartEntry(entry);
            } else {
                disabledAirband.add(rawId);
                saveDisabled();
                updateAirbandButtons();
                updateReceptionDisplay();
                stopStreamMonitoring(entry);
            }
        } else {
            const rawId = String(entry.rawId);
            if (disabledTetrapol.has(rawId)) {
                disabledTetrapol.delete(rawId);
                saveDisabled();
                updateTetrapolListing();
                updateReceptionDisplay();
                if (audioEnabled && shouldMonitorEntry(entry) && entry.on_air) requestStartEntry(entry);
            } else {
                disabledTetrapol.add(rawId);
                saveDisabled();
                updateTetrapolListing();
                updateReceptionDisplay();
                stopStreamMonitoring(entry);
            }
        }
    }

    /* ---- Shared players / queue ---- */

    function ensureStreamsSection() {
        if (!el.streamsSection) return;
        const count = players.size + exclusiveQueue.length + (exclusiveActive ? 1 : 0);
        // Une fois l'audio activé, les commandes (volume / file) restent visibles.
        // Seules les cards apparaissent / disparaissent dans le conteneur.
        if (audioEnabled || count > 0) {
            el.streamsSection.style.display = 'block';
        }
        if (el.streamsContainer && count === 0) {
            if (!el.streamsContainer.querySelector('.no-data')) {
                el.streamsContainer.innerHTML = '<div class="no-data">Aucun stream actif</div>';
            }
        } else if (el.streamsContainer && count > 0) {
            const placeholder = el.streamsContainer.querySelector('.no-data');
            if (placeholder) placeholder.remove();
        }
        setDot(el.streamsStatus, count > 0);
    }

    function desiredPlaybackRate() {
        return playbackRateFast ? PLAYBACK_RATE_FAST : PLAYBACK_RATE_NORMAL;
    }

    function applyAudioOutput(audio) {
        if (!audio) return;
        audio.volume = globalVolume / 100;
        try { audio.playbackRate = desiredPlaybackRate(); } catch (_) {}
    }

    function applyPlaybackRateToAll() {
        const rate = desiredPlaybackRate();
        players.forEach((p) => {
            const a = getAudio(p);
            if (a) try { a.playbackRate = rate; } catch (_) {}
        });
        const allEx = [...exclusiveQueue];
        if (exclusiveActive) allEx.push(exclusiveActive);
        allEx.forEach((seg) => {
            if (seg.queueAudioEl) {
                try { seg.queueAudioEl.playbackRate = rate; } catch (_) {}
            }
            if (seg.playbackPlayer?.audioElement && seg.playbackPlayer.audioElement !== seg.queueAudioEl) {
                try { seg.playbackPlayer.audioElement.playbackRate = rate; } catch (_) {}
            }
        });
    }

    function getAudio(entry) {
        if (!entry) return null;
        return entry.player?.audioElement || entry.audio || null;
    }

    function updateCardLabels(card, audioEl, paused) {
        const muteBtn = card.querySelector('.mute-btn');
        const pauseBtn = card.querySelector('.pause-btn');
        if (muteBtn && audioEl) muteBtn.textContent = audioEl.muted ? '🔊 Unmute' : '🔇 Mute';
        if (pauseBtn) pauseBtn.textContent = paused ? '▶️ Lecture' : '⏸️ Pause';
    }

    function cancelBlankCountdown(key) {
        if (blankTimers[key]) {
            clearTimeout(blankTimers[key]);
            delete blankTimers[key];
        }
        delete blankSince[key];
        delete blankDeadlines[key];
    }

    /** Reprise réelle : le hold 7 s ne doit plus fermer la carte. */
    function clearBlankHoldForSpeech(seg) {
        if (!seg?.key) return;
        cancelBlankCountdown(seg.key);
        seg.playbackBlankSinceMs = null;
        seg.pendingFinishAfterDrain = false;
        seg._uiBlankSinceMs = null;
        if (seg.card) {
            seg.card.classList.remove('ending');
            seg.card.classList.add('playing');
        }
    }

    function isStickyIcyTitle(key, title) {
        const prev = (lastNonEmptyStreamTitles[key] || '').trim();
        return !!(title && prev && title === prev);
    }

    function tetrapolKeyIsOnAir(key) {
        if (!key || !String(key).startsWith('tp:')) return false;
        const entry = catalog.get(key);
        return !!(entry && entry.on_air);
    }

    /** Listing encore actif (Tetrapol ou airband) — ne pas fermer sur un blanc ICY transitoire. */
    function catalogKeyIsOnAir(key) {
        if (!key) return false;
        const entry = catalog.get(key);
        return !!(entry && entry.on_air);
    }

    /** Parole live réelle (ring) = StreamTitle non vide, y compris pendant la marge 0,7 s. */
    function ringHasLiveTalk(seg) {
        const rb = seg?.ringBuffer;
        if (!rb) return false;
        if (rb.inBlank) return false;
        const title = String(rb.lastTitle || '').trim();
        if (!title) return false;
        return true;
    }

    /** Vrai StreamTitle ICY déjà vu dans le ring (Label|RFSI ou RFSI / label). */
    function ringHadRealIcyTitle(seg) {
        const rb = seg?.ringBuffer;
        if (!rb) return false;
        if (typeof rb.ringHasRealTitle === 'function') return !!rb.ringHasRealTitle();
        if (rb._hadRealTitle) return true;
        return (rb.titleChanges || []).some((e) => String(e.title || '').trim());
    }

    /**
     * Tetrapol avec titre ICY réel : un blanc confirmé peut fermer la carte même si
     * le listing reste on_air (TCH-only colle souvent après la fin de parole).
     */
    function tetrapolMayEndOnIcyBlank(key, seg) {
        if (!key || !String(key).startsWith('tp:')) return false;
        return ringHadRealIcyTitle(seg);
    }

    function isBlockedAfterBlankClose(key) {
        if (!blankClosedOnAirGen.has(key)) return false;
        const closedGen = blankClosedOnAirGen.get(key);
        const curGen = onAirGeneration.get(key) || 0;
        // Même génération : rester bloqué (fermeture ICY avec listing encore on_air,
        // ou on_air↓ classique) jusqu'au prochain on_air↑ qui incrémente la gen.
        if (closedGen === curGen) return true;
        blankClosedOnAirGen.delete(key);
        return false;
    }

    function stopEntryAfterBlank(key) {
        const liveSeg = (exclusiveActive?.key === key ? exclusiveActive : null)
            || exclusiveQueue.find((s) => s.key === key)
            || null;
        // Groupe / canal encore on_air : sauf Tetrapol déjà titré ICY (blanc = fin).
        if (catalogKeyIsOnAir(key) && !tetrapolMayEndOnIcyBlank(key, liveSeg)) {
            if (liveSeg) {
                debugStreamTitleLog('blank-close-skipped-on_air', {
                    key,
                    groupLabel: labelOf(catalogEntryFor(liveSeg)),
                    path: 'listing-still-active'
                });
                clearBlankHoldForSpeech(liveSeg);
            }
            return;
        }
        if (liveSeg && ringHasLiveTalk(liveSeg)) {
            debugStreamTitleLog('blank-close-skipped-speech', {
                key,
                groupLabel: labelOf(catalogEntryFor(liveSeg)),
                path: 'ring-keep-card'
            });
            clearBlankHoldForSpeech(liveSeg);
            maybeSkipBlankToNextTransmission(liveSeg);
            return;
        }
        // Préempté / en file avec audio restant : ne pas jeter — reprendre après la prio.
        if (liveSeg
            && exclusiveActive !== liveSeg
            && segmentHasRingContent(liveSeg)) {
            debugStreamTitleLog('blank-close-skipped-queued', {
                key,
                groupLabel: labelOf(catalogEntryFor(liveSeg)),
                unreadSec: Number((getRingWaitingSeconds(liveSeg) || 0).toFixed(2)),
                path: 'preempt-keep-buffer'
            });
            clearBlankHoldForSpeech(liveSeg);
            liveSeg.pendingFinishAfterDrain = true;
            return;
        }
        blankClosedOnAirGen.set(key, onAirGeneration.get(key) || 0);
        stopEntry(key);
    }

    const INGEST_RECONNECT_HOLD_MS = 15000;

    function isTransientIngestError(err) {
        const msg = String(err && (err.message || err) || '');
        return /HTTP 50[234]|HTTP 404|Flux terminé|Failed to fetch|NetworkError|Load failed/i.test(msg);
    }

    function ingestIsReconnecting(seg) {
        if (!seg || seg.ingestDownSinceMs == null) return false;
        // Tetrapol déjà off-air : le mount Icecast a disparu avec la com.
        // Ne pas prolonger la carte sur une boucle 404 / « Flux terminé ».
        if (String(seg.key || '').startsWith('tp:') && !tetrapolKeyIsOnAir(seg.key)) {
            return false;
        }
        return (performance.now() - seg.ingestDownSinceMs) < INGEST_RECONNECT_HOLD_MS;
    }

    function markIngestDown(seg, err) {
        if (!seg || !isTransientIngestError(err)) return;
        if (seg.ingestDownSinceMs == null) seg.ingestDownSinceMs = performance.now();
    }

    function markIngestUp(seg) {
        if (seg) seg.ingestDownSinceMs = null;
    }

    function getBlankAgeMs(key, seg) {
        // Rattrapage : l'âge du blanc live ne doit pas fermer la carte.
        if (seg && getRingWaitingSeconds(seg) > 0.5 && !isPlaybackInBlank(seg)) {
            return 0;
        }
        if (seg?.ringBuffer && typeof seg.ringBuffer.effectiveBlankSecAt === 'function') {
            const rb = seg.ringBuffer;
            const bytePos = getPlaybackBytePos(seg);
            const inBlank = typeof rb.isBlankAtByte === 'function' && rb.isBlankAtByte(bytePos);
            // Ne compter que si la lecture est dans le blanc — pas le silence live
            // pendant qu'on rattrape encore de la parole.
            if (inBlank) {
                const fromRing = Math.round(rb.effectiveBlankSecAt(bytePos) * 1000);
                if (fromRing > 0) return fromRing;
                if (rb.inBlank && rb.blankStartedAtMs) {
                    return Math.max(0, Math.round(performance.now() - rb.blankStartedAtMs));
                }
            }
        }
        if (seg?.playbackBlankSinceMs != null) {
            return Math.max(0, performance.now() - seg.playbackBlankSinceMs);
        }
        if (blankSince[key] != null) return Math.max(0, Date.now() - blankSince[key]);
        return 0;
    }

    function isExclusiveSegmentKey(key) {
        return exclusiveActive?.key === key || exclusiveQueue.some((s) => s.key === key);
    }

    function isPlaybackInBlank(seg) {
        const bp = seg?.bufferPlayer;
        const rb = seg?.ringBuffer;
        if (!bp?.hasStarted || !rb || typeof rb.isBlankAtByte !== 'function') return false;
        const bytePos = typeof bp.playbackByteOffset === 'function'
            ? bp.playbackByteOffset()
            : rb.readByteOffset;
        return rb.isBlankAtByte(bytePos);
    }

    function updatePlaybackBlankState(seg) {
        if (!seg?.ringBuffer) {
            seg.playbackBlankSinceMs = null;
            return false;
        }
        const isActive = exclusiveActive === seg;
        if (!isActive || !seg.bufferPlayer?.hasStarted) {
            return !!seg.playbackBlankSinceMs;
        }
        const inBlank = isPlaybackInBlank(seg);
        if (inBlank) {
            if (seg.playbackBlankSinceMs == null) seg.playbackBlankSinceMs = performance.now();
            return true;
        }
        // Ne pas annuler le compte à rebours sur un faux positif MSE :
        // tant que le live est blanc / deadline active, on reste en "fin de com".
        if (blankDeadlines[seg.key] && liveEdgeIsBlank(seg)) return true;
        if (seg.playbackBlankSinceMs != null && liveEdgeIsBlank(seg)
            && getRingWaitingSeconds(seg) <= 0.5) {
            return true;
        }
        // Sortie réelle du blanc (nouvelle parole) — gérée par maybeSkip / title ahead.
        if (getTitleAheadOfPlayback(seg) || !liveEdgeIsBlank(seg)) {
            seg.playbackBlankSinceMs = null;
            return false;
        }
        return seg.playbackBlankSinceMs != null;
    }

    function getPlaybackBytePos(seg) {
        const bp = seg?.bufferPlayer;
        const rb = seg?.ringBuffer;
        if (!rb) return 0;
        if (bp && typeof bp.playbackByteOffset === 'function') return bp.playbackByteOffset();
        return rb.readByteOffset || 0;
    }

    /**
     * Titre non vide strictement après la position de lecture (nouvelle com).
     * Ne pas utiliser l'historique passé — sinon une vieille com bloque la fermeture.
     */
    function getTitleAheadOfPlayback(seg) {
        const rb = seg?.ringBuffer;
        if (!rb) return '';
        const bytePos = getPlaybackBytePos(seg);
        const next = typeof rb.nextTitleAfterByte === 'function'
            ? rb.nextTitleAfterByte(bytePos)
            : null;
        if (next) {
            const labeled = String(next.title || '').trim();
            if (labeled) return labeled;
            if (next.liveResume && String(rb.lastTitle || '').trim()) {
                return String(rb.lastTitle).trim();
            }
        }
        const live = String(rb.lastTitle || '').trim();
        if (!live || rb.inBlank) return '';
        // Live a quitté le blanc alors que la lecture est encore dessus.
        if (rb.isBlankAtByte?.(bytePos)) return live;
        return '';
    }

    function playbackHasSpeech(seg) {
        if (!seg) return false;
        if (getTitleAheadOfPlayback(seg)) return true;
        const rb = seg.ringBuffer;
        // Live déjà en blanc : le tampon peut encore jouer l'ancien titre — ce n'est pas une reprise.
        if (rb && (rb.inBlank || (rb.blankGraceActive && !String(rb.lastTitle || '').trim()))) {
            return false;
        }
        const shown = (getRingBufferDisplayTitle(seg) || '').trim();
        if (shown && !isPlaybackInBlank(seg)) return true;
        return liveHasRealSpeech(seg);
    }

    function liveEdgeIsBlank(seg) {
        const rb = seg?.ringBuffer;
        if (!rb) return true;
        if (rb.inBlank) return true;
        // Grâce après on_air↓ / ICY vide : lastTitle est déjà vide → blanc live.
        if (rb.blankGraceActive && !String(rb.lastTitle || '').trim()) return true;
        if (ringHasLiveTalk(seg)) return false;
        return !String(rb.lastTitle || '').trim();
    }

    /**
     * Si la lecture est dans un blanc mais qu'une nouvelle transmission est déjà
     * dans le tampon (ou arrive en live), sauter le blanc et continuer / rattraper
     * le direct — sans fermer la carte.
     */
    function maybeSkipBlankToNextTransmission(seg) {
        if (!seg || exclusiveActive !== seg || seg.suspendedByUser) return false;
        const rb = seg.ringBuffer;
        const bp = seg.bufferPlayer;
        if (!rb || !bp?.hasStarted) return false;

        const bytePos = getPlaybackBytePos(seg);
        const inBlank = typeof rb.isBlankAtByte === 'function' && rb.isBlankAtByte(bytePos);
        const ending = !!(blankDeadlines[seg.key]
            || blankTimers[seg.key]
            || seg.pendingFinishAfterDrain
            || seg.playbackBlankSinceMs != null);
        const next = (typeof rb.nextTitleAfterByte === 'function'
            ? rb.nextTitleAfterByte(bytePos)
            : null)
            || (inBlank && typeof rb.nextTitleAtOrAfterByte === 'function'
                ? rb.nextTitleAtOrAfterByte(bytePos)
                : null);
        const live = String(rb.lastTitle || '').trim();
        const liveHasTitle = !rb.inBlank && !!live;
        const shown = (getRingBufferDisplayTitle(seg) || '').trim();
        const liveResume = ringHasLiveTalk(seg);

        // Déjà sur la nouvelle com : annuler le hold, pas de seek.
        if (!inBlank && (shown || liveHasTitle || liveResume)) {
            if (ending) clearBlankHoldForSpeech(seg);
            return true;
        }

        if (!inBlank && !ending) return false;
        if (!next && !liveHasTitle && !liveResume) return false;

        clearBlankHoldForSpeech(seg);

        // Éviter une boucle seek toutes les ~250 ms (audio haché + titre qui
        // oscille blanc ↔ label) : un saut MSE soft (jumpToLive) laisse souvent
        // la tête dans la marge blanche déjà bufferisée.
        const now = performance.now();
        if (seg._skipBlankBusy) return true;
        if (seg._lastBlankSkipMs != null && (now - seg._lastBlankSkipMs) < 800) return true;
        seg._skipBlankBusy = true;
        seg._lastBlankSkipMs = now;

        let resumeAt = next;
        if (!resumeAt) {
            for (let i = (rb.titleChanges || []).length - 1; i >= 0; i--) {
                const e = rb.titleChanges[i];
                const speech = String(e.title || '').trim() || e.liveResume;
                if (speech && e.byteOffset >= bytePos) {
                    resumeAt = e;
                    break;
                }
            }
        }

        // Prefer seekToByte(title). jumpToLive only if almost caught up — otherwise
        // it dumps the whole catch-up buffer (gauge empties, card dies).
        const waiting = getRingWaitingSeconds(seg);
        let action;
        if (resumeAt && typeof bp.seekToByte === 'function') {
            const target = resumeAt.byteOffset;
            // Ne pas seek sur un marqueur liveResume vide à la fin du ring (= live).
            const isLiveEdgeResume = !!resumeAt.liveResume
                && !String(resumeAt.title || '').trim()
                && target >= (rb.totalBytesWritten - 64);
            if (!isLiveEdgeResume) {
                action = bp.seekToByte(target);
            } else if (waiting <= 0.5 && typeof bp.jumpToLive === 'function') {
                action = bp.jumpToLive();
            } else {
                action = Promise.resolve();
            }
        } else if (waiting <= 0.5 && typeof bp.jumpToLive === 'function') {
            action = bp.jumpToLive();
        } else {
            action = Promise.resolve();
        }

        Promise.resolve(action)
            .then(() => {
                syncRingBufferTitleDisplay(seg);
                updateRingProgressBar(seg);
            })
            .catch((err) => logStreamError(seg.key, err))
            .finally(() => { seg._skipBlankBusy = false; });
        return true;
    }

    function getBufferedLatestTitle(seg) {
        const rb = seg?.ringBuffer;
        if (!rb?.titleChanges?.length) return (seg?.ringLastTitle || '').trim();
        // Préférer le titre courant live s'il est non vide.
        if (!rb.inBlank && String(rb.lastTitle || '').trim()) {
            return String(rb.lastTitle).trim();
        }
        for (let i = rb.titleChanges.length - 1; i >= 0; i--) {
            const t = String(rb.titleChanges[i].title || '').trim();
            if (t) return t;
        }
        return '';
    }

    function getSegmentDisplayTitle(seg) {
        if (!seg) return '';
        // Listing off + hold : cacher le label seulement si plus rien à jouer
        // (sinon les coms < 1 s perdent le titre alors que l'audio est encore dans le ring).
        if (String(seg.key || '').startsWith('tp:')
            && !tetrapolKeyIsOnAir(seg.key)
            && (blankDeadlines[seg.key] || blankTimers[seg.key])) {
            const stillPlaying = segmentHasRingContent(seg)
                || ringHasLiveTalk(seg)
                || (getRingWaitingSeconds(seg) > 0.15 && !isPlaybackInBlank(seg));
            if (!stillPlaying) return '';
        }
        if (exclusiveActive === seg && seg.bufferPlayer?.hasStarted) {
            return getRingBufferDisplayTitle(seg);
        }
        return getBufferedLatestTitle(seg);
    }

    function updateEndingStatus(key) {
        const deadline = blankDeadlines[key];
        if (!deadline) return;
        const seg = (exclusiveActive?.key === key ? exclusiveActive : null)
            || exclusiveQueue.find(s => s.key === key)
            || null;
        // Tampon encore à vider (com courte) : rester en lecture, pas « En attente ».
        if (seg && usesQueueRingBuffer()) {
            const waiting = getRingWaitingSeconds(seg);
            const draining = waiting > 0.2
                && !isPlaybackInBlank(seg)
                && (segmentHasRingContent(seg) || ringHasLiveTalk(seg));
            if (draining) {
                if (seg.card) {
                    seg.card.classList.remove('ending');
                    seg.card.classList.add('playing');
                    const st = seg.card.querySelector('.card-status');
                    if (st) {
                        st.textContent = waiting > getLiveWaitingThreshold()
                            ? `Rattrapage (${waiting.toFixed(1)}s)`
                            : 'En cours';
                    }
                    const entry = catalogEntryFor(seg);
                    updateCardHeader(seg.card, entry, {
                        txTitle: getSegmentDisplayTitle(seg),
                        role: queueRoleLabel(seg, exclusiveActive === seg),
                        groupLabel: segmentDisplayName(seg)
                    });
                }
                return;
            }
        }
        const remainingMs = Math.max(0, deadline - Date.now());
        const remainingSec = Math.ceil(remainingMs / 1000);
        const text = remainingSec > 0 ? `En attente (${remainingSec}s)` : 'Fermeture…';
        const apply = (card) => {
            if (!card) return;
            card.classList.add('ending');
            card.classList.remove('playing');
            const st = card.querySelector('.card-status');
            if (st) st.textContent = text;
        };
        const p = players.get(key);
        apply(p?.card);
        if (exclusiveActive?.key === key) apply(exclusiveActive.card);
        const queued = exclusiveQueue.find(s => s.key === key);
        if (queued && queued !== exclusiveActive) apply(queued.card);
        try {
            document.querySelectorAll('.stream-card[data-key="' + CSS.escape(String(key)) + '"]').forEach(apply);
        } catch (_) { /* CSS.escape / query */ }
    }

    function ensureEndingStatusInterval() {
        if (inactivityStatusInterval) return;
        inactivityStatusInterval = setInterval(() => {
            const keys = Object.keys(blankDeadlines);
            keys.forEach(updateEndingStatus);
            if (!Object.keys(blankDeadlines).length) {
                clearInterval(inactivityStatusInterval);
                inactivityStatusInterval = null;
            }
        }, 250);
    }

    function armBlankCountdown(key, opts = {}) {
        if (blankTimers[key]) return;
        // Fenêtre post-on_air↑ : attendre un StreamTitle avant de relancer le hold 7 s.
        if (onAirTitleWaitTimers.has(key)) return;
        const holdSeg = (exclusiveActive?.key === key ? exclusiveActive : null)
            || exclusiveQueue.find(s => s.key === key)
            || null;
        // Listing encore actif : blanc ICY ≠ fin de com (airband / TCH sans titre).
        // Tetrapol déjà titré (Label|RFSI) : autoriser la fin même si on_air colle.
        // force = on_air↓ explicite (listing déjà faux).
        if (!opts.force && catalogKeyIsOnAir(key) && !tetrapolMayEndOnIcyBlank(key, holdSeg)) return;
        // force : listing on_air↓ alors que le ring a encore le talker ICY (pas de blanc stream).
        if (!opts.force) {
            if (holdSeg && playbackHasSpeech(holdSeg)) return;
            if (holdSeg && ringHasLiveTalk(holdSeg)) return;
        }
        // Rattrapage : vider le tampon d'abord, hold 7 s ensuite.
        if (holdSeg && getRingWaitingSeconds(holdSeg) > 0.5 && !isPlaybackInBlank(holdSeg)) {
            holdSeg.pendingFinishAfterDrain = true;
            return;
        }

        const seg = holdSeg;
        if (blankSince[key] == null) {
            blankSince[key] = seg?.playbackBlankSinceMs != null
                ? (Date.now() - (performance.now() - seg.playbackBlankSinceMs))
                : Date.now();
        }
        const age = getBlankAgeMs(key, seg);
        const entryForLabel = catalogEntryFor(seg) || catalog.get(key);
        debugStreamTitleLog('blank-countdown', {
            key,
            groupLabel: entryForLabel ? labelOf(entryForLabel) : '',
            ageMs: Math.round(age),
            holdMs: STREAM_END_HOLD_MS,
            liveBlank: seg ? liveEdgeIsBlank(seg) : null,
            playbackInBlank: seg ? isPlaybackInBlank(seg) : null,
            StreamTitleAtPlay: seg ? (getSegmentDisplayTitle(seg) || '(empty)') : null
        });

        // Blanc déjà ≥ 7 s (sauté à l'ingest + wall) → aucune reprise : fermer tout de suite.
        if (age >= STREAM_END_HOLD_MS) {
            const liveSeg = (exclusiveActive?.key === key ? exclusiveActive : null)
                || exclusiveQueue.find((s) => s.key === key);
            if (liveSeg && (getTitleAheadOfPlayback(liveSeg) || playbackHasSpeech(liveSeg)
                || ringHasLiveTalk(liveSeg))) {
                maybeSkipBlankToNextTransmission(liveSeg);
                return;
            }
            // En file avec tampon : garder pour reprise (préemption prio).
            if (liveSeg && exclusiveActive !== liveSeg && segmentHasRingContent(liveSeg)) {
                liveSeg.pendingFinishAfterDrain = true;
                return;
            }
            if (ingestIsReconnecting(liveSeg)) return;
            // Rattrapage encore à lire.
            if (liveSeg && getRingWaitingSeconds(liveSeg) > 0.5 && !isPlaybackInBlank(liveSeg)) {
                liveSeg.pendingFinishAfterDrain = true;
                return;
            }
            if (!liveSeg || liveEdgeIsBlank(liveSeg)) stopEntryAfterBlank(key);
            return;
        }

        const remainingMs = Math.max(50, STREAM_END_HOLD_MS - age);
        blankDeadlines[key] = Date.now() + remainingMs;

        const p = players.get(key);
        if (p) {
            p.ending = true;
            updateEndingStatus(key);
        } else {
            updateEndingStatus(key);
        }

        blankTimers[key] = setTimeout(() => {
            delete blankTimers[key];
            delete blankSince[key];
            delete blankDeadlines[key];
            const liveSeg = (exclusiveActive?.key === key ? exclusiveActive : null)
                || exclusiveQueue.find((s) => s.key === key);
            // Nouvelle com dans le tampon / en cours : sauter le blanc, ne pas fermer.
            if (liveSeg && (getTitleAheadOfPlayback(liveSeg) || playbackHasSpeech(liveSeg))) {
                maybeSkipBlankToNextTransmission(liveSeg);
                return;
            }
            const waiting = liveSeg ? getRingWaitingSeconds(liveSeg) : 0;
            // En file (préempté ou jamais lu) avec audio : attendre la promotion.
            if (liveSeg
                && exclusiveActive !== liveSeg
                && (segmentHasRingContent(liveSeg) || ringHasLiveTalk(liveSeg)
                    || getTitleAheadOfPlayback(liveSeg) || waiting > 0.3)) {
                liveSeg.pendingFinishAfterDrain = true;
                return;
            }
            // Listing off + tampon vidé : fermer même si un StreamTitle est encore collé dans le ring.
            if (liveSeg && String(key).startsWith('tp:') && !tetrapolKeyIsOnAir(key) && waiting <= 0.3) {
                stopEntryAfterBlank(key);
                return;
            }
            if (liveSeg && ringHasLiveTalk(liveSeg)) {
                maybeSkipBlankToNextTransmission(liveSeg);
                return;
            }
            if (liveSeg && !liveEdgeIsBlank(liveSeg)) return;

            if (liveSeg && usesQueueRingBuffer()) {
                // Tampon encore à lire (même si le live est déjà blanc).
                if (waiting > 0.3) {
                    liveSeg.pendingFinishAfterDrain = true;
                    if (liveSeg.card && exclusiveActive === liveSeg) {
                        liveSeg.card.classList.add('ending');
                        const st = liveSeg.card.querySelector('.card-status');
                        if (st) st.textContent = 'Fin du tampon…';
                    }
                    return;
                }
            }
            if (ingestIsReconnecting(liveSeg)) {
                armBlankCountdown(key);
                return;
            }
            stopEntryAfterBlank(key);
        }, remainingMs);

        ensureEndingStatusInterval();
    }

    function cancelOnAirTitleWait(key) {
        const t = onAirTitleWaitTimers.get(key);
        if (t) {
            clearTimeout(t);
            onAirTitleWaitTimers.delete(key);
        }
    }

    /** on_air↑ sans titre ICY après ON_AIR_TITLE_WAIT_MS → com fantôme / listing collé. */
    function failOnAirTitleWait(key) {
        onAirTitleWaitTimers.delete(key);
        const liveEntry = catalog.get(key);
        const liveSeg = (exclusiveActive?.key === key ? exclusiveActive : null)
            || exclusiveQueue.find((s) => s.key === key)
            || null;
        if (!liveSeg?.ringBuffer) return;
        const rb = liveSeg.ringBuffer;
        const title = String(rb.lastTitle || '').trim();
        if (title && !rb.inBlank && !rb.blankGraceActive) return;

        debugStreamTitleLog('on_air-title-timeout', {
            key,
            groupLabel: labelOf(liveEntry || catalogEntryFor(liveSeg)),
            on_air: !!liveEntry?.on_air,
            waitMs: ON_AIR_TITLE_WAIT_MS,
            lastTitle: title || '(empty)',
            path: 'on_air-title-wait'
        });
        liveSeg.ringLastTitle = '';
        lastStreamTitles[key] = '';
        syncRingBufferTitleDisplay(liveSeg);
        // liveSpeech reste true si listing encore on_air : un titre tardif peut encore sauver.
        armBlankCountdown(key);
    }

    function armOnAirTitleWait(key) {
        cancelOnAirTitleWait(key);
        onAirTitleWaitTimers.set(key, setTimeout(() => {
            failOnAirTitleWait(key);
        }, ON_AIR_TITLE_WAIT_MS));
    }

    /**
     * Tetrapol on_air↑ immédiat : stop countdown, stocker l'audio, attendre un titre ≤ 5 s.
     */
    function applyTetrapolOnAirResume(entry, seg) {
        if (!entry?.key || !seg) return;
        const key = entry.key;
        if (!entry.on_air) return;
        if (typeof seg.ringBuffer?.setLiveSpeech === 'function') {
            seg.ringBuffer.setLiveSpeech(true);
        }
        clearBlankHoldForSpeech(seg);
        maybeSkipBlankToNextTransmission(seg);
        const rb = seg.ringBuffer;
        const hasTitle = !!(rb && String(rb.lastTitle || '').trim()
            && !rb.inBlank && !rb.blankGraceActive);
        debugStreamTitleLog('on_air-resume', {
            key,
            source: entry?.source || '?',
            groupLabel: labelOf(entry),
            ringInBlank: !!rb?.inBlank,
            blankGraceActive: !!rb?.blankGraceActive,
            hasTitle,
            playbackInBlank: !!isPlaybackInBlank(seg),
            path: 'on_air-rising-edge'
        });
        if (hasTitle) {
            cancelOnAirTitleWait(key);
            return;
        }
        armOnAirTitleWait(key);
    }

    function noteEntryActivityForBlank(entry) {
        if (!entry || !entry.key) return;
        const key = entry.key;
        const onAir = !!entry.on_air;
        const was = entryWasOnAir.get(key);
        if (onAir && was !== true) {
            onAirGeneration.set(key, (onAirGeneration.get(key) || 0) + 1);
            blankClosedOnAirGen.delete(key);
        }
        entryWasOnAir.set(key, onAir);
        const seg = (exclusiveActive?.key === key ? exclusiveActive : null)
            || exclusiveQueue.find(s => s.key === key)
            || null;
        const isTetrapol = (entry.source === 'tetrapol')
            || String(key).startsWith('tp:');

        // Analogique : comme Tetrapol — ne pas injecter de blanc dans le ring sur
        // on_air↓ (coms courtes en file : sinon tampon vidé → carte fermée sans lecture).
        if (!isTetrapol) {
            if (onAir && was !== true) {
                // Front montant : reprendre le stockage / annuler un hold lancé sur blanc ICY.
                if (seg?.ringBuffer && typeof seg.ringBuffer.setLiveSpeech === 'function') {
                    seg.ringBuffer.setLiveSpeech(true);
                }
                if (seg) {
                    clearBlankHoldForSpeech(seg);
                    if (exclusiveActive === seg) maybeSkipBlankToNextTransmission(seg);
                    debugStreamTitleLog('on_air-resume', {
                        key,
                        source: entry?.source || '?',
                        groupLabel: labelOf(entry),
                        ringInBlank: !!seg.ringBuffer?.inBlank,
                        blankGraceActive: !!seg.ringBuffer?.blankGraceActive,
                        path: 'on_air-rising-edge'
                    });
                }
                return;
            }
            if (was === true && !onAir) {
                if (seg?.ringBuffer && typeof seg.ringBuffer.setLiveSpeech === 'function') {
                    seg.ringBuffer.setLiveSpeech(false);
                }
                if (seg && !seg.ringBuffer?.awaitingFirstTitle) {
                    debugStreamTitleLog('blank-from-on_air', {
                        key,
                        source: entry?.source || '?',
                        groupLabel: labelOf(entry),
                        StreamTitle: '(empty)',
                        ringLastTitle: String(seg.ringBuffer?.lastTitle || '').trim() || '(empty)',
                        unreadSec: Number((getRingWaitingSeconds(seg) || 0).toFixed(2)),
                        path: 'on_air-falling-edge-ui-only'
                    });
                    if (!segmentHasRingContent(seg) && !ringHasLiveTalk(seg)) {
                        seg.ringLastTitle = '';
                        lastStreamTitles[key] = '';
                    }
                    syncRingBufferTitleDisplay(seg);
                    // Countdown seulement en lecture ; en file, garder le tampon jusqu'au tour.
                    if (exclusiveActive === seg) {
                        armBlankCountdown(key, { force: true });
                    }
                } else if (!seg) {
                    const hadTitle = !!(lastStreamTitles[key] || '').trim()
                        || !!(players.get(key));
                    if (hadTitle) handleStreamMetadata(key, '');
                }
            }
            return;
        }

        // Tetrapol : on_air↓ = UI + countdown seulement — ne pas injecter de blanc
        // dans le ring (le StreamTitle Icecast peut encore être non vide).
        if (!onAir) {
            cancelOnAirTitleWait(key);
            if (seg?.ringBuffer && typeof seg.ringBuffer.setLiveSpeech === 'function') {
                seg.ringBuffer.setLiveSpeech(false);
            }
            if (was === true && seg && !seg.ringBuffer?.awaitingFirstTitle) {
                debugStreamTitleLog('blank-from-on_air', {
                    key,
                    source: entry?.source || '?',
                    groupLabel: labelOf(entry),
                    StreamTitle: '(empty)',
                    ringLastTitle: String(seg.ringBuffer?.lastTitle || '').trim() || '(empty)',
                    unreadSec: Number((getRingWaitingSeconds(seg) || 0).toFixed(2)),
                    path: 'on_air-falling-edge-ui-only'
                });
                // Ne pas effacer le label tant qu'il reste de l'audio / un titre ring :
                // les coms < 1 s ont souvent on_air↓ avant la fin de lecture.
                if (!segmentHasRingContent(seg) && !ringHasLiveTalk(seg)) {
                    seg.ringLastTitle = '';
                    lastStreamTitles[key] = '';
                }
                syncRingBufferTitleDisplay(seg);
                armBlankCountdown(key, { force: true });
            }
            return;
        }

        if (was === true) return;

        // Front montant : reprise immédiate + fenêtre 5 s pour un StreamTitle.
        if (seg) {
            applyTetrapolOnAirResume(entry, seg);
        } else {
            // Pas encore de carte : requestStartEntry créera le segment ; liveSpeech
            // sera posé au premier noteEntry une fois le ring prêt (was déjà true).
            // Forcer was=false n'est pas possible ici — le ring démarre avec _liveSpeech true.
        }
    }

    function handleStreamMetadata(key, meta) {
        let t = '';
        if (typeof meta === 'string') {
            t = meta.trim();
        } else if (meta && typeof meta === 'object') {
            t = String(meta.StreamTitle || '').trim();
        }
        const previous = (lastStreamTitles[key] || '').trim();
        const isExclusive = isExclusiveSegmentKey(key);

        const p = players.get(key);
        const seg = (exclusiveActive?.key === key ? exclusiveActive : null)
            || exclusiveQueue.find(s => s.key === key);
        const card = p?.card || seg?.card;
        const entry = catalogEntryFor(p?.entry || seg) || p?.entry || seg?.entry || catalog.get(key);
        const ending = !!(blankTimers[key] || blankDeadlines[key] || p?.ending
            || seg?.playbackBlankSinceMs != null);

        debugStreamTitleLog(t ? 'title' : 'blank', {
            key,
            source: entry?.source || '?',
            groupLabel: entry ? labelOf(entry) : '',
            StreamTitle: t || '(empty)',
            previous: previous || '(empty)',
            sticky: !!(t && isStickyIcyTitle(key, t)),
            path: 'live-player'
        });

        if (t && ending && isStickyIcyTitle(key, t)) {
            debugStreamTitleLog('sticky-ignored', {
                key,
                StreamTitle: t,
                lastNonEmpty: lastNonEmptyStreamTitles[key] || '',
                path: 'live-player'
            });
            return;
        }

        if (isExclusive) {
            if (t && seg) {
                lastNonEmptyStreamTitles[key] = t;
                seg.ringLastTitle = t;
                const rb = seg.ringBuffer;
                const accepted = !!(rb && !rb.inBlank
                    && String(rb.lastTitle || '').trim() === t);
                if (accepted) {
                    clearBlankHoldForSpeech(seg);
                    maybeSkipBlankToNextTransmission(seg);
                }
                syncRingBufferTitleDisplay(seg);
            } else if (!t && seg) {
                lastStreamTitles[key] = '';
                const waiting = getRingWaitingSeconds(seg);
                if (!catalogKeyIsOnAir(key)
                    && (isPlaybackInBlank(seg) || waiting <= 0.5 || !seg.bufferPlayer?.hasStarted)) {
                    armBlankCountdown(key);
                }
            }
            return;
        }

        lastStreamTitles[key] = t;

        if (t) {
            lastNonEmptyStreamTitles[key] = t;
            if (String(key || '').startsWith('tp:')) {
                rememberGroupStreamNetwork(String(key).slice(3), t);
            }
            cancelBlankCountdown(key);
            if (p) {
                p.ending = false;
                if (p.card) {
                    p.card.classList.add('icecast-active', 'playing');
                    p.card.classList.remove('ending');
                    updateCardHeader(p.card, p.entry, { txTitle: t });
                    const st = p.card.querySelector('.card-status');
                    if (st) st.textContent = 'En cours';
                    updateCardLabels(p.card, getAudio(p), p.paused);
                }
            }
            if (card && seg) {
                card.classList.remove('ending');
                if (exclusiveActive?.key === key) card.classList.add('playing');
                updateCardHeader(card, entry, {
                    txTitle: t,
                    role: queueRoleLabel(seg, exclusiveActive === seg),
                    groupLabel: segmentDisplayName(seg)
                });
                const st = card.querySelector('.card-status');
                if (st && exclusiveActive?.key === key) st.textContent = 'En cours';
            }
        } else {
            if (card && entry) {
                updateCardHeader(card, entry, {
                    txTitle: '',
                    role: queueRoleLabel(seg, exclusiveActive === seg),
                    groupLabel: segmentDisplayName(seg)
                });
            }
            const everHadTitle = !!(lastNonEmptyStreamTitles[key] || previous);
            const startedAt = p?.startedAtMs || seg?.createdAtMs || 0;
            const tooNew = !everHadTitle && startedAt && (performance.now() - startedAt) < 3500;
            if (!tooNew && (p || seg)) armBlankCountdown(key);
        }
    }

    function clearPendingCardStart(key) {
        const t = pendingCardStarts.get(key);
        if (t) clearTimeout(t);
        pendingCardStarts.delete(key);
    }

    /**
     * Démarre une carte seulement si on_air reste vrai ≥ MIN_ON_AIR_START_MS.
     * Si la carte existe déjà, ne rien filtrer (les petites reprises comptent).
     */
    function requestStartEntry(entry) {
        if (!shouldMonitorEntry(entry)) return;
        const key = entry.key;
        if (isBlockedAfterBlankClose(key)) return;
        const ownerKey = streamUrlOwnerKey(entry.streamUrl);
        if (ownerKey !== key) return;
        const has = players.has(key)
            || exclusiveQueue.some((s) => s.key === key)
            || (exclusiveActive && exclusiveActive.key === key);
        if (has) {
            clearPendingCardStart(key);
            return;
        }
        if (pendingCardStarts.has(key)) return;
        const timer = setTimeout(() => {
            pendingCardStarts.delete(key);
            const e = catalog.get(key) || entry;
            if (!e || !e.on_air) return;
            if (!shouldMonitorEntry(e)) return;
            const ownerKey = streamUrlOwnerKey(e.streamUrl);
            if (ownerKey !== key) return;
            const stillHas = players.has(key)
                || exclusiveQueue.some((s) => s.key === key)
                || (exclusiveActive && exclusiveActive.key === key);
            if (stillHas) return;
            if (isBlockedAfterBlankClose(key)) return;
            startEntry(e);
        }, MIN_ON_AIR_START_MS);
        pendingCardStarts.set(key, timer);
    }

    function startEntry(entry) {
        clearPendingCardStart(entry?.key);
        if (!shouldMonitorEntry(entry)) return;
        if (isBlockedAfterBlankClose(entry.key)) return;
        const ownerKey = streamUrlOwnerKey(entry.streamUrl);
        if (ownerKey !== entry.key) return;
        if (players.has(entry.key)) return;
        // Already in exclusive queue
        if (exclusiveQueue.some(s => s.key === entry.key) || (exclusiveActive && exclusiveActive.key === entry.key))
            return;

        if (exclusivePlaybackMode) {
            createExclusiveSegment(entry);
            ensureStreamsSection();
            return;
        }

        let audioEl;
        let player = createStreamPlayer(entry.streamUrl, entry.key);
        if (player) {
            audioEl = player.audioElement;
            applyAudioOutput(audioEl);
            player.play();
        } else {
            audioEl = new Audio(entry.streamUrl);
            audioEl.crossOrigin = 'anonymous';
            applyAudioOutput(audioEl);
            audioEl.play().catch(() => {});
        }

        const card = createLiveCard(entry, audioEl);
        players.set(entry.key, {
            player, audio: audioEl, card, entry,
            paused: false, ending: false, userPaused: false,
            startedAtMs: performance.now()
        });
        // Until first non-empty title, do not arm blank (stream may buffer)
        lastStreamTitles[entry.key] = '';
        ensureStreamsSection();
    }

    function stopEntry(key) {
        clearPendingCardStart(key);
        cancelBlankCountdown(key);
        cancelOnAirTitleWait(key);
        delete lastStreamTitles[key];
        delete lastNonEmptyStreamTitles[key];
        const p = players.get(key);
        if (p) {
            try {
                if (p.player?.stop) p.player.stop();
                else if (p.audio) { p.audio.pause(); p.audio.src = ''; }
            } catch (_) {}
            if (p.card?.parentNode) p.card.parentNode.removeChild(p.card);
            players.delete(key);
        }
        exclusiveQueue = exclusiveQueue.filter(s => {
            if (s.key !== key) return true;
            cleanupExclusive(s);
            return false;
        });
        if (exclusiveActive && exclusiveActive.key === key) {
            cleanupExclusive(exclusiveActive);
            exclusiveActive = null;
            promoteNext();
        }
        offAirSince.delete(key);
        ensureStreamsSection();
    }

    function createLiveCard(entry, audioEl) {
        const placeholder = el.streamsContainer.querySelector('.no-data');
        if (placeholder) placeholder.remove();

        const card = document.createElement('div');
        card.className = 'stream-card playing';
        card.dataset.key = entry.key;
        card.innerHTML = `
            ${cardHeaderHtml(entry, '')}
            <div class="stream-controls stream-card-zone stream-card-zone-controls">
                <button type="button" class="mute-btn">🔇 Mute</button>
                <button type="button" class="pause-btn">⏸️ Pause</button>
                <button type="button" class="stop-btn">⏹️ Stop</button>
            </div>
            <div class="icecast-info stream-card-zone stream-card-zone-status">
                <div><strong>📊 Status:</strong> <span class="card-status">En cours</span></div>
            </div>`;

        card.querySelector('.mute-btn').addEventListener('click', () => {
            const p = players.get(entry.key);
            const a = getAudio(p);
            if (!a) return;
            a.muted = !a.muted;
            updateCardLabels(card, a, p.paused);
        });
        card.querySelector('.pause-btn').addEventListener('click', () => {
            const p = players.get(entry.key);
            const a = getAudio(p);
            if (!a || !p) return;
            if (p.paused) {
                if (p.player?.play) p.player.play();
                else a.play().catch(() => {});
                p.paused = false;
                p.userPaused = false;
                p.ending = false;
                card.classList.add('playing');
                card.classList.remove('ending');
                card.querySelector('.card-status').textContent = 'En cours';
            } else {
                a.pause();
                p.paused = true;
                p.userPaused = true;
                card.classList.remove('playing');
                card.querySelector('.card-status').textContent = 'En pause';
            }
            updateCardLabels(card, a, p.paused);
        });
        card.querySelector('.stop-btn').addEventListener('click', () => {
            if (entry.source === 'airband') disabledAirband.add(String(entry.rawId));
            else disabledTetrapol.add(String(entry.rawId));
            saveDisabled();
            lastAirBtnSig = '';
            lastTpSig = '';
            stopStreamMonitoring(entry);
            updateAirbandButtons();
            updateTetrapolListing();
        });

        el.streamsContainer.appendChild(card);
        return card;
    }

    function getRingBufferKit() {
        return window.QueueRingBufferKit || null;
    }

    /** Config ring commune (Tetrapol + airband) : blankMarginSec = 0,7 s sous l'ancien titre. */
    function ringConfigForSegment(seg) {
        const kit = getRingBufferKit();
        const base = kit?.QUEUE_BUFFER_CONFIG || {};
        if (seg?.ringConfig) return seg.ringConfig;
        seg.ringConfig = base;
        return base;
    }

    /** Firefox (et Safari) : pas de SourceBuffer audio/mpeg → pas de file MSE. */
    function mseMpegSupported() {
        try {
            return typeof MediaSource !== 'undefined'
                && typeof MediaSource.isTypeSupported === 'function'
                && MediaSource.isTypeSupported('audio/mpeg');
        } catch (_) {
            return false;
        }
    }

    function usesQueueRingBuffer() {
        return exclusivePlaybackMode && !!getRingBufferKit() && mseMpegSupported();
    }

    function ensureRingUiTimer() {
        if (ringUiTimer || !exclusivePlaybackMode) return;
        ringUiTimer = setInterval(() => {
            const all = [...exclusiveQueue];
            if (exclusiveActive) all.push(exclusiveActive);
            if (!all.length) {
                clearInterval(ringUiTimer);
                ringUiTimer = null;
                return;
            }
            all.forEach((seg) => {
                if (typeof seg.ringBuffer?.pollBlankGrace === 'function') {
                    seg.ringBuffer.pollBlankGrace();
                }
                updateRingProgressBar(seg);
                updatePlaybackBlankState(seg);
                // Toujours rafraîchir le label (même pendant le hold 7 s) — sinon le
                // titre ICY arrivé après on_air↓ n'apparaît jamais sur la carte.
                syncRingBufferTitleDisplay(seg);
                if (blankDeadlines[seg.key]) updateEndingStatus(seg.key);
                maybeForcePromoteNearFullBuffer(seg);
                if (exclusiveActive === seg) applyBlankPlaybackFallback(seg);
                maybeFinishRingSegment(seg);
            });
            if (exclusiveActive) maybeResumeActiveRingPlayback(exclusiveActive);
            if (exclusiveActive) checkPriorityPreempt();
        }, RING_UI_INTERVAL_MS);
    }

    function getRingWaitingSeconds(seg) {
        if (!seg?.ringBuffer) return 0;
        const ahead = seg.bufferPlayer ? seg.bufferPlayer.bufferedAhead() : 0;
        return seg.ringBuffer.unreadSeconds + ahead;
    }

    function segmentHasRingContent(seg) {
        if (!seg?.ringBuffer) return false;
        // Playable audio only — a leftover ICY title must not keep the card alive.
        return (seg.ringBuffer.bytesInBuffer || 0) > 0
            || getRingWaitingSeconds(seg) > 0.05;
    }

    function updateRingProgressBar(seg) {
        if (!seg?.card || !seg.ringBuffer) return;
        const cfg = getRingBufferKit()?.QUEUE_BUFFER_CONFIG;
        const max = cfg?.bufferDurationSec || 120;
        const waiting = getRingWaitingSeconds(seg);
        const fillPct = Math.min(100, (waiting / max) * 100);
        let bar = seg.card.querySelector('.ring-progress');
        if (!bar) {
            bar = document.createElement('div');
            bar.className = 'ring-progress';
            bar.innerHTML = '<div class="ring-progress-fill"></div>';
            const info = seg.card.querySelector('.icecast-info') || seg.card;
            info.appendChild(bar);
        }
        const fill = bar.querySelector('.ring-progress-fill');
        if (fill) fill.style.width = fillPct + '%';
        const bufEl = seg.card.querySelector('.card-buffer');
        if (bufEl) {
            // Same basis as the gauge (waiting seconds), not fillRatio (stored window).
            bufEl.textContent = waiting >= 0.3
                ? `${waiting.toFixed(1)} s / ${max} s`
                : 'direct';
        }
        updateLiveButton(seg);
    }

    function getRingBufferDisplayTitle(seg) {
        const bp = seg?.bufferPlayer;
        const rb = seg?.ringBuffer;
        if (!bp || !rb) return (seg?.ringLastTitle || '').trim();
        const bytePos = typeof bp.playbackByteOffset === 'function'
            ? bp.playbackByteOffset()
            : rb.readByteOffset;
        // Tête dans le blanc → pas de label (ne pas coller lastTitle / ancien talker).
        if (typeof rb.isBlankAtByte === 'function' && rb.isBlankAtByte(bytePos)) {
            return '';
        }
        if (typeof rb.displayTitleAtByte === 'function') {
            const labeled = rb.displayTitleAtByte(bytePos);
            if (labeled) return labeled;
        }
        if (typeof rb.titleForByte === 'function') {
            const titleAtPos = rb.titleForByte(bytePos);
            if (titleAtPos !== null && titleAtPos !== undefined && String(titleAtPos).trim()) {
                return String(titleAtPos).trim();
            }
        }
        // Grâce fin de com : audio encore sous l'ancien StreamTitle.
        if (rb.blankGraceActive) return String(rb.lastTitle || '').trim();
        return '';
    }

    function syncRingBufferTitleDisplay(seg) {
        if (!seg?.card || !seg.ringBuffer) return;
        const entry = catalogEntryFor(seg);
        const display = getSegmentDisplayTitle(seg);
        // Lecture déjà sur un vrai titre : mettre à jour le label ; n'annuler le hold
        // que si ce n'est pas un titre collé pendant listing off (drain com courte).
        if (display && exclusiveActive === seg && seg.bufferPlayer?.hasStarted) {
            seg.ringLastTitle = display;
            const ending = !!(blankDeadlines[seg.key] || blankTimers[seg.key]
                || seg.playbackBlankSinceMs != null);
            if (ending) {
                const listingOff = String(seg.key || '').startsWith('tp:')
                    && !tetrapolKeyIsOnAir(seg.key);
                if (listingOff && isStickyIcyTitle(seg.key, display)) {
                    // garder le countdown, afficher le talker pendant le rattrapage
                } else {
                    lastNonEmptyStreamTitles[seg.key] = display;
                    clearBlankHoldForSpeech(seg);
                    maybeSkipBlankToNextTransmission(seg);
                }
            } else {
                lastNonEmptyStreamTitles[seg.key] = display;
            }
        }
        if (window.__MONITOR_DEBUG__) {
            const prev = seg._debugLastPlayTitle;
            const next = display || '(empty)';
            if (prev !== next) {
                seg._debugLastPlayTitle = next;
                console.log(
                    '%c[debug:play]%c StreamTitle at playback',
                    'background:#6a1b9a;color:#fff;padding:1px 6px;border-radius:3px',
                    'color:inherit',
                    {
                        t: new Date().toISOString().slice(11, 23),
                        key: seg.key,
                        groupLabel: labelOf(entry),
                        StreamTitle: next,
                        bytePos: Math.round(getPlaybackBytePos(seg)),
                        unreadSec: Number((getRingWaitingSeconds(seg) || 0).toFixed(2)),
                        playbackInBlank: isPlaybackInBlank(seg),
                        active: exclusiveActive === seg
                    }
                );
            }
        }
        updateCardHeader(seg.card, entry, {
            txTitle: display,
            role: queueRoleLabel(seg, exclusiveActive === seg),
            groupLabel: segmentDisplayName(seg)
        });
        const status = seg.card.querySelector('.card-status');
        if (!status) return;
        if (blankDeadlines[seg.key]) {
            updateEndingStatus(seg.key);
            return;
        }
        const isActive = exclusiveActive === seg;
        if (isActive) {
            const inBlank = isPlaybackInBlank(seg);
            const waiting = getRingWaitingSeconds(seg);
            if (seg.ringPlaybackPending) {
                status.textContent = 'Tamponnage…';
            } else if (onAirTitleWaitTimers.has(seg.key) && !(getSegmentDisplayTitle(seg) || '').trim()) {
                status.textContent = 'Attente titre…';
            } else if (inBlank && waiting <= 0.5) {
                status.textContent = 'Fin de com…';
            } else if (waiting > getLiveWaitingThreshold()) {
                status.textContent = `Rattrapage (${waiting.toFixed(1)}s)`;
            } else {
                status.textContent = 'En cours';
            }
        } else if (seg.suspendedByUser) {
            status.textContent = 'En pause';
        } else if (segmentHasRingContent(seg)) {
            status.textContent = `En attente (${getRingWaitingSeconds(seg).toFixed(1)}s)`;
        } else {
            status.textContent = 'En attente';
        }
    }

    function initExclusiveRingBuffer(seg) {
        const kit = getRingBufferKit();
        if (!kit || seg.ringBuffer) return;
        const cfg = ringConfigForSegment(seg);
        seg.ringBuffer = new kit.AudioRingBuffer(cfg.bufferDurationSec, cfg.fallbackBitrate, cfg);
        // Analogique : ne pas remplir le tampon tant qu'il n'y a pas de StreamTitle (blanc initial).
        if (seg.entry?.source === 'airband' && typeof seg.ringBuffer.beginAwaitingStreamTitle === 'function') {
            seg.ringBuffer.beginAwaitingStreamTitle();
        }
        seg.ringBlankSinceMs = null;
        seg.playbackBlankSinceMs = null;
        seg.ringLastTitle = '';
        seg.ringLastStoredBytes = 0;
        seg.ringLastGrowthMs = null;
        seg.ringPlaybackPending = false;
        seg.bufferForcePromoted = false;
    }

    function ensureSegmentQueueAudio(seg) {
        if (seg.queueAudioEl) return seg.queueAudioEl;
        const audio = document.createElement('audio');
        audio.className = 'queue-ring-audio';
        audio.style.display = 'none';
        audio.setAttribute('playsinline', '');
        audio.setAttribute('webkit-playsinline', '');
        audio.preload = 'auto';
        try { audio.setAttribute('autoplay', ''); } catch (_) {}
        document.body.appendChild(audio);
        seg.queueAudioEl = audio;
        return audio;
    }

    function ensureSegmentBufferPlayer(seg) {
        const kit = getRingBufferKit();
        if (!seg.ringBuffer || seg.bufferPlayer) return seg.bufferPlayer;
        const audio = ensureSegmentQueueAudio(seg);
        seg.bufferPlayer = new kit.BufferPlayer(audio, seg.ringBuffer, ringConfigForSegment(seg));
        applyAudioOutput(audio);
        return seg.bufferPlayer;
    }

    function buildBufferPlayerWrapper(seg) {
        const bp = seg.bufferPlayer;
        return {
            audioElement: seg.queueAudioEl,
            play: (opts) => bp?.play(opts),
            pause: () => bp?.pause(),
            stop: () => {
                bp?.pause();
                bp?.reset();
            }
        };
    }

    function startQueueRingIngestion(seg) {
        const kit = getRingBufferKit();
        if (!kit) return;
        initExclusiveRingBuffer(seg);
        if (seg.ringIngestor) return;

        const isActive = exclusiveActive === seg && !seg.suspendedByUser;
        if (!isActive) seg.ringBuffer.setPlaybackPaused(true);

        console.info('queue', 'ingestion ring', seg.key);
        seg.ringIngestor = new kit.StreamIngestor(
            seg.ringBuffer,
            seg.url,
            ringConfigForSegment(seg),
            {
                onConnected: (meta) => {
                    markIngestUp(seg);
                    if (meta.icyBr) seg.ringBuffer.setBitrate(meta.icyBr);
                    debugStreamTitleLog('ingest-connected', {
                        key: seg.key,
                        groupLabel: labelOf(catalogEntryFor(seg)),
                        icyMetaInt: meta.icyMetaInt,
                        icyBr: meta.icyBr,
                        path: 'ring-ingest'
                    });
                },
                onStream: () => onRingBufferStream(seg),
                onMetadata: (value, changed) => onRingBufferMetadata(seg, value, changed),
                onError: (err) => {
                    if (err?.name === 'AbortError') return;
                    markIngestDown(seg, err);
                    logStreamError(seg.key, err);
                }
            }
        );
        seg.ringLastStoredBytes = seg.ringBuffer.totalBytesWritten || 0;
        seg.ringLastGrowthMs = performance.now();
        seg.ringIngestor.start();
        ensureRingUiTimer();
    }

    function onRingBufferMetadata(seg, value, changed) {
        const meta = value?.metadata || value || {};
        const rawTitle = meta.StreamTitle ?? meta.streamTitle ?? meta.title ?? meta.Song ?? '';
        const title = String(rawTitle ?? '').trim();
        const rb = seg.ringBuffer;
        const entry = catalogEntryFor(seg);
        // État ring après addTitleChange (sticky / live-speech-off) = source de vérité.
        const ringTitle = String(rb?.lastTitle ?? '').trim();
        const ringBlank = !!(rb && !rb.awaitingFirstTitle
            && (rb.blankGraceActive || rb.inBlank || !ringTitle));
        debugStreamTitleLog(ringBlank ? 'ingest-blank' : 'ingest-title', {
            key: seg.key,
            source: entry?.source || seg.entry?.source || '?',
            groupLabel: labelOf(entry || seg.entry),
            StreamTitle: ringTitle || '(empty)',
            raw: rawTitle == null ? null : String(rawTitle),
            metaKeys: meta && typeof meta === 'object' ? Object.keys(meta) : [],
            changed: !!changed,
            ringInBlank: !!rb?.inBlank,
            blankGraceActive: !!rb?.blankGraceActive,
            awaitingFirstTitle: !!rb?.awaitingFirstTitle,
            lastTitle: rb?.lastTitle == null ? null : String(rb.lastTitle),
            bytesWritten: rb?.totalBytesWritten || 0,
            unreadSec: Number(((rb?.unreadSeconds || 0).toFixed?.(2)) ?? (rb?.unreadSeconds || 0)),
            playbackInBlank: !!isPlaybackInBlank(seg),
            ending: !!(blankDeadlines[seg.key] || seg.playbackBlankSinceMs != null),
            path: 'ring-ingest'
        });
        const acceptedSpeech = !!(ringTitle
            && rb
            && !rb.inBlank
            && !rb.blankGraceActive
            && ringTitle === title);
        if (acceptedSpeech) {
            // Titre collé pendant hold + listing off : ne pas annuler le countdown.
            // Nouveau talker (y compris com courte : titre après on_air↓) : accepter.
            const listingOff = String(seg.key || '').startsWith('tp:') && !tetrapolKeyIsOnAir(seg.key);
            const ending = !!(blankDeadlines[seg.key] || blankTimers[seg.key]
                || seg.playbackBlankSinceMs != null);
            if (listingOff && ending && isStickyIcyTitle(seg.key, title)) {
                debugStreamTitleLog('sticky-ignored', {
                    key: seg.key,
                    StreamTitle: title,
                    lastNonEmpty: lastNonEmptyStreamTitles[seg.key] || '',
                    path: 'ring-ingest-listing-off'
                });
            } else {
                cancelOnAirTitleWait(seg.key);
                lastNonEmptyStreamTitles[seg.key] = title;
                seg.ringLastTitle = title;
                seg.analyzerConfirmedEnded = false;
                if (String(seg.key || '').startsWith('tp:')) {
                    rememberGroupStreamNetwork(String(seg.key).slice(3), title);
                }
                clearBlankHoldForSpeech(seg);
                if (exclusiveActive === seg) {
                    maybeSkipBlankToNextTransmission(seg);
                    maybeResumeActiveRingPlayback(seg);
                }
            }
        }
        if (ringBlank && exclusiveActive === seg) {
            seg.ringLastTitle = '';
            if (catalogKeyIsOnAir(seg.key)) {
                // Listing encore actif : ne pas lancer le hold 7 s (prochaine com possible).
                if (blankDeadlines[seg.key] || blankTimers[seg.key]
                    || seg.playbackBlankSinceMs != null) {
                    clearBlankHoldForSpeech(seg);
                }
            } else {
                armBlankCountdown(seg.key);
            }
        }
        // Les métadonnées live alimentent le ring buffer ; l'affichage suit la lecture.
        syncRingBufferTitleDisplay(seg);
        updateRingProgressBar(seg);
        if (exclusiveQueue.includes(seg)) maybeForcePromoteNearFullBuffer(seg);
        if (exclusiveActive === seg) maybeFinishRingSegment(seg);
    }

    function onRingBufferStream(seg) {
        const rb = seg.ringBuffer;
        if (rb) {
            const stored = rb.totalBytesWritten;
            if (stored > (seg.ringLastStoredBytes || 0)) {
                seg.ringLastStoredBytes = stored;
                seg.ringLastGrowthMs = performance.now();
            }
        }
        updateRingProgressBar(seg);
        if (exclusiveQueue.includes(seg)) maybeForcePromoteNearFullBuffer(seg);
        if (exclusiveActive === seg && !seg.suspendedByUser) {
            maybeResumeActiveRingPlayback(seg);
            checkPriorityPreempt();
        }
    }

    function maybeForcePromoteNearFullBuffer(seg) {
        if (!seg?.ringBuffer) return;
        if (!exclusiveQueue.includes(seg) || seg.suspendedByUser) return;
        const rb = seg.ringBuffer;
        const fill = rb.fillRatio;
        const saturated = fill >= 1
            || (rb.maxBytes > 0 && rb.unreadBytes >= rb.maxBytes);
        // Option ON : bascule dès 92 %. Option OFF : seulement à saturation réelle.
        const shouldPromote = forceBeforeFull
            ? fill >= RING_FORCE_PROMOTE_RATIO
            : saturated;
        if (!shouldPromote) return;

        if (!exclusiveActive) {
            exclusiveQueue = exclusiveQueue.filter(s => s.key !== seg.key);
            seg.bufferForcePromoted = true;
            exclusiveActive = seg;
            startExclusivePlay(seg);
            refreshExclusiveCards();
            return;
        }
        if (exclusiveActive === seg) return;
        const active = exclusiveActive;
        // Une ★ persistante ne cède à la saturation que si elle peut rendre (blanc + <80 %).
        if (isPermanentPrioritySeg(active)) {
            const unread = getActiveRingUnreadRatio();
            if (unread >= RING_YIELD_MAX_RATIO || !isPlaybackInBlank(active)) return;
        }
        if (!active.bufferPlayer?.hasStarted) return;
        const unreadRatio = active.ringBuffer?.maxBytes
            ? Math.min(1, active.ringBuffer.unreadBytes / active.ringBuffer.maxBytes)
            : 1;
        if (unreadRatio >= RING_YIELD_MAX_RATIO) return;
        if (!active.manualPriority) {
            const inBlank = isPlaybackInBlank(active);
            if (!inBlank && active.playbackBlankSinceMs == null) return;
        }

        active.suspendedByUser = false;
        active.manualPriority = false;
        clearBlankHoldForSpeech(active);
        exclusiveQueue.unshift(active);
        stopExclusivePlay(active, { keepBuffer: true });
        exclusiveQueue = exclusiveQueue.filter(s => s.key !== seg.key);
        exclusiveActive = seg;
        seg.bufferForcePromoted = true;
        startExclusivePlay(seg);
        refreshExclusiveCards();
        // If we displaced a ★, resume it as soon as this catch-up can yield.
        checkPriorityPreempt();
    }

    function maybeResumeActiveRingPlayback(seg) {
        if (!seg || exclusiveActive !== seg || seg.suspendedByUser || seg.ringPlaybackPending) return;
        const bp = seg.bufferPlayer;
        if (!bp) return;
        if (!bp.hasStarted) {
            if ((seg.ringBuffer?.bytesInBuffer || 0) >= 256) {
                activateSegmentRingPlayback(seg, { playFromStart: true });
            }
            return;
        }
        if (!bp.isPlaying && (bp.canPlay() || (seg.ringBuffer?.unreadBytes || 0) >= 256)) {
            bp.play().catch(() => {});
        }
    }

    async function activateSegmentRingPlayback(seg, options = {}) {
        if (!seg.ringBuffer) startQueueRingIngestion(seg);
        ensureSegmentBufferPlayer(seg);
        const bp = seg.bufferPlayer;
        if (!bp) return false;

        seg.ringPlaybackPending = true;
        seg.playbackPlayer = buildBufferPlayerWrapper(seg);
        applyAudioOutput(seg.queueAudioEl);

        const playGen = (seg._ringPlayGen = (seg._ringPlayGen || 0) + 1);
        const stillActive = () => seg._ringPlayGen === playGen
            && exclusiveActive === seg
            && !seg.suspendedByUser;

        if (options.playFromStart && seg.ringBuffer) {
            seg.ringBuffer.readByteOffset = seg.ringBuffer.oldestByteOffset;
        }

        const deadline = Date.now() + 30000;
        while ((seg.ringBuffer?.bytesInBuffer || 0) < 256 && Date.now() < deadline) {
            if (!stillActive()) {
                seg.ringPlaybackPending = false;
                return false;
            }
            await new Promise((r) => setTimeout(r, 100));
        }

        if (!stillActive()) {
            seg.ringPlaybackPending = false;
            return false;
        }
        if ((seg.ringBuffer?.bytesInBuffer || 0) < 256) {
            seg.ringPlaybackPending = false;
            return false;
        }

        try {
            await bp.play({ fromStart: options.playFromStart === true });
            seg.ringPlaybackPending = false;
            updateRingProgressBar(seg);
            refreshExclusiveCards();
            return true;
        } catch (err) {
            seg.ringPlaybackPending = false;
            logStreamError(seg.key, err);
            return false;
        }
    }

    function stopQueueRingPipeline(seg, options = {}) {
        if (!seg) return;
        if (seg.ringIngestor) {
            try { seg.ringIngestor.stop(); } catch (_) {}
            seg.ringIngestor = null;
        }
        if (!options.keepBuffer) {
            if (seg.bufferPlayer) {
                try { seg.bufferPlayer.reset(); } catch (_) {}
                seg.bufferPlayer = null;
            }
            if (seg.ringBuffer) {
                try { seg.ringBuffer.reset(); } catch (_) {}
                seg.ringBuffer = null;
            }
        } else if (seg.bufferPlayer) {
            try { seg.bufferPlayer.pause(); } catch (_) {}
            if (seg.ringBuffer) seg.ringBuffer.setPlaybackPaused(true);
        }
        if (!options.keepAudio && seg.queueAudioEl) {
            try {
                seg.queueAudioEl.pause();
                seg.queueAudioEl.removeAttribute('src');
                seg.queueAudioEl.load();
            } catch (_) {}
            if (!options.keepBuffer) {
                seg.queueAudioEl.remove();
                seg.queueAudioEl = null;
            }
        }
        seg.playbackPlayer = null;
        seg.ringPlaybackPending = false;
    }

    function liveHasRealSpeech(seg) {
        const rb = seg?.ringBuffer;
        if (!rb) return false;
        if (getTitleAheadOfPlayback(seg)) return true;
        const shown = (getRingBufferDisplayTitle(seg) || '').trim();
        if (shown && !isPlaybackInBlank(seg)) return true;
        const entry = catalogEntryFor(seg);
        const isTetrapol = (entry?.source === 'tetrapol')
            || String(seg.key || '').startsWith('tp:');
        if (isTetrapol && ringHasLiveTalk(seg)) return true;
        if (isTetrapol && !ringHasLiveTalk(seg)) return false;
        // Grâce 0,7 s : lastTitle encore le talker → parole.
        if (!rb.inBlank && String(rb.lastTitle || '').trim()) return true;
        return false;
    }

    function ringReportsBlank(seg) {
        const rb = seg?.ringBuffer;
        if (!rb) return false;
        if (rb.inBlank) return true;
        if (rb.blankGraceActive && !String(rb.lastTitle || '').trim()) return true;
        if (ringHasLiveTalk(seg)) return false;
        if (!rb.blankGraceActive && !String(rb.lastTitle || '').trim()) return true;
        const pos = getPlaybackBytePos(seg);
        if (typeof rb.isBlankAtByte === 'function' && rb.isBlankAtByte(pos)) return true;
        return false;
    }

    /**
     * Filet UI : si le live a encore de la parole / pas de blanc → ne pas rester
     * en lecture de silence. Si blanc réel et plus d'activité → fermer après 7 s
     * même si MSE continue à « jouer » du blanc.
     */
    function applyBlankPlaybackFallback(seg) {
        if (!seg || exclusiveActive !== seg || !seg.bufferPlayer?.hasStarted) return false;
        // Listing encore on_air : blanc ICY = trou, pas fin de carte.
        if (catalogKeyIsOnAir(seg.key)) {
            if (blankDeadlines[seg.key] || blankTimers[seg.key]
                || seg.playbackBlankSinceMs != null
                || (seg.card && seg.card.classList.contains('ending'))) {
                clearBlankHoldForSpeech(seg);
            }
            seg._uiBlankSinceMs = null;
            return false;
        }
        const speech = playbackHasSpeech(seg);
        const hasBlank = ringReportsBlank(seg);
        const playingBlank = isPlaybackInBlank(seg)
            || !!blankDeadlines[seg.key]
            || seg.playbackBlankSinceMs != null
            || !!(seg.card && seg.card.classList.contains('ending'));

        if (speech) {
            if (playingBlank) {
                if (isPlaybackInBlank(seg)) maybeSkipBlankToNextTransmission(seg);
                else clearBlankHoldForSpeech(seg);
                return true;
            }
            return false;
        }
        // Rattrapage en cours : ne pas lancer / avancer le hold 7 s.
        const waiting = getRingWaitingSeconds(seg);
        if (waiting > 0.5 && !isPlaybackInBlank(seg)) {
            seg._uiBlankSinceMs = null;
            return false;
        }
        if (!hasBlank) {
            // Grâce / flicker ICY : ne pas annuler un décompte déjà lancé.
            if (!playingBlank) seg._uiBlankSinceMs = null;
            return false;
        }

        const inBlankPlay = isPlaybackInBlank(seg)
            || (waiting <= 0.5 && hasBlank && liveEdgeIsBlank(seg));
        if (!inBlankPlay) {
            seg._uiBlankSinceMs = null;
            return false;
        }
        if (seg._uiBlankSinceMs == null) seg._uiBlankSinceMs = performance.now();
        const uiAge = Math.max(
            getBlankAgeMs(seg.key, seg),
            performance.now() - seg._uiBlankSinceMs
        );
        if (uiAge >= STREAM_END_HOLD_MS) {
            if (onAirTitleWaitTimers.has(seg.key)) return false;
            if (playbackHasSpeech(seg) || getTitleAheadOfPlayback(seg) || ringHasLiveTalk(seg)) {
                maybeSkipBlankToNextTransmission(seg);
                return true;
            }
            if (ingestIsReconnecting(seg)) return false;
            stopEntryAfterBlank(seg.key);
            return true;
        }
        if (uiAge >= 200 && !blankTimers[seg.key]) armBlankCountdown(seg.key);
        return false;
    }

    function maybeFinishRingSegment(seg) {
        if (!seg || !usesQueueRingBuffer()) return;
        const isActive = exclusiveActive === seg;
        const inQueue = exclusiveQueue.includes(seg);
        if (!isActive && !inQueue) return;

        const ageMs = performance.now() - (seg.createdAtMs || 0);
        const rb = seg.ringBuffer;
        // Attente 1er titre expirée, jamais de parole tamponnée → carte inactive.
        // Ne pas fermer tant que le listing est encore on_air (airband inclus :
        // tetrapolKeyIsOnAir ne couvre que tp:*, et le titre ICY peut arriver tard).
        if (!seg.bufferPlayer?.hasStarted) {
            const waitMax = rb?.config?.firstTitleWaitMaxMs || 3500;
            if (ageMs >= waitMax + 500
                && rb
                && !rb.awaitingFirstTitle
                && liveEdgeIsBlank(seg)
                && !segmentHasRingContent(seg)
                && !String(rb.lastTitle || '').trim()) {
                const stillOnAir = catalogKeyIsOnAir(seg.key);
                if (!ingestIsReconnecting(seg) && !stillOnAir) stopEntry(seg.key);
            }
            return;
        }
        if (ageMs < 2000) return;

        if (isActive && applyBlankPlaybackFallback(seg)) return;

        // Vraie reprise : titre devant la tête, ou déjà en lecture.
        if (isActive && (getTitleAheadOfPlayback(seg) || playbackHasSpeech(seg))) {
            maybeSkipBlankToNextTransmission(seg);
            return;
        }

        const waiting = getRingWaitingSeconds(seg);
        const inPlaybackBlank = isActive ? updatePlaybackBlankState(seg) : false;
        const titleAhead = getTitleAheadOfPlayback(seg);
        const liveBlank = liveEdgeIsBlank(seg);

        if (seg.pendingFinishAfterDrain) {
            if (titleAhead || playbackHasSpeech(seg)) {
                maybeSkipBlankToNextTransmission(seg);
                return;
            }
            // Pas actif (préempté / en file) : attendre la promotion pour vider le tampon.
            if (!isActive && segmentHasRingContent(seg)) {
                return;
            }
            if (waiting <= 0.3 && !titleAhead && liveBlank) {
                if (!ingestIsReconnecting(seg)) stopEntryAfterBlank(seg.key);
            }
            return;
        }

        // Encore du rattrapage : ne pas fermer, même si le live est déjà blanc.
        if (waiting > 0.5 && !inPlaybackBlank) {
            seg._nearLiveBlankSinceMs = null;
            return;
        }

        if (!isActive) return;

        if (titleAhead) {
            maybeSkipBlankToNextTransmission(seg);
            return;
        }

        // Blanc à la lecture, ou rattrapé le direct alors que le live est blanc.
        const nearLiveBlank = waiting <= 0.5 && liveBlank;
        if (!inPlaybackBlank && !nearLiveBlank) {
            seg._nearLiveBlankSinceMs = null;
            return;
        }

        if (inPlaybackBlank) {
            seg._nearLiveBlankSinceMs = null;
        } else if (seg._nearLiveBlankSinceMs == null) {
            seg._nearLiveBlankSinceMs = performance.now();
        }

        const effectiveBlankMs = getBlankAgeMs(seg.key, seg);

        if (effectiveBlankMs >= STREAM_END_HOLD_MS) {
            if (onAirTitleWaitTimers.has(seg.key)) return;
            if (titleAhead || playbackHasSpeech(seg) || ringHasLiveTalk(seg)) {
                maybeSkipBlankToNextTransmission(seg);
            } else if (!ingestIsReconnecting(seg)) {
                stopEntryAfterBlank(seg.key);
            }
        } else if (effectiveBlankMs >= 200 && !blankTimers[seg.key]) {
            armBlankCountdown(seg.key);
        }
    }

    function createExclusiveSegment(entry) {
        if (exclusiveQueue.some(s => s.key === entry.key) || (exclusiveActive && exclusiveActive.key === entry.key))
            return;
        const seg = {
            key: entry.key,
            entry,
            url: entry.streamUrl,
            suspendedByUser: false,
            card: null,
            playbackPlayer: null,
            ringBuffer: null,
            ringIngestor: null,
            bufferPlayer: null,
            queueAudioEl: null,
            ringLastTitle: '',
            ringBlankSinceMs: null,
            playbackBlankSinceMs: null,
            createdAtMs: performance.now(),
            analyzerConfirmedEnded: false,
            /** Prio temporaire : carte lancée via « Écouter » (cède à une vraie prio / saturation). */
            manualPriority: false,
            // Nom + département figés à la création (canal d'activation), comme tetrapol-kit.
            displayName: entry.source === 'tetrapol'
                ? getGroupDisplayNameWithDepartment(entry.rawId)
                : ''
        };
        seg.card = createExclusiveCard(seg);
        // ★ waiters ahead of normal (same rule as Pulse / Tetrapol enqueue).
        if (isStreamPrioritySeg(seg)) {
            const idx = exclusiveQueue.findIndex((s) => !isStreamPrioritySeg(s));
            if (idx === -1) exclusiveQueue.push(seg);
            else exclusiveQueue.splice(idx, 0, seg);
        } else {
            exclusiveQueue.push(seg);
        }
        lastStreamTitles[entry.key] = '';
        if (usesQueueRingBuffer()) {
            startQueueRingIngestion(seg);
        } else if (exclusivePlaybackMode && mseMpegSupported() && !getRingBufferKit()) {
            // Module still loading — retry once ready (Chrome/Chromium only).
            window.addEventListener('queue-ring-buffer-ready', () => {
                if (usesQueueRingBuffer()
                    && (exclusiveQueue.includes(seg) || exclusiveActive === seg)) {
                    startQueueRingIngestion(seg);
                }
            }, { once: true });
        }
        // Nouvelle carte déjà on_air : fenêtre 5 s pour le premier StreamTitle.
        if ((entry.source === 'tetrapol' || String(entry.key).startsWith('tp:')) && entry.on_air) {
            armOnAirTitleWait(entry.key);
        }
        // Prio : activer dès le début de com (sauf autre prio actif / tampon force ≥ 80 %).
        if (!exclusiveActive) {
            promoteNext();
        } else if (isStreamPrioritySeg(seg)) {
            if (!preemptActiveWithPriority(seg)) refreshExclusiveCards();
        } else {
            refreshExclusiveCards();
        }
        ensureRingUiTimer();
    }

    function createExclusiveCard(seg) {
        const placeholder = el.streamsContainer.querySelector('.no-data');
        if (placeholder) placeholder.remove();
        const card = document.createElement('div');
        card.className = 'stream-card queued';
        card.dataset.key = seg.key;
        // File : uniquement « Écouter » (comme tetrapol-kit). Actif : Mute/Pause/Direct/Stop.
        card.innerHTML = `
            ${cardHeaderHtml(catalogEntryFor(seg), '', segmentDisplayName(seg))}
            <div class="stream-controls stream-card-zone stream-card-zone-controls queue-card-controls">
                <button type="button" class="queue-promote-btn" title="Écouter maintenant">▶️ Écouter</button>
                <button type="button" class="mute-btn" style="display:none">🔇 Mute</button>
                <button type="button" class="pause-btn" style="display:none">⏸️ Pause</button>
                <button type="button" class="live-btn" data-live-enabled="0" disabled style="display:none">⏭️ Direct</button>
                <button type="button" class="stop-btn" style="display:none">⏹️ Stop</button>
            </div>
            <div class="icecast-info stream-card-zone stream-card-zone-status">
                <div><strong>📊 Status:</strong> <span class="card-status">En attente</span></div>
                <div><strong>💾 Tampon:</strong> <span class="card-buffer">—</span></div>
                <div class="ring-progress"><div class="ring-progress-fill"></div></div>
            </div>`;
        updateCardHeader(card, catalogEntryFor(seg), {
            role: 'file',
            groupLabel: segmentDisplayName(seg)
        });
        card.querySelector('.queue-promote-btn').addEventListener('click', (e) => {
            e.stopPropagation();
            promote(seg.key);
        });
        card.querySelector('.pause-btn').addEventListener('click', (e) => {
            e.stopPropagation();
            if (exclusiveActive && exclusiveActive.key === seg.key) suspend(seg.key);
        });
        card.querySelector('.live-btn').addEventListener('click', (e) => {
            e.stopPropagation();
            skipToLive(seg);
        });
        card.querySelector('.mute-btn').addEventListener('click', (e) => {
            e.stopPropagation();
            const audio = seg.queueAudioEl || seg.playbackPlayer?.audioElement;
            if (!audio) return;
            audio.muted = !audio.muted;
            card.querySelector('.mute-btn').textContent = audio.muted ? '🔊 Unmute' : '🔇 Mute';
        });
        card.querySelector('.stop-btn').addEventListener('click', (e) => {
            e.stopPropagation();
            if (seg.entry.source === 'airband') disabledAirband.add(String(seg.entry.rawId));
            else disabledTetrapol.add(String(seg.entry.rawId));
            saveDisabled();
            lastAirBtnSig = '';
            lastTpSig = '';
            stopStreamMonitoring(seg.entry);
            updateAirbandButtons();
            updateTetrapolListing();
        });
        el.streamsContainer.appendChild(card);
        return card;
    }

    function getLiveWaitingThreshold() {
        const cfg = getRingBufferKit()?.QUEUE_BUFFER_CONFIG;
        return cfg?.liveWaitingThresholdSec ?? 2.5;
    }

    function updateLiveButton(seg) {
        if (!seg?.card) return;
        const liveBtn = seg.card.querySelector('.live-btn');
        if (!liveBtn) return;
        const isActive = exclusiveActive === seg && !seg.suspendedByUser;
        let enabled = liveBtn.dataset.liveEnabled === '1';
        if (isActive && usesQueueRingBuffer() && seg.bufferPlayer?.hasStarted) {
            const waiting = typeof seg.bufferPlayer.waitingSeconds === 'function'
                ? seg.bufferPlayer.waitingSeconds()
                : getRingWaitingSeconds(seg);
            const liveOn = getLiveWaitingThreshold() + 0.35;
            const liveOff = Math.max(0.2, getLiveWaitingThreshold() - 0.35);
            if (!enabled && waiting > liveOn) enabled = true;
            else if (enabled && waiting <= liveOff) enabled = false;
        } else {
            enabled = false;
        }
        liveBtn.dataset.liveEnabled = enabled ? '1' : '0';
        liveBtn.disabled = !enabled;
        liveBtn.style.display = isActive ? '' : 'none';
    }

    function skipToLive(seg) {
        if (!seg || exclusiveActive !== seg) return;
        if (!seg.bufferPlayer || typeof seg.bufferPlayer.jumpToLive !== 'function') return;
        const liveBtn = seg.card?.querySelector('.live-btn');
        if (liveBtn) liveBtn.dataset.liveEnabled = '0';
        // Jump discards catch-up backlog and resumes near the live edge.
        cancelBlankCountdown(seg.key);
        seg.pendingFinishAfterDrain = false;
        seg.bufferPlayer.jumpToLive()
            .then(() => {
                updateRingProgressBar(seg);
                updateLiveButton(seg);
                if (seg.playbackBlankSinceMs != null && getRingWaitingSeconds(seg) <= 0.5) {
                    armBlankCountdown(seg.key);
                }
            })
            .catch((err) => logStreamError(seg.key, err));
    }

    function cleanupExclusive(seg) {
        cancelBlankCountdown(seg.key);
        cancelOnAirTitleWait(seg.key);
        stopExclusivePlay(seg, { keepBuffer: false });
        stopQueueRingPipeline(seg);
        if (seg.card?.parentNode) seg.card.parentNode.removeChild(seg.card);
    }

    function promote(key) {
        const seg = exclusiveQueue.find(s => s.key === key) || (exclusiveActive?.key === key ? exclusiveActive : null);
        if (!seg) return;
        exclusiveQueue.forEach((s) => { s.manualPriority = false; });
        if (exclusiveActive && exclusiveActive.key === key) {
            seg.manualPriority = true;
            seg.suspendedByUser = false;
            refreshExclusiveCards();
            return;
        }
        if (exclusiveActive) {
            exclusiveActive.manualPriority = false;
            clearBlankHoldForSpeech(exclusiveActive);
            exclusiveQueue.unshift(exclusiveActive);
            stopExclusivePlay(exclusiveActive, { keepBuffer: true });
        }
        exclusiveQueue = exclusiveQueue.filter(s => s.key !== key);
        exclusiveActive = seg;
        seg.suspendedByUser = false;
        seg.manualPriority = true;
        startExclusivePlay(seg);
        refreshExclusiveCards();
    }

    function suspend(key) {
        if (!exclusiveActive || exclusiveActive.key !== key) return;
        exclusiveActive.suspendedByUser = true;
        exclusiveActive.manualPriority = false;
        stopExclusivePlay(exclusiveActive, { keepBuffer: true });
        exclusiveQueue.push(exclusiveActive);
        exclusiveActive = null;
        promoteNext();
        refreshExclusiveCards();
    }

    function isPermanentPrioritySeg(seg) {
        if (!seg) return false;
        return isStreamPriority(seg.key) || !!(seg.entry && seg.entry.priority);
    }

    function isStreamPrioritySeg(seg) {
        if (!seg) return false;
        // priorityKeys is authoritative; entry.priority can lag after catalog rebuilds.
        return isPermanentPrioritySeg(seg) || !!seg.manualPriority;
    }

    function getActiveRingUnreadRatio() {
        const active = exclusiveActive;
        const rb = active?.ringBuffer;
        if (!rb || !rb.maxBytes) return 0;
        return Math.min(1, rb.unreadBytes / rb.maxBytes);
    }

    /**
     * Un stream prio peut prendre la main sauf si :
     * - l'actif est déjà un groupe prio (étoile persistante), ou
     * - l'actif est un choix manuel : seule une vraie prio (nouvelle com ★) peut couper, ou
     * - « Bascule avant fin tampon » et l'actif force-promu a encore ≥ 80 % non lus
     *   ET n'est pas en blanc (fin de com) — un blanc autorise la prio même ≥ 80 %.
     */
    function canPreemptActiveForPriority(incoming) {
        const active = exclusiveActive;
        if (!active) return true;
        if (isPermanentPrioritySeg(active)) return false;
        if (active.manualPriority) {
            return !!(incoming && isPermanentPrioritySeg(incoming));
        }
        // Sticky force-promote flag must clear once caught up, or prio stays blocked forever
        // while a live active keeps refill ≥ 80 %.
        const unread = getActiveRingUnreadRatio();
        if (active.bufferForcePromoted && unread < RING_YIELD_MAX_RATIO) {
            active.bufferForcePromoted = false;
        }
        if (forceBeforeFull
            && active.bufferForcePromoted
            && unread >= RING_YIELD_MAX_RATIO
            && !isPlaybackInBlank(active)) {
            return false;
        }
        return true;
    }

    /** Passe un segment prio en actif (préemption) si les règles le permettent. */
    function preemptActiveWithPriority(seg) {
        if (!exclusivePlaybackMode || !seg || seg.suspendedByUser) return false;
        // Refresh flag from authoritative set before deciding.
        if (seg.entry) seg.entry.priority = isStreamPriority(seg.key);
        if (!isPermanentPrioritySeg(seg)) return false;
        if (exclusiveActive === seg) return false;
        if (!canPreemptActiveForPriority(seg)) return false;

        if (exclusiveActive) {
            const displaced = exclusiveActive;
            displaced.manualPriority = false;
            // Annuler le hold 7 s : la carte reprendra après la prio avec son tampon.
            clearBlankHoldForSpeech(displaced);
            exclusiveQueue = exclusiveQueue.filter((s) => s.key !== displaced.key);
            exclusiveQueue.unshift(displaced);
            stopExclusivePlay(displaced, { keepBuffer: true });
        }
        exclusiveQueue = exclusiveQueue.filter((s) => s.key !== seg.key);
        exclusiveActive = seg;
        seg.suspendedByUser = false;
        startExclusivePlay(seg);
        refreshExclusiveCards();
        return true;
    }

    /** Réévalue une prio en file quand le tampon de l'actif redescend sous 80 %. */
    function checkPriorityPreempt() {
        if (!exclusivePlaybackMode || !exclusiveActive) return;
        if (isPermanentPrioritySeg(exclusiveActive)) return;
        // Choix manuel : la prio déjà en file ne reprend pas tout de suite.
        if (exclusiveActive.manualPriority) return;
        const waiting = exclusiveQueue.find(
            (s) => {
                if (s.entry) s.entry.priority = isStreamPriority(s.key);
                return isPermanentPrioritySeg(s) && !s.suspendedByUser;
            }
        );
        if (!waiting) return;
        preemptActiveWithPriority(waiting);
    }

    function promoteNext() {
        const prioIdx = exclusiveQueue.findIndex((s) => {
            if (s.entry) s.entry.priority = isStreamPriority(s.key);
            return isPermanentPrioritySeg(s) && !s.suspendedByUser;
        });
        if (prioIdx >= 0) {
            const [seg] = exclusiveQueue.splice(prioIdx, 1);
            exclusiveActive = seg;
            seg.manualPriority = false;
            startExclusivePlay(seg);
            refreshExclusiveCards();
            return;
        }
        while (exclusiveQueue.length) {
            const next = exclusiveQueue.shift();
            if (next.suspendedByUser) {
                exclusiveQueue.push(next);
                if (exclusiveQueue.every(s => s.suspendedByUser)) break;
                continue;
            }
            exclusiveActive = next;
            next.manualPriority = false;
            startExclusivePlay(next);
            refreshExclusiveCards();
            return;
        }
        exclusiveActive = null;
        refreshExclusiveCards();
    }

    function startExclusivePlayHtml(seg) {
        stopQueueRingPipeline(seg);
        if (seg._htmlPlayer) {
            try { seg._htmlPlayer.stop(); } catch (_) {}
            seg._htmlPlayer = null;
        }
        // <audio> sans Icy-MetaData → Icecast envoie du MP3 nu (OK Firefox).
        const audioEl = new Audio(seg.url);
        audioEl.crossOrigin = 'anonymous';
        applyAudioOutput(audioEl);
        audioEl.play().catch(() => {});
        const player = {
            audioElement: audioEl,
            play: () => audioEl.play().catch(() => {}),
            pause: () => audioEl.pause(),
            stop: () => {
                try {
                    audioEl.pause();
                    audioEl.removeAttribute('src');
                    audioEl.load();
                } catch (_) {}
            }
        };
        seg._htmlPlayer = player;
        seg.queueAudioEl = audioEl;
        seg.playbackPlayer = player;
        seg.ringPlaybackPending = false;
        if (seg.card) {
            seg.card.classList.add('playing');
            seg.card.classList.remove('queued');
            const st = seg.card.querySelector('.card-status');
            if (st) st.textContent = 'En cours';
        }
    }

    function startExclusivePlay(seg) {
        stopExclusivePlay(seg, { keepBuffer: true, keepIngest: true });
        // Queue MSE uniquement (Chrome). Firefox : l'option est masquée, pas de fallback HTML.
        if (!mseMpegSupported()) return;
        // Mode exclusif = pipeline ring buffer MSE (Chrome/Chromium).
        if (!getRingBufferKit()) {
            if (seg.card) {
                const st = seg.card.querySelector('.card-status');
                if (st) st.textContent = 'Tamponnage…';
            }
            window.addEventListener('queue-ring-buffer-ready', () => {
                if (exclusiveActive === seg) startExclusivePlay(seg);
            }, { once: true });
            return;
        }
        if (!seg.ringIngestor) startQueueRingIngestion(seg);
        if (seg.ringBuffer) seg.ringBuffer.setPlaybackPaused(false);
        const fromStart = !(seg.bufferPlayer && seg.bufferPlayer.hasStarted);
        activateSegmentRingPlayback(seg, { playFromStart: fromStart });
        if (seg.card) {
            seg.card.classList.add('playing');
            seg.card.classList.remove('queued');
        }
    }

    function stopExclusivePlay(seg, options = {}) {
        if (!seg) return;
        seg._ringPlayGen = (seg._ringPlayGen || 0) + 1;
        if (seg._htmlPlayer) {
            try { seg._htmlPlayer.stop(); } catch (_) {}
            seg._htmlPlayer = null;
        }
        if (seg.bufferPlayer) {
            try { seg.bufferPlayer.pause(); } catch (_) {}
        }
        if (seg.ringBuffer) seg.ringBuffer.setPlaybackPaused(true);
        if (!options.keepBuffer && !options.keepIngest) {
            stopQueueRingPipeline(seg, options);
        } else if (!options.keepIngest && seg.ringIngestor && !options.keepBuffer) {
            stopQueueRingPipeline(seg, options);
        }
        seg.playbackPlayer = null;
        if (seg.card) {
            seg.card.classList.remove('playing');
            seg.card.classList.add('queued');
        }
    }

    function refreshExclusiveCards() {
        const all = [...exclusiveQueue];
        if (exclusiveActive) all.unshift(exclusiveActive);
        // Playing card first, then waiting cards (queue order).
        if (el.streamsContainer) {
            all.forEach((seg) => {
                if (seg.card && seg.card.parentNode === el.streamsContainer) {
                    el.streamsContainer.appendChild(seg.card);
                }
            });
        }
        all.forEach(seg => {
            if (!seg.card) return;
            const isActive = exclusiveActive && exclusiveActive.key === seg.key;
            const status = seg.card.querySelector('.card-status');
            const pauseBtn = seg.card.querySelector('.pause-btn');
            const promoteBtn = seg.card.querySelector('.queue-promote-btn');
            const entry = catalogEntryFor(seg);
            const title = getSegmentDisplayTitle(seg);
            updateCardHeader(seg.card, entry, {
                txTitle: title,
                role: queueRoleLabel(seg, isActive),
                groupLabel: segmentDisplayName(seg)
            });
            if (status && !blankDeadlines[seg.key]) {
                if (isActive) {
                    const waiting = getRingWaitingSeconds(seg);
                    if (seg.ringPlaybackPending) {
                        status.textContent = 'Tamponnage…';
                    } else if (isPlaybackInBlank(seg) && waiting <= 0.5) {
                        status.textContent = 'Fin de com…';
                    } else if (waiting > getLiveWaitingThreshold()) {
                        status.textContent = `Rattrapage (${waiting.toFixed(1)}s)`;
                    } else {
                        status.textContent = seg.manualPriority ? 'En cours · choix' : 'En cours';
                    }
                } else {
                    status.textContent = seg.suspendedByUser
                        ? 'En pause'
                        : (segmentHasRingContent(seg)
                            ? `En attente (${getRingWaitingSeconds(seg).toFixed(1)}s)`
                            : 'En attente');
                }
            }
            // File = uniquement Écouter ; actif = Mute / Pause / Direct / Stop
            if (promoteBtn) {
                promoteBtn.style.display = isActive ? 'none' : '';
                promoteBtn.textContent = seg.suspendedByUser ? '▶️ Reprendre' : '▶️ Écouter';
            }
            ['mute-btn', 'pause-btn', 'live-btn', 'stop-btn'].forEach((cls) => {
                const btn = seg.card.querySelector('.' + cls);
                if (btn) btn.style.display = isActive ? '' : 'none';
            });
            if (pauseBtn && isActive) pauseBtn.textContent = '⏸️ Pause';
            seg.card.classList.toggle('playing', !!isActive);
            seg.card.classList.toggle('queued', !isActive);
            seg.card.classList.toggle('manual-priority', !!(isActive && seg.manualPriority));
            if (isActive) syncRingBufferTitleDisplay(seg);
            updateRingProgressBar(seg);
            updateLiveButton(seg);
        });
        ensureRingUiTimer();
    }

    function syncPlayersFromCatalogNow() {
        if (isAdminPage || !audioEnabled) return;
        const now = Date.now();
        catalog.forEach(entry => {
            noteEntryActivityForBlank(entry);
            const inExclusiveQ = exclusiveQueue.some(s => s.key === entry.key);
            const isActiveEx = exclusiveActive && exclusiveActive.key === entry.key;
            const has = players.has(entry.key) || inExclusiveQ || isActiveEx;
            const monitored = shouldMonitorEntry(entry);

            // Start when activity detected — wait MIN_ON_AIR_START_MS to ignore parasites.
            // If a card is already open, short transmissions still fill the buffer as usual.
            if (entry.on_air && monitored && !has) {
                const ownerKey = streamUrlOwnerKey(entry.streamUrl);
                // Exiger le propriétaire exact — ne pas démarrer si ownerKey est null
                // (ex. fréquence courante désactivée sur un mount partagé).
                if (ownerKey === entry.key) {
                    offAirSince.delete(entry.key);
                    requestStartEntry(entry);
                }
                return;
            }
            if (!entry.on_air) {
                clearPendingCardStart(entry.key);
            }

            if (has && !isEnabled(entry)) {
                stopEntry(entry.key);
                return;
            }

            // Remplacement live par une autre fréquence / entrée sur le même mount.
            if (has && entry.on_air) {
                const ownerKey = streamUrlOwnerKey(entry.streamUrl);
                if (ownerKey && ownerKey !== entry.key) {
                    stopEntry(entry.key);
                    return;
                }
            }

            // Fin de com live : garder la carte tant que le ring a de l'audio à lire.
            if (exclusivePlaybackMode && has && !entry.on_air) {
                const seg = getExclusiveSegment(entry.key);
                if (seg && usesQueueRingBuffer() && segmentHasRingContent(seg)) {
                    offAirSince.delete(entry.key);
                    return;
                }
            }

            // Queued exclusive: keep while ring still holds audio / title.
            // Only drop empty segments after off-air grace (legacy / no content).
            if (exclusivePlaybackMode && inExclusiveQ && !isActiveEx && !entry.on_air) {
                const seg = exclusiveQueue.find(s => s.key === entry.key);
                if (seg && usesQueueRingBuffer() && segmentHasRingContent(seg)) {
                    offAirSince.delete(entry.key);
                    return;
                }
                if (!offAirSince.has(entry.key)) offAirSince.set(entry.key, now);
                if (now - offAirSince.get(entry.key) >= OFF_AIR_GRACE_MS) {
                    if (!seg || !segmentHasRingContent(seg)) stopEntry(entry.key);
                }
            } else if (entry.on_air) {
                offAirSince.delete(entry.key);
            }
        });
        pruneStreamCardsByUrlOwner();
        [...players.keys()].forEach(key => {
            if (!catalog.has(key)) stopEntry(key);
        });
    }

    function syncPlayersFromCatalog() {
        if (syncPlayersTimer) clearTimeout(syncPlayersTimer);
        syncPlayersTimer = setTimeout(() => {
            syncPlayersTimer = null;
            syncPlayersFromCatalogNow();
        }, SYNC_DEBOUNCE_MS);
    }

    /* ---- Pulse admin (Airband local + Tetrapol remote) ---- */

    function updatePulseAdmin(state) {
        if (state) pulseExclusiveState = state;
        // Appliquer priority_groups AVANT le calcul de signature / early-return,
        // sinon les autres instances ne voient jamais le changement de prio.
        applyAirbandPulsePriorities(pulseExclusiveState);
        applyTetrapolPulsePriorities(tpPulseExclusiveState);
        if (!isAdminMode) return;
        // Boutons réseaux Pulse : toujours (même si early-return fills/struct)
        updatePulseNetworkButtons();

        const abState = pulseExclusiveState || {};
        const tpState = tpPulseExclusiveState || {};
        const exclusive = !!(abState.exclusive || tpState.exclusive);
        const forceOn = !!(abState.force_before_full || tpState.force_before_full);
        const catchupFast = !!(abState.catchup_fast || tpState.catchup_fast);
        const abActive = abState.active_group_id ? String(abState.active_group_id) : '';
        const tpActive = tpState.active_group_id ? String(tpState.active_group_id) : '';
        const arb = pulseArbiterState;
        // File unifiée : un seul actif (arbitre). Ne pas OR les is_active locaux
        // Airband + Tetrapol, sinon deux boutons s'allument en même temps.
        const arbEnabled = !!(arb && arb.enabled !== false && exclusive);
        const arbActive = arbEnabled && arb.active ? arb.active : null;
        const displayActiveId = arbActive ? String(arbActive.id)
            : (arbEnabled ? '' : (abActive || tpActive || ''));
        const displayActiveSource = arbActive ? arbActive.source
            : (arbEnabled ? '' : (abActive ? 'airband' : (tpActive ? 'tetrapol' : '')));
        // Avec l'arbitre : seul le Stop utilisateur compte (pas le manual_hold
        // technique posé à chaque yield/activate).
        const manualHold = arbEnabled
            ? !!arb.user_hold
            : !!(abState.manual_hold || tpState.manual_hold);

        const abListen = new Map((abState.listen_groups || []).map(g => [String(g.group_id), g]));
        const tpListen = new Map((tpState.listen_groups || []).map(g => [String(g.group_id), g]));
        const airPulse = [...catalog.values()].filter(e =>
            e.source === 'airband' && e.meta && e.meta.has_pulse);
        const tpPulse = [...catalog.values()].filter(e =>
            e.source === 'tetrapol' && !e.encrypted);
        // Sticky: garder les groupes connus via télémétrie même si le catalog est vide un tick.
        const pulseByKey = new Map([...airPulse, ...tpPulse].map((e) => [e.key, e]));
        abListen.forEach((g, gid) => {
            const key = airKey(gid);
            if (pulseByKey.has(key)) return;
            pulseByKey.set(key, {
                key,
                source: 'airband',
                rawId: gid,
                label: pulseGroupLabel(gid),
                on_air: pulseGroupOnAir('airband', gid, null, g),
                meta: { has_pulse: true, listen: true },
                has_stream: false,
                streamUrl: null
            });
        });
        tpListen.forEach((g, gid) => {
            const key = tpKey(gid);
            if (pulseByKey.has(key)) return;
            pulseByKey.set(key, {
                key,
                source: 'tetrapol',
                rawId: gid,
                label: pulseGroupLabel(gid),
                on_air: pulseGroupOnAir('tetrapol', gid, null, g),
                encrypted: false,
                meta: { listen: true, encrypted: false },
                has_stream: true,
                streamUrl: buildTetrapolUrl(gid)
            });
        });
        const pulseEntries = [...pulseByKey.values()];
        const structSig = JSON.stringify({
            e: exclusive,
            a: displayActiveId,
            as: displayActiveSource,
            m: manualHold,
            f: forceOn,
            c: catchupFast,
            arbA: arbActive ? `${arbActive.source}:${arbActive.id}` : '',
            g: pulseEntries.map(e => [
                e.key,
                (e.meta && e.meta.listen !== false) ? 1 : 0
            ])
        });
        const fillSig = JSON.stringify({
            qAb: abState.queue_order || [],
            qTp: tpState.queue_order || [],
            arbQ: arb && arb.queue ? arb.queue.map((s) => `${s.source}:${s.id}`).join(',') : '',
            g: pulseEntries.map(e => {
                const pe = e.source === 'tetrapol'
                    ? (tpListen.get(String(e.rawId)) || {})
                    : (abListen.get(String(e.rawId)) || {});
                const onAir = pulseGroupOnAir(e.source, e.rawId, e, pe);
                const isAct = exclusive && pulseGroupIsPlaying(
                    e.source, e.rawId, displayActiveId, displayActiveSource);
                return [
                    e.key,
                    Math.floor((Number(pe.fill_ratio) || 0) * 100),
                    Math.floor((Number(pe.buffer_seconds) || 0) * 2),
                    onAir ? 1 : 0,
                    isAct ? 1 : 0,
                    (isPulsePriority(e.key) || pe.priority) ? 1 : 0,
                    pe.suspended ? 1 : 0,
                    pe.in_queue ? 1 : 0,
                    e.source === 'tetrapol' ? pulseRfsiForGroup(e.rawId) : ''
                ];
            })
        });
        const needStruct = structSig !== lastPulseSig;
        const needFills = fillSig !== lastPulseFillSig;
        if (!needStruct && !needFills) return;
        lastPulseSig = structSig;
        lastPulseFillSig = fillSig;

        const section = document.getElementById('adminPulseAudioSection');
        if (section) section.style.display = 'block';
        const toggle = document.getElementById('pulseExclusivePlaybackToggle');
        const force = document.getElementById('pulseExclusiveForceBufferToggle');
        const forceCtl = document.getElementById('pulseExclusiveForceBufferControl');
        const catchup = document.getElementById('pulseCatchupFastToggle');
        const btnBox = document.getElementById('pulseAudioButtonsContainer');
        const statusPanel = document.getElementById('pulseExclusiveStatusPanel');
        const summary = document.getElementById('pulseExclusiveStatusSummary');
        const list = document.getElementById('pulseExclusiveStatusList');

        if (toggle && document.activeElement !== toggle) toggle.checked = exclusive;
        if (forceCtl) forceCtl.style.display = exclusive ? '' : 'none';
        if (force && document.activeElement !== force) force.checked = forceOn;
        if (catchup && document.activeElement !== catchup) catchup.checked = catchupFast;

        const uiState = {
            exclusive,
            force_before_full: forceOn,
            catchup_fast: catchupFast,
            manual_hold: manualHold,
            active_group_id: displayActiveId,
            active_source: displayActiveSource,
            buffer_max_seconds: abState.buffer_max_seconds || tpState.buffer_max_seconds || 120,
            listen_groups: buildStablePulseListenGroups(),
            queue_order: [
                ...((abState.queue_order || []).map(String)),
                ...((tpState.queue_order || []).map(String))
            ]
        };

        if (btnBox && needStruct) {
            const sortedPulse = sortEntriesByGroupNumber(pulseEntries);
            const wanted = new Set(sortedPulse.map(e => e.key));
            [...btnBox.querySelectorAll('.pulse-btn')].forEach(btn => {
                if (!wanted.has(btn.dataset.key)) btn.remove();
            });
            sortedPulse.forEach(entry => {
                const key = entry.key;
                const gid = String(entry.rawId);
                const pe = entry.source === 'tetrapol'
                    ? (tpListen.get(gid) || {})
                    : (abListen.get(gid) || {});
                const listening = !(entry.meta && entry.meta.listen === false);
                const prio = isPulsePriority(key) || !!(pe.priority);
                let btn = btnBox.querySelector('.pulse-btn[data-key="' + esc(key) + '"]');
                if (!btn) {
                    btn = document.createElement('button');
                    btn.type = 'button';
                    btn.className = 'pulse-btn';
                    btn.dataset.key = key;
                    btn.dataset.groupId = gid;
                    btn.dataset.source = entry.source;
                    const label = document.createElement('span');
                    label.className = 'pulse-btn-label';
                    btn.appendChild(label);
                    const fill = document.createElement('div');
                    fill.className = 'pulse-btn-fill';
                    fill.innerHTML = '<div class="pulse-btn-fill-inner"></div>';
                    btn.appendChild(fill);
                    bindLongPressPriority(
                        btn,
                        () => togglePulsePriority(btn.dataset.key),
                        async () => {
                            const id = btn.dataset.groupId;
                            const src = btn.dataset.source;
                            if (src === 'airband') {
                                const ch = airbandById.get(id);
                                const next = !(ch && ch.listen);
                                await apiPulseToggle(id, next);
                                if (ch) ch.listen = next;
                            } else {
                                const g = tetrapolGroups.get(String(id));
                                const next = !(g && g.listen !== false);
                                await apiTetrapolToggle(id, next);
                                if (g) g.listen = next;
                            }
                            lastPulseSig = '';
                            lastPulseFillSig = '';
                            rebuildCatalog();
                            updatePulseAdmin();
                        }
                    );
                }
                btnBox.appendChild(btn);
                const labelEl = btn.querySelector('.pulse-btn-label');
                if (labelEl) {
                    const prefix = entry.source === 'tetrapol' ? '📻 ' : '✈️ ';
                    labelEl.textContent = prefix + labelOf(entry);
                }
                btn.classList.toggle('enabled', !!listening);
                btn.classList.toggle('on-air', pulseGroupOnAir(entry.source, gid, entry, pe));
                btn.classList.toggle('priority', !!prio);
                const isAct = exclusive && pulseGroupIsPlaying(
                    entry.source, gid, displayActiveId, displayActiveSource);
                btn.classList.toggle('pulse-active', isAct);
                const fillInner = btn.querySelector('.pulse-btn-fill-inner');
                if (fillInner && fillInner.parentElement) {
                    const pct = Math.max(0, Math.min(100, Math.round((pe.fill_ratio || 0) * 100)));
                    fillInner.style.width = pct + '%';
                    fillInner.parentElement.style.display = '';
                }
                btn.title = isAct
                    ? ('En lecture Pulse · ' + gid)
                    : (prio
                    ? 'Priorité Pulse — clic droit / appui long pour retirer'
                    : ((entry.source === 'tetrapol' ? 'Pulse Tetrapol · ' : 'Pulse Analogique · ')
                        + gid + (listening ? ' · ON' : ' · OFF')));
            });
        } else if (btnBox && needFills) {
            pulseEntries.forEach((entry) => {
                const pe = entry.source === 'tetrapol'
                    ? (tpListen.get(String(entry.rawId)) || {})
                    : (abListen.get(String(entry.rawId)) || {});
                const btn = btnBox.querySelector('.pulse-btn[data-key="' + esc(entry.key) + '"]');
                if (!btn) return;
                const onAir = pulseGroupOnAir(entry.source, entry.rawId, entry, pe);
                const prio = isPulsePriority(entry.key) || !!(pe.priority);
                const isAct = exclusive && pulseGroupIsPlaying(
                    entry.source, entry.rawId, displayActiveId, displayActiveSource);
                btn.classList.toggle('on-air', onAir);
                btn.classList.toggle('priority', !!prio);
                btn.classList.toggle('pulse-active', isAct);
                const fillInner = btn.querySelector('.pulse-btn-fill-inner');
                if (fillInner && fillInner.parentElement) {
                    const pct = Math.max(0, Math.min(100, Math.round((pe.fill_ratio || 0) * 100)));
                    fillInner.style.width = pct + '%';
                    fillInner.parentElement.style.display = '';
                }
            });
        }
        if (statusPanel && summary && list) {
            renderPulseExclusiveStatusPanel(uiState, statusPanel, summary, list);
        }
    }

    function formatPulseBufferSeconds(seconds, maxSec) {
        const s = Math.max(0, Number(seconds) || 0);
        const max = Math.max(1, Number(maxSec) || 120);
        if (s < 1) return `0 / ${max} s`;
        return `${s.toFixed(s >= 10 ? 0 : 1)} / ${max} s`;
    }

    function pulseGroupLabel(gid) {
        const entry = catalog.get(airKey(gid)) || catalog.get(tpKey(gid));
        if (entry) return labelOf(entry);
        const ch = airbandById.get(gid);
        if (ch) return ch.label || gid;
        const g = tetrapolGroups.get(String(gid));
        if (g) return g.group_label || gid;
        return String(gid);
    }

    function pulseRfsiForGroup(gid) {
        const channels = tetrapolChannels.filter(c => String(c.group_id) === String(gid));
        if (!channels.length) return '';
        const onAir = channels.filter(c => c.on_air).sort((a, b) => (b.last_active_ms || 0) - (a.last_active_ms || 0));
        const ch = onAir[0] || [...channels].sort((a, b) => (b.last_active_ms || 0) - (a.last_active_ms || 0))[0];
        if (!ch) return '';
        const label = (ch.rfsi_label || '').trim();
        const rfsi = (ch.rfsi || '').trim();
        if (label && rfsi && label !== rfsi) return `${label} (${rfsi})`;
        return label || rfsi || '';
    }

    function buildStablePulseListenGroups() {
        const abState = pulseExclusiveState || {};
        const tpState = tpPulseExclusiveState || {};
        const abTel = {};
        (abState.listen_groups || []).forEach((g) => {
            abTel[String(g.group_id)] = g;
        });
        const tpTel = {};
        (tpState.listen_groups || []).forEach((g) => {
            tpTel[String(g.group_id)] = g;
        });
        const abQueue = Array.isArray(abState.queue_order) ? abState.queue_order.map(String) : [];
        const tpQueue = Array.isArray(tpState.queue_order) ? tpState.queue_order.map(String) : [];
        const arb = pulseArbiterState;
        const exclusive = !!(abState.exclusive || tpState.exclusive);
        const arbEnabled = !!(arb && arb.enabled !== false && exclusive);
        const arbActive = arbEnabled && arb.active ? arb.active : null;
        const abActive = abState.active_group_id ? String(abState.active_group_id) : '';
        const tpActive = tpState.active_group_id ? String(tpState.active_group_id) : '';
        const displayActiveId = arbActive ? String(arbActive.id)
            : (arbEnabled ? '' : (abActive || tpActive || ''));
        const displayActiveSource = arbActive ? arbActive.source
            : (arbEnabled ? '' : (abActive ? 'airband' : (tpActive ? 'tetrapol' : '')));

        const rows = [];
        const seen = new Set();

        [...catalog.values()].filter(e => e.source === 'airband' && e.meta && e.meta.has_pulse).forEach((entry) => {
            if (entry.meta.listen === false) return;
            const gid = String(entry.rawId);
            seen.add('airband:' + gid);
            const tel = abTel[gid] || {};
            const isAct = pulseGroupIsPlaying('airband', gid, displayActiveId, displayActiveSource);
            rows.push({
                group_id: gid,
                priority: !!(tel.priority || isPulsePriority(airKey(gid))),
                is_active: isAct,
                suspended: !!tel.suspended,
                fill_ratio: Number(tel.fill_ratio) || 0,
                buffer_seconds: Number(tel.buffer_seconds) || 0,
                in_queue: !!(tel.in_queue || abQueue.includes(gid)
                    || (arb && arb.queue && arb.queue.some(s => s.source === 'airband' && String(s.id) === gid))),
                on_air: pulseGroupOnAir('airband', gid, entry, tel),
                source: 'airband'
            });
        });
        Object.keys(abTel).forEach((gid) => {
            if (seen.has('airband:' + gid)) return;
            const tel = abTel[gid];
            rows.push({
                group_id: gid,
                priority: !!tel.priority,
                is_active: pulseGroupIsPlaying('airband', gid, displayActiveId, displayActiveSource),
                suspended: !!tel.suspended,
                fill_ratio: Number(tel.fill_ratio) || 0,
                buffer_seconds: Number(tel.buffer_seconds) || 0,
                in_queue: !!(tel.in_queue || abQueue.includes(gid)),
                on_air: pulseGroupOnAir('airband', gid, null, tel),
                source: 'airband'
            });
        });

        [...catalog.values()].filter(e => e.source === 'tetrapol' && !e.encrypted).forEach((entry) => {
            if (entry.meta && entry.meta.listen === false) return;
            const gid = String(entry.rawId);
            seen.add('tetrapol:' + gid);
            const tel = tpTel[gid] || {};
            const isAct = pulseGroupIsPlaying('tetrapol', gid, displayActiveId, displayActiveSource);
            rows.push({
                group_id: gid,
                priority: !!(tel.priority || isPulsePriority(tpKey(gid))),
                is_active: isAct,
                suspended: !!tel.suspended,
                fill_ratio: Number(tel.fill_ratio) || 0,
                buffer_seconds: Number(tel.buffer_seconds) || 0,
                in_queue: !!(tel.in_queue || tpQueue.includes(gid)
                    || (arb && arb.queue && arb.queue.some(s => s.source === 'tetrapol' && String(s.id) === gid))),
                on_air: pulseGroupOnAir('tetrapol', gid, entry, tel),
                source: 'tetrapol'
            });
        });
        Object.keys(tpTel).forEach((gid) => {
            if (seen.has('tetrapol:' + gid)) return;
            const tel = tpTel[gid];
            rows.push({
                group_id: gid,
                priority: !!tel.priority,
                is_active: pulseGroupIsPlaying('tetrapol', gid, displayActiveId, displayActiveSource),
                suspended: !!tel.suspended,
                fill_ratio: Number(tel.fill_ratio) || 0,
                buffer_seconds: Number(tel.buffer_seconds) || 0,
                in_queue: !!(tel.in_queue || tpQueue.includes(gid)),
                on_air: pulseGroupOnAir('tetrapol', gid, null, tel),
                source: 'tetrapol'
            });
        });

        rows.sort((a, b) => {
            if (a.source !== b.source) return a.source === 'airband' ? -1 : 1;
            return String(a.group_id).localeCompare(String(b.group_id), undefined, { numeric: true });
        });
        return rows;
    }

    function pulseActionButtonFromEvent(e, panel) {
        const btn = e.target && e.target.closest ? e.target.closest('[data-pulse-action]') : null;
        if (!btn || !panel.contains(btn)) return null;
        return btn;
    }

    async function handlePulsePanelAction(btn) {
        const action = btn.getAttribute('data-pulse-action');
        let gid = btn.getAttribute('data-group-id') || '';
        let src = btn.getAttribute('data-source') || resolvePulseSource(gid);
        const dedupeKey = action + ':' + src + ':' + gid;
        const now = Date.now();
        if (dedupeKey === lastPulseActionKey && now - lastPulseActionAt < 400) return;
        lastPulseActionKey = dedupeKey;
        lastPulseActionAt = now;
        if (pulseControlInFlight) return;
        lastPulseSig = '';
        lastPulseFillSig = '';
        pulseLocalEditUntil = Date.now() + 2000;
        pulseControlInFlight = true;

        try {
        const prevAb = pulseExclusiveState && typeof pulseExclusiveState === 'object'
            ? pulseExclusiveState : {};
        const prevTp = tpPulseExclusiveState && typeof tpPulseExclusiveState === 'object'
            ? tpPulseExclusiveState : {};
        if (action === 'stop') {
            let stopGid = gid;
            let stopSrc = src;
            if (!stopGid && pulseArbiterState && pulseArbiterState.active) {
                stopGid = String(pulseArbiterState.active.id);
                stopSrc = pulseArbiterState.active.source;
            }
            if (!stopGid) {
                stopGid = String(prevAb.active_group_id || prevTp.active_group_id || '');
                stopSrc = resolvePulseSource(stopGid);
            }
            if (stopGid) {
                applyLocalPulseListen(stopSrc, stopGid, false);
                gid = stopGid;
                src = stopSrc;
            }
            pulseExclusiveState = { ...prevAb, active_group_id: '' };
            tpPulseExclusiveState = { ...prevTp, active_group_id: '' };
            updatePulseAdmin();
        } else if (action === 'pause') {
            pulseExclusiveState = { ...prevAb, active_group_id: '', manual_hold: true };
            tpPulseExclusiveState = { ...prevTp, active_group_id: '', manual_hold: true };
            updatePulseAdmin();
        } else if (action === 'auto') {
            pulseExclusiveState = { ...prevAb, manual_hold: false };
            tpPulseExclusiveState = { ...prevTp, manual_hold: false };
            updatePulseAdmin();
        } else if (action === 'play' && gid) {
            if (src === 'tetrapol') {
                tpPulseExclusiveState = { ...prevTp, active_group_id: gid, manual_hold: false };
                pulseExclusiveState = { ...prevAb, active_group_id: '', manual_hold: false };
            } else {
                pulseExclusiveState = { ...prevAb, active_group_id: gid, manual_hold: false };
                tpPulseExclusiveState = { ...prevTp, active_group_id: '', manual_hold: false };
            }
            if (pulseArbiterState && typeof pulseArbiterState === 'object') {
                pulseArbiterState = { ...pulseArbiterState, user_hold: false };
            }
            updatePulseAdmin();
        } else if (action === 'clear' && gid) {
            const clearListen = (prev) => {
                const listen = Array.isArray(prev.listen_groups)
                    ? prev.listen_groups.map((g) => {
                        if (String(g.group_id) !== String(gid)) return g;
                        return { ...g, fill_ratio: 0, buffer_seconds: 0 };
                    })
                    : prev.listen_groups;
                const fills = { ...(prev.fill_ratios || {}) };
                fills[gid] = 0;
                return { ...prev, listen_groups: listen, fill_ratios: fills };
            };
            if (src === 'tetrapol') tpPulseExclusiveState = clearListen(prevTp);
            else pulseExclusiveState = clearListen(prevAb);
            updatePulseAdmin();
        }

            const arbOn = !!(pulseArbiterState && pulseArbiterState.enabled !== false
                && (pulseExclusiveState.exclusive || tpPulseExclusiveState.exclusive));
            let result = null;
            if (arbOn) {
                if (action === 'stop') {
                    if (gid) {
                        if (src === 'tetrapol') await apiTetrapolToggle(gid, false);
                        else await apiPulseToggle(gid, false);
                    }
                    result = await apiPulseArbiter('stop');
                } else if (action === 'pause') {
                    result = await apiPulseArbiter('pause');
                } else if (action === 'auto') {
                    result = await apiPulseArbiter('auto');
                } else if (action === 'play' && gid) {
                    result = await apiPulseArbiter('play', { source: src, id: gid });
                } else if (action === 'clear' && gid) {
                    result = await apiPulseArbiter('clear', { source: src, id: gid });
                }
                if (result && result.arbiter) pulseArbiterState = result.arbiter;
            } else if (src === 'tetrapol') {
                if (action === 'stop') {
                    if (gid) await apiTetrapolToggle(gid, false);
                    result = await apiTetrapolPulseExclusive({
                        yield_active: true, active_group_id: ''
                    });
                } else if (action === 'pause') {
                    result = await apiTetrapolPulseExclusive({
                        yield_active: true, active_group_id: '', manual_hold: true
                    });
                } else if (action === 'auto') {
                    result = await apiTetrapolPulseExclusive({ manual_hold: false });
                } else if (action === 'play' && gid) {
                    result = await apiTetrapolPulseExclusive({
                        active_group_id: gid, manual_hold: false
                    });
                } else if (action === 'clear' && gid) {
                    result = await apiTetrapolPulseExclusive({ clear_buffer: gid });
                }
                if (result) tpPulseExclusiveState = { ...tpPulseExclusiveState, ...result };
            } else {
                if (action === 'stop') {
                    if (gid) await apiPulseToggle(gid, false);
                    result = await apiPulseExclusive({ stop_active: true });
                } else if (action === 'pause') {
                    result = await apiPulseExclusive({ stop_active: true, manual_hold: true });
                } else if (action === 'auto') {
                    result = await apiPulseExclusive({ manual_hold: false });
                } else if (action === 'play' && gid) {
                    result = await apiPulseExclusive({
                        active_group_id: gid, manual_hold: false
                    });
                } else if (action === 'clear' && gid) {
                    result = await apiPulseExclusive({ clear_buffer: gid });
                }
                if (result) pulseExclusiveState = { ...pulseExclusiveState, ...result };
            }
            pulseLocalEditUntil = Date.now() + 1500;
            updatePulseAdmin();
        } finally {
            pulseControlInFlight = false;
        }
    }

    function ensurePulseExclusivePanelDelegation(panel) {
        if (!panel || pulsePanelDelegated) return;
        pulsePanelDelegated = true;
        // pointerdown : l'action part tout de suite, avant qu'un refresh télémétrie
        // ne remplace le bouton et n'annule le click (mousedown + mouseup).
        // Le click suivant est ignoré : après Play le DOM peut remplacer le bouton
        // par Pause sous le curseur, et le click retomberait sur Pause.
        let pulsePointerHandledAt = 0;
        panel.addEventListener('pointerdown', (e) => {
            if (e.pointerType === 'mouse' && e.button !== 0) return;
            const btn = pulseActionButtonFromEvent(e, panel);
            if (!btn) return;
            e.preventDefault();
            e.stopPropagation();
            pulsePointerHandledAt = Date.now();
            handlePulsePanelAction(btn);
        });
        panel.addEventListener('click', (e) => {
            const btn = pulseActionButtonFromEvent(e, panel);
            if (!btn) return;
            e.preventDefault();
            e.stopPropagation();
            if (Date.now() - pulsePointerHandledAt < 700) return;
            handlePulsePanelAction(btn);
        });
    }

    function renderPulseExclusiveStatusPanel(state, panel, summaryEl, listEl) {
        const exclusive = !!state.exclusive;
        const maxSec = state.buffer_max_seconds || 120;
        const atRiskRatio = 0.80;
        const forceRatio = 0.92;
        const activeId = state.active_group_id ? String(state.active_group_id) : '';
        const activeSource = state.active_source || '';
        const manualHold = !!state.manual_hold;
        const forceOn = !!state.force_before_full;

        panel.style.display = '';
        ensurePulseExclusivePanelDelegation(panel);

        if (!exclusive) {
            if (summaryEl.dataset.pulseChrome !== 'parallel') {
                summaryEl.dataset.pulseChrome = 'parallel';
                summaryEl.innerHTML =
                    '<span class="pulse-mode-badge parallel">Mode parallèle</span> '
                    + 'Tous les groupes activés écoutent simultanément sur PulseAudio.';
            }
            if (listEl.innerHTML) listEl.innerHTML = '';
            return;
        }

        const listenGroups = Array.isArray(state.listen_groups)
            ? state.listen_groups
            : buildStablePulseListenGroups();

        const patchPlaySlot = (playSlot, g, isActive) => {
            if (!playSlot) return;
            const gid = String(g.group_id);
            const src = g.source || 'airband';
            if (isActive) {
                if (playSlot.querySelector('.pulse-fill-play-btn')) playSlot.innerHTML = '';
                if (!playSlot.querySelector('.pulse-fill-active-icon')) {
                    playSlot.innerHTML = '<span class="pulse-fill-active-icon" title="En lecture" aria-hidden="true">▶</span>';
                }
                return;
            }
            if (playSlot.querySelector('.pulse-fill-active-icon')) playSlot.innerHTML = '';
            let playBtn = playSlot.querySelector('.pulse-fill-play-btn');
            if (!playBtn) {
                playBtn = document.createElement('button');
                playBtn.type = 'button';
                playBtn.className = 'pulse-manual-btn pulse-fill-play-btn';
                playBtn.title = 'Lancer ce groupe sur PulseAudio maintenant';
                playBtn.setAttribute('data-pulse-action', 'play');
                playBtn.textContent = '▶';
                playSlot.appendChild(playBtn);
            }
            playBtn.setAttribute('data-group-id', gid);
            playBtn.setAttribute('data-source', src);
        };

        const patchClearBtn = (titleRow, g, show) => {
            if (!titleRow) return;
            const gid = String(g.group_id);
            const src = g.source || 'airband';
            let clearBtn = titleRow.querySelector('.pulse-fill-clear-btn');
            if (!show) {
                if (clearBtn) clearBtn.remove();
                return;
            }
            if (!clearBtn) {
                clearBtn = document.createElement('button');
                clearBtn.type = 'button';
                clearBtn.className = 'pulse-manual-btn pulse-clear-btn pulse-fill-clear-btn';
                clearBtn.title = 'Vider le tampon et revenir au direct';
                clearBtn.setAttribute('data-pulse-action', 'clear');
                clearBtn.textContent = '✕';
                titleRow.appendChild(clearBtn);
            }
            clearBtn.setAttribute('data-group-id', gid);
            clearBtn.setAttribute('data-source', src);
        };

        const patchRowFill = (row, g) => {
            let fill = Number(g.fill_ratio);
            if (!Number.isFinite(fill)) fill = 0;
            if (fill > 1) fill = fill / 100;
            fill = Math.min(1, Math.max(0, fill));
            let bufSec = Number(g.buffer_seconds);
            if (!Number.isFinite(bufSec)) bufSec = 0;
            if (bufSec > 0 && maxSec > 0) fill = Math.min(fill, bufSec / maxSec);
            const pct = Math.round(fill * 100);
            const atRisk = fill >= atRiskRatio;
            const forceSoon = fill >= forceRatio;
            const gid = String(g.group_id);
            const src = g.source || 'airband';
            const isActive = !!(g.is_active || (gid === activeId && (!activeSource || src === activeSource)));
            const onAir = !!g.on_air;
            const rfsiText = src === 'tetrapol' ? pulseRfsiForGroup(gid) : '';
            const groupLabel = pulseGroupLabel(gid);
            row.classList.toggle('pulse-fill-active', isActive);
            row.classList.toggle('pulse-fill-at-risk', atRisk);
            row.classList.toggle('pulse-fill-on-air', onAir);
            patchPlaySlot(row.querySelector('.pulse-fill-play-slot'), g, isActive);
            const nameEl = row.querySelector('.pulse-fill-name');
            if (nameEl) {
                const nextName = (src === 'tetrapol' ? '📻 ' : '✈️ ') + groupLabel;
                if (nameEl.textContent !== nextName) nameEl.textContent = nextName;
                nameEl.title = `Groupe ${gid}`;
            }
            const idEl = row.querySelector('.pulse-fill-id');
            if (idEl && idEl.textContent !== `#${gid}`) idEl.textContent = `#${gid}`;
            patchClearBtn(row.querySelector('.pulse-fill-title-row'), g, bufSec > 0.15 || pct > 0);
            const rfsiEl = row.querySelector('.pulse-fill-rfsi');
            if (rfsiEl) {
                const nextRfsi = rfsiText || '—';
                if (rfsiEl.textContent !== nextRfsi) rfsiEl.textContent = nextRfsi;
                rfsiEl.classList.toggle('pulse-fill-rfsi-empty', !rfsiText);
            }
            const meter = row.querySelector('.pulse-fill-meter');
            if (meter) {
                meter.classList.toggle('pulse-fill-meter-risk', atRisk);
                const bar = meter.querySelector('.pulse-fill-bar');
                if (bar) bar.setAttribute('aria-valuenow', String(pct));
                const inner = meter.querySelector('.pulse-fill-bar-inner');
                if (inner) inner.style.width = pct + '%';
                const timeEl = meter.querySelector('.pulse-fill-time');
                if (timeEl) timeEl.textContent = formatPulseBufferSeconds(bufSec, maxSec);
                const pctEl = meter.querySelector('.pulse-fill-pct');
                if (pctEl) pctEl.textContent = pct + ' %';
            }
            const badges = row.querySelector('.pulse-fill-badges');
            if (badges) {
                let html = '';
                if (isActive) html += '<span class="pulse-fill-badge active">ACTIF</span>';
                else if (g.in_queue) html += '<span class="pulse-fill-badge queue" title="En file d\'attente">FILE</span>';
                if (g.priority) html += '<span class="pulse-fill-badge priority">PRIO</span>';
                if (g.suspended) html += '<span class="pulse-fill-badge suspended">PAUSE</span>';
                if (onAir) html += '<span class="pulse-fill-badge on-air">ON AIR</span>';
                if (forceSoon && !isActive) html += '<span class="pulse-fill-badge force">≥92%</span>';
                if (badges.innerHTML !== html) badges.innerHTML = html;
            }
        };

        const createPulseFillRow = (g) => {
            const row = document.createElement('div');
            row.className = 'pulse-fill-row';
            row.dataset.groupId = String(g.group_id);
            row.dataset.source = g.source || 'airband';
            row.innerHTML =
                '<div class="pulse-fill-play-slot"></div>'
                + '<div class="pulse-fill-main">'
                + '<div class="pulse-fill-title-row">'
                + '<div class="pulse-fill-name"></div>'
                + '<span class="pulse-fill-id"></span>'
                + '</div>'
                + '<div class="pulse-fill-rfsi pulse-fill-rfsi-empty">—</div>'
                + '</div>'
                + '<div class="pulse-fill-badges"></div>'
                + '<div class="pulse-fill-meter">'
                + '<div class="pulse-fill-bar" role="progressbar" aria-valuemin="0" aria-valuemax="100">'
                + '<div class="pulse-fill-bar-inner" style="width:0%"></div></div>'
                + '<div class="pulse-fill-meter-meta">'
                + '<span class="pulse-fill-time"></span>'
                + '<span class="pulse-fill-pct"></span>'
                + '</div></div>';
            return row;
        };

        const buildArbiterLineHtml = () => {
            if (!(pulseArbiterState && pulseArbiterState.enabled
                && (pulseArbiterState.active || (pulseArbiterState.queue && pulseArbiterState.queue.length)))) {
                return '';
            }
            const a = pulseArbiterState.active;
            const q = pulseArbiterState.queue || [];
            const srcLabel = (s) => (s === 'tetrapol' ? 'TP' : 'AB');
            let html = '<span class="pulse-mode-badge">File unifiée</span> ';
            if (a) {
                html += `▶ ${srcLabel(a.source)} #${escHtml(String(a.id))}`;
                if (a.priority) html += ' ★';
            } else {
                html += '▶ —';
            }
            if (q.length) {
                html += ' · file: ' + q.map((s, i) =>
                    `${i + 1}. ${srcLabel(s.source)} #${escHtml(String(s.id))}${s.priority ? '★' : ''}`
                ).join(' → ');
            }
            return html;
        };

        const syncArbiterLine = () => {
            const html = buildArbiterLineHtml();
            let line = summaryEl.querySelector('.pulse-arbiter-line');
            if (!html) {
                if (line) line.remove();
                return;
            }
            if (!line) {
                line = document.createElement('div');
                line.className = 'pulse-arbiter-line';
                line.title = 'File Pulse unifiée Airband + Tetrapol';
                summaryEl.appendChild(line);
            }
            if (line.innerHTML !== html) line.innerHTML = html;
        };

        const activeFillMeta = () => {
            if (!activeId) return { fill: 0, bufSec: 0, pct: 0, atRisk: false };
            const g = listenGroups.find((x) =>
                String(x.group_id) === activeId
                && (!activeSource || (x.source || 'airband') === activeSource)
            ) || {};
            let fill = Number(g.fill_ratio);
            if (!Number.isFinite(fill)) fill = 0;
            if (fill > 1) fill = fill / 100;
            fill = Math.min(1, Math.max(0, fill));
            let bufSec = Number(g.buffer_seconds);
            if (!Number.isFinite(bufSec)) bufSec = 0;
            if (bufSec > 0 && maxSec > 0) fill = Math.min(fill, bufSec / maxSec);
            return {
                fill,
                bufSec,
                pct: Math.round(fill * 100),
                atRisk: fill >= atRiskRatio
            };
        };

        const syncActiveMeter = () => {
            const block = summaryEl.querySelector('.pulse-active-block');
            if (!block || !activeId) return;
            let meter = block.querySelector('.pulse-active-meter');
            if (!meter) {
                meter = document.createElement('div');
                meter.className = 'pulse-active-meter pulse-fill-meter';
                meter.innerHTML =
                    '<div class="pulse-fill-bar" role="progressbar" aria-valuemin="0" aria-valuemax="100">'
                    + '<div class="pulse-fill-bar-inner" style="width:0%"></div></div>'
                    + '<div class="pulse-fill-meter-meta">'
                    + '<span class="pulse-fill-time"></span>'
                    + '<span class="pulse-fill-pct"></span>'
                    + '</div>';
                block.appendChild(meter);
            }
            const meta = activeFillMeta();
            meter.classList.toggle('pulse-fill-meter-risk', meta.atRisk);
            const bar = meter.querySelector('.pulse-fill-bar');
            if (bar) bar.setAttribute('aria-valuenow', String(meta.pct));
            const inner = meter.querySelector('.pulse-fill-bar-inner');
            if (inner) inner.style.width = meta.pct + '%';
            const timeEl = meter.querySelector('.pulse-fill-time');
            if (timeEl) timeEl.textContent = formatPulseBufferSeconds(meta.bufSec, maxSec);
            const pctEl = meter.querySelector('.pulse-fill-pct');
            if (pctEl) pctEl.textContent = meta.pct + ' %';
        };

        const chromeKey = [2, activeId, activeSource, manualHold ? 1 : 0, forceOn ? 1 : 0].join('|');
        if (summaryEl.dataset.pulseChrome !== chromeKey) {
            summaryEl.dataset.pulseChrome = chromeKey;
            const activeLabel = activeId ? pulseGroupLabel(activeId) : null;
            let summaryHtml = '<span class="pulse-mode-badge">File exclusive</span> ';
            if (manualHold) {
                summaryHtml += '<span class="pulse-fill-badge suspended">MANUEL</span> ';
            }
            if (activeId) {
                const rfsiText = activeSource === 'tetrapol' ? pulseRfsiForGroup(activeId) : '';
                const srcBadge = activeSource === 'tetrapol' ? 'TP' : (activeSource === 'airband' ? 'AB' : '');
                const fillMeta = activeFillMeta();
                summaryHtml += '<div class="pulse-active-block">';
                summaryHtml += '<div class="pulse-active-main">';
                summaryHtml += '<div class="pulse-active-line">'
                    + '<span class="pulse-active-prefix">▶ En lecture :</span> '
                    + (srcBadge ? `<span class="pulse-mode-badge">${srcBadge}</span> ` : '')
                    + `<span class="pulse-active-name" title="${escHtml(activeLabel)} (#${escHtml(activeId)})">${escHtml(activeLabel)}</span> `
                    + `<span class="pulse-active-id">(#${escHtml(activeId)})</span>`
                    + '</div>';
                if (rfsiText) {
                    summaryHtml += `<div class="pulse-active-rfsi" title="RFSI">${escHtml(rfsiText)}</div>`;
                }
                summaryHtml += '</div>';
                summaryHtml += '<div class="pulse-active-actions">';
                summaryHtml += '<button type="button" class="pulse-manual-btn pulse-pause-btn" data-pulse-action="pause" title="Pause : arrêter la lecture sans relance auto (mode manuel)">⏸ Pause</button>';
                summaryHtml += `<button type="button" class="pulse-manual-btn pulse-stop-btn" data-pulse-action="stop" data-group-id="${escHtml(activeId)}" data-source="${escHtml(activeSource || 'airband')}" title="Stop : désactiver ce groupe Pulse (plus d'écoute jusqu'au clic sur son bouton)">⏹ Stop</button>`;
                summaryHtml += `<button type="button" class="pulse-manual-btn pulse-clear-btn" data-pulse-action="clear" data-group-id="${escHtml(activeId)}" data-source="${escHtml(activeSource || 'airband')}" title="Vider le tampon et revenir au direct">✕</button>`;
                summaryHtml += '</div>';
                summaryHtml += `<div class="pulse-active-meter pulse-fill-meter${fillMeta.atRisk ? ' pulse-fill-meter-risk' : ''}">`
                    + `<div class="pulse-fill-bar" role="progressbar" aria-valuenow="${fillMeta.pct}" aria-valuemin="0" aria-valuemax="100">`
                    + `<div class="pulse-fill-bar-inner" style="width:${fillMeta.pct}%"></div></div>`
                    + '<div class="pulse-fill-meter-meta">'
                    + `<span class="pulse-fill-time">${escHtml(formatPulseBufferSeconds(fillMeta.bufSec, maxSec))}</span>`
                    + `<span class="pulse-fill-pct">${fillMeta.pct} %</span>`
                    + '</div></div>';
                summaryHtml += '</div>';
            } else {
                summaryHtml += '▶ Aucun groupe actif (en attente de PCM ou fin de transmission) ';
            }
            if (manualHold) {
                summaryHtml += '<button type="button" class="pulse-manual-btn" data-pulse-action="auto" title="Reprendre la file automatique">↻ Auto</button> ';
            }
            if (forceOn) {
                summaryHtml += ` · ⚡ Bascule tampon ≥ ${Math.round(forceRatio * 100)} %`;
            }
            summaryEl.innerHTML = summaryHtml;
        } else {
            const rfsiText = activeSource === 'tetrapol' && activeId ? pulseRfsiForGroup(activeId) : '';
            const main = summaryEl.querySelector('.pulse-active-main');
            if (main) {
                let rfsiEl = main.querySelector('.pulse-active-rfsi');
                if (rfsiText) {
                    if (!rfsiEl) {
                        rfsiEl = document.createElement('div');
                        rfsiEl.className = 'pulse-active-rfsi';
                        rfsiEl.title = 'RFSI';
                        main.appendChild(rfsiEl);
                    }
                    if (rfsiEl.textContent !== rfsiText) rfsiEl.textContent = rfsiText;
                } else if (rfsiEl) {
                    rfsiEl.remove();
                }
            }
        }
        syncActiveMeter();
        syncArbiterLine();

        if (!listenGroups.length) {
            if (listEl.dataset.pulseEmpty !== '1') {
                listEl.dataset.pulseEmpty = '1';
                listEl.innerHTML = '<div class="pulse-fill-empty">Aucun groupe activé pour PulseAudio.</div>';
            }
            return;
        }
        listEl.dataset.pulseEmpty = '0';
        const emptyEl = listEl.querySelector('.pulse-fill-empty');
        if (emptyEl) emptyEl.remove();

        const wanted = new Set();
        listenGroups.forEach((g, i) => {
            const src = g.source || 'airband';
            const gid = String(g.group_id);
            wanted.add(src + ':' + gid);
            let row = listEl.querySelector(
                '.pulse-fill-row[data-source="' + cssEscapeAttr(src) + '"][data-group-id="'
                + cssEscapeAttr(gid) + '"]'
            );
            if (!row) {
                row = createPulseFillRow(g);
            }
            patchRowFill(row, g);
            const current = listEl.children[i];
            if (current !== row) listEl.insertBefore(row, current || null);
        });
        [...listEl.querySelectorAll('.pulse-fill-row')].forEach((row) => {
            const key = (row.dataset.source || '') + ':' + (row.dataset.groupId || '');
            if (!wanted.has(key)) row.remove();
        });
    }

    function wirePulseAdminControls() {
        const toggle = document.getElementById('pulseExclusivePlaybackToggle');
        const force = document.getElementById('pulseExclusiveForceBufferToggle');
        const catchup = document.getElementById('pulseCatchupFastToggle');
        const syncSettings = () => {
            lastPulseSig = '';
            apiPulseExclusiveBoth({
                exclusive: toggle ? toggle.checked : true,
                force_before_full: force ? force.checked : true,
                catchup_fast: catchup ? catchup.checked : false
            });
        };
        if (toggle && !toggle.dataset.bound) {
            toggle.dataset.bound = '1';
            toggle.addEventListener('change', syncSettings);
        }
        if (force && !force.dataset.bound) {
            force.dataset.bound = '1';
            force.addEventListener('change', syncSettings);
        }
        if (catchup && !catchup.dataset.bound) {
            catchup.dataset.bound = '1';
            catchup.addEventListener('change', syncSettings);
        }
        // Pas de redémarrage mixer Tetrapol côté Airband
        const restart = document.getElementById('restartMixerBtn');
        if (restart) restart.style.display = 'none';
        wireAdminReloadControls();
        checkAdminMode();
    }

    function checkAdminMode() {
        isAdminMode = isAdminPage || window.location.hash.includes('admin');
        const pulse = document.getElementById('adminPulseAudioSection');
        const reload = document.getElementById('adminReloadSection');
        if (pulse) pulse.style.display = isAdminMode ? 'block' : 'none';
        if (reload) reload.style.display = isAdminMode ? 'block' : 'none';
        if (isAdminMode) {
            lastPulseSig = '';
            updatePulseAdmin(pulseExclusiveState);
        }
    }

    /* ---- WS handlers ---- */

    function applyTetrapolFromBridge(tp) {
        if (!tp) return;
        if (el.tetrapolSection) {
            el.tetrapolSection.style.display = (tp.connected || tetrapolWsUrl) ? 'block' : el.tetrapolSection.style.display;
        }
        const tpTitle = document.getElementById('tetrapolGroupButtonsTitle');
        if (tpTitle && (tp.connected || tetrapolWsUrl)) tpTitle.style.display = '';
        tetrapolBridged = !!tp.connected;
        if (!tp.connected) {
            // Keep last-known channels/cells visible; mark cells KO (like tetrapol-kit timeout).
            tetrapolCells.forEach((c) => {
                if (c.syncStatus !== 'lost') c.syncStatus = 'unsynced';
            });
            markAllCellsUnsynced('disconnect');
        } else {
            const chObj = tp.channels || {};
            tetrapolChannels = chObj.channels || (Array.isArray(chObj) ? chObj : []);
            const grObj = tp.groups || {};
            const groups = grObj.groups || (Array.isArray(grObj) ? grObj : []);
            tetrapolGroups = new Map(groups.map(g => [String(g.group_id), Object.assign({}, g, {
                listen: g.listen !== false,
                encrypted: !!g.encrypted
            })]));
            tetrapolChannels.forEach(c => {
                if (c.group_id == null) return;
                const gid = String(c.group_id);
                if (!tetrapolGroups.has(gid)) {
                    tetrapolGroups.set(gid, {
                        group_id: gid,
                        group_label: c.group_label || gid,
                        on_air: !!c.on_air,
                        encrypted: !!c.encrypted,
                        listen: true
                    });
                } else if (c.encrypted) {
                    tetrapolGroups.get(gid).encrypted = true;
                }
            });
            syncTetrapolGroupsOnAirFromChannels();
            scanTetrapolClearGroups();
            const cellsRaw = tp.cells;
            const cellsList = Array.isArray(cellsRaw) ? cellsRaw
                : (cellsRaw && Array.isArray(cellsRaw.cells) ? cellsRaw.cells : []);
            if (cellsList.length) applyTetrapolCellsSnapshot(cellsList);
        }
        if (tp.pulse_exclusive) {
            tpPulseExclusiveState = tp.pulse_exclusive;
            applyTetrapolPulsePriorities(tp.pulse_exclusive);
            applyDisabledNetworksFromServer(tp.pulse_exclusive.disabled_networks);
        }
        lastTpSig = '';
        lastReceptionSig = '';
        rebuildCatalog();
        updateTetrapolListing();
        updateReceptionDisplay();
        setDot(el.tetrapolStatus, tetrapolBridged && (
            tetrapolChannels.some(c => c.on_air) || [...tetrapolGroups.values()].some(g => g.on_air)
        ));
        if (!tetrapolBridged && el.tetrapolStatus) {
            setDot(el.tetrapolStatus, false);
        }
    }

    function refreshExclusiveSegmentEntries() {
        exclusiveQueue.forEach((seg) => {
            const entry = catalog.get(seg.key);
            if (entry) seg.entry = entry;
            syncRingBufferTitleDisplay(seg);
        });
        if (exclusiveActive) {
            const entry = catalog.get(exclusiveActive.key);
            if (entry) exclusiveActive.entry = entry;
            syncRingBufferTitleDisplay(exclusiveActive);
        }
    }

    function stopAirbandStreamsOnly() {
        const keys = new Set();
        players.forEach((p, key) => {
            if ((p.entry && p.entry.source === 'airband') || String(key).startsWith('ab:')) keys.add(key);
        });
        exclusiveQueue.forEach((s) => {
            if ((s.entry && s.entry.source === 'airband') || String(s.key).startsWith('ab:')) keys.add(s.key);
        });
        if (exclusiveActive && ((exclusiveActive.entry && exclusiveActive.entry.source === 'airband')
            || String(exclusiveActive.key).startsWith('ab:'))) {
            keys.add(exclusiveActive.key);
        }
        keys.forEach((key) => {
            try { stopEntry(key); } catch (_) { /* ignore */ }
        });
    }

    function markAirbandOffline() {
        airbandBridged = false;
        stopAirbandStreamsOnly();
        const inactiveListing = [...airbandById.values()].map((ch) => ({
            ...ch,
            on_air: false,
            last_close_ms: ch.last_close_ms || Date.now()
        }));
        airbandById = new Map(inactiveListing.map((c) => [c.stream_id, c]));
        rebuildCatalog();
        // Keep last pulseExclusiveState so fill / listen lists stay until projet désactivé.
        if (pulseExclusiveState && Array.isArray(pulseExclusiveState.listen_groups)) {
            pulseExclusiveState = {
                ...pulseExclusiveState,
                active_group_id: null,
                listen_groups: pulseExclusiveState.listen_groups.map((g) => ({
                    ...g,
                    on_air: false,
                    is_active: false
                }))
            };
        }
        lastAirSig = '\0';
        lastAirBtnSig = '';
        lastReceptionSig = '';
        lastPulseSig = '';
        lastPulseFillSig = '';
        setDot(el.channelsStatus, false);
        updateAirbandTable(inactiveListing.length ? inactiveListing : []);
        updateAirbandButtons();
        updateReceptionDisplay();
        updatePulseAdmin();
    }

    function applyMonitorSourcesAndConfig(msg) {
        const wasAirband = airbandBridged;
        if (msg.sources) {
            if (msg.sources.airband != null) airbandBridged = !!msg.sources.airband;
            if (msg.sources.tetrapol != null) tetrapolBridged = !!msg.sources.tetrapol;
        }
        if (msg.config) {
            if (msg.config.icecast_public_base != null) icecastPublicBase = msg.config.icecast_public_base;
            if (msg.config.tetrapol_ws_url != null) tetrapolWsUrl = msg.config.tetrapol_ws_url;
            if (msg.config.tetrapol_bridged != null) tetrapolBridged = !!msg.config.tetrapol_bridged;
            if (msg.config.airband_bridged != null) airbandBridged = !!msg.config.airband_bridged;
            if (tetrapolWsUrl && el.tetrapolSection) el.tetrapolSection.style.display = 'block';
            const tpTitle = document.getElementById('tetrapolGroupButtonsTitle');
            if (tetrapolWsUrl && tpTitle) tpTitle.style.display = '';
            if (tetrapolWsUrl && el.networkButtonsTitle) el.networkButtonsTitle.style.display = '';
        }
        if (msg.NETWORKS) {
            applyNetworksPayload(msg.NETWORKS);
            updatePulseNetworkButtons();
            lastScopeSig = '';
            updateScopeButtons();
        }
        return wasAirband;
    }

    function applyPulseExclusiveFromMsg(msg) {
        if (msg.pulse_arbiter !== undefined) pulseArbiterState = msg.pulse_arbiter;
        if (airbandBridged && msg.pulse_exclusive) {
            const remote = msg.pulse_exclusive;
            if (Date.now() < pulseLocalEditUntil && pulseExclusiveState) {
                pulseExclusiveState = {
                    ...remote,
                    active_group_id: pulseExclusiveState.active_group_id,
                    manual_hold: pulseExclusiveState.manual_hold
                };
            } else {
                pulseExclusiveState = remote;
            }
        }
        // Hors ligne : ne pas vider listen_groups / fills — la liste reste jusqu'à
        // désactivation projet ou nouvelle télémétrie.
        if (msg.tetrapol && msg.tetrapol.pulse_exclusive) {
            const remoteTp = msg.tetrapol.pulse_exclusive;
            if (Date.now() < pulseLocalEditUntil && tpPulseExclusiveState) {
                tpPulseExclusiveState = {
                    ...remoteTp,
                    active_group_id: tpPulseExclusiveState.active_group_id,
                    manual_hold: tpPulseExclusiveState.manual_hold
                };
            } else {
                tpPulseExclusiveState = remoteTp;
            }
            applyDisabledNetworksFromServer(remoteTp.disabled_networks);
        }
    }

    function refreshMonitorConnectionStatus() {
        setDot(el.channelsStatus, airbandBridged && [...airbandById.values()].some(c => c.on_air));
        if (el.connectionStatus) {
            const ab = airbandBridged
                ? `Analogique OK (${airbandById.size})`
                : 'Analogique déconnecté';
            let tp = '';
            if (tetrapolWsUrl) {
                tp = tetrapolBridged ? ' · Tetrapol OK' : ' · Tetrapol déconnecté';
            }
            el.connectionStatus.textContent = ab + tp;
            el.connectionStatus.style.color = (!airbandBridged || (tetrapolWsUrl && !tetrapolBridged))
                ? '#c66'
                : '';
        }
    }

    function applyTetrapolChannelsPatch(patch) {
        if (!patch) return false;
        let list = tetrapolChannels.slice();
        const byId = new Map(list.map((c) => [String(c.channel_id), c]));
        let changed = false;
        if (Array.isArray(patch.remove)) {
            patch.remove.forEach((rawId) => {
                const id = String(rawId);
                if (byId.has(id)) {
                    byId.delete(id);
                    changed = true;
                }
            });
        }
        if (Array.isArray(patch.upsert)) {
            patch.upsert.forEach((item) => {
                if (!item || item.channel_id == null) return;
                byId.set(String(item.channel_id), item);
                changed = true;
            });
        }
        if (changed) tetrapolChannels = [...byId.values()];
        return changed;
    }

    function applyTetrapolGroupsPatch(patch) {
        if (!patch) return false;
        let changed = false;
        if (Array.isArray(patch.remove)) {
            patch.remove.forEach((rawId) => {
                const gid = String(rawId);
                if (tetrapolGroups.has(gid)) {
                    tetrapolGroups.delete(gid);
                    changed = true;
                }
            });
        }
        if (Array.isArray(patch.upsert)) {
            patch.upsert.forEach((g) => {
                if (!g || g.group_id == null) return;
                const gid = String(g.group_id);
                tetrapolGroups.set(gid, Object.assign({}, g, {
                    listen: g.listen !== false,
                    encrypted: !!g.encrypted
                }));
                changed = true;
            });
        }
        return changed;
    }

    function applyTetrapolCellsPatch(patch) {
        if (!patch) return false;
        let changed = false;
        if (Array.isArray(patch.remove)) {
            patch.remove.forEach((rawId) => {
                const channelId = Number(rawId);
                if (tetrapolCells.has(channelId)) {
                    tetrapolCells.delete(channelId);
                    changed = true;
                }
            });
        }
        if (Array.isArray(patch.upsert) && patch.upsert.length) {
            applyTetrapolCellsSnapshot(patch.upsert);
            changed = true;
        }
        return changed;
    }

    function applyTetrapolPatch(tp) {
        if (!tp) return false;
        let changed = false;
        if (tp.connected != null) {
            const next = !!tp.connected;
            if (next !== tetrapolBridged) {
                tetrapolBridged = next;
                changed = true;
            }
            if (el.tetrapolSection) {
                el.tetrapolSection.style.display = (tetrapolBridged || tetrapolWsUrl)
                    ? 'block'
                    : el.tetrapolSection.style.display;
            }
            const tpTitle = document.getElementById('tetrapolGroupButtonsTitle');
            if (tpTitle && (tetrapolBridged || tetrapolWsUrl)) tpTitle.style.display = '';
            if (!tetrapolBridged) {
                // Keep last-known channel list; only force cells KO.
                tetrapolCells.forEach((c) => {
                    if (c.syncStatus !== 'lost') c.syncStatus = 'unsynced';
                });
                markAllCellsUnsynced('disconnect');
                changed = true;
            }
        }
        if (tetrapolBridged) {
            if (applyTetrapolChannelsPatch(tp.channels)) changed = true;
            if (applyTetrapolGroupsPatch(tp.groups)) changed = true;
            if (applyTetrapolCellsPatch(tp.cells)) changed = true;
            // Ensure groups exist for any channel group_id (same as full apply).
            tetrapolChannels.forEach((c) => {
                if (c.group_id == null) return;
                const gid = String(c.group_id);
                if (!tetrapolGroups.has(gid)) {
                    tetrapolGroups.set(gid, {
                        group_id: gid,
                        group_label: c.group_label || gid,
                        on_air: !!c.on_air,
                        encrypted: !!c.encrypted,
                        listen: true
                    });
                    changed = true;
                } else if (c.encrypted) {
                    tetrapolGroups.get(gid).encrypted = true;
                }
            });
            if (syncTetrapolGroupsOnAirFromChannels()) changed = true;
            scanTetrapolClearGroups();
        }
        if (tp.pulse_exclusive) {
            // Pulse fills are applied via applyPulseExclusiveFromMsg — do not
            // treat them as listing changes (would force tetrapol button redraws).
        }
        if (changed) {
            lastTpSig = '';
            lastReceptionSig = '';
            rebuildCatalog();
            updateTetrapolListing();
            updateReceptionDisplay();
            setDot(el.tetrapolStatus, tetrapolBridged && (
                tetrapolChannels.some(c => c.on_air)
                || [...tetrapolGroups.values()].some(g => g.on_air)
            ));
            if (!tetrapolBridged && el.tetrapolStatus) {
                setDot(el.tetrapolStatus, false);
            }
        }
        return changed;
    }

    function onMonitorUpdate(msg) {
        const wasAirband = applyMonitorSourcesAndConfig(msg);

        if (wasAirband && !airbandBridged) {
            markAirbandOffline();
        } else if (airbandBridged) {
            const channels = (msg.channels && msg.channels.channels) || [];
            airbandById = new Map(channels.map(c => [c.stream_id, c]));
            updateAirbandTable(channels);
        }

        if (msg.tetrapol) applyTetrapolFromBridge(msg.tetrapol);
        rebuildCatalog();
        refreshExclusiveSegmentEntries();
        updateScopeButtons();
        updateAirbandButtons();
        updateReceptionDisplay();
        syncPlayersFromCatalog();
        applyPulseExclusiveFromMsg(msg);
        updatePulseAdmin();
        refreshMonitorConnectionStatus();
    }

    /** Incremental bridge patch — merge entity upserts/removes into local state. */
    function onMonitorPatch(msg) {
        const wasAirband = applyMonitorSourcesAndConfig(msg);
        let airChanged = false;
        let tpChanged = false;

        if (wasAirband && !airbandBridged) {
            markAirbandOffline();
            airChanged = true;
        } else if (airbandBridged && msg.channels) {
            if (Array.isArray(msg.channels.remove)) {
                msg.channels.remove.forEach((rawId) => {
                    if (airbandById.has(rawId)) {
                        airbandById.delete(rawId);
                        airChanged = true;
                    } else {
                        const n = Number(rawId);
                        if (Number.isFinite(n) && airbandById.has(n)) {
                            airbandById.delete(n);
                            airChanged = true;
                        } else if (airbandById.has(String(rawId))) {
                            airbandById.delete(String(rawId));
                            airChanged = true;
                        }
                    }
                });
            }
            if (Array.isArray(msg.channels.upsert)) {
                msg.channels.upsert.forEach((item) => {
                    if (!item || item.stream_id == null) return;
                    airbandById.set(item.stream_id, item);
                    airChanged = true;
                });
            }
            if (airChanged) {
                updateAirbandTable([...airbandById.values()]);
            }
        }

        if (msg.tetrapol) tpChanged = !!applyTetrapolPatch(msg.tetrapol);

        const listingChanged = airChanged || tpChanged || !!msg.groups;
        const pulseChanged = msg.pulse_exclusive != null
            || msg.pulse_arbiter !== undefined
            || !!(msg.tetrapol && msg.tetrapol.pulse_exclusive != null);

        if (listingChanged) {
            rebuildCatalog();
            refreshExclusiveSegmentEntries();
            updateAirbandButtons();
            updateReceptionDisplay();
            syncPlayersFromCatalog();
        }
        if (msg.NETWORKS) {
            applyNetworksPayload(msg.NETWORKS);
            lastScopeSig = '';
            updatePulseNetworkButtons();
        }
        updateScopeButtons();
        if (pulseChanged) {
            applyPulseExclusiveFromMsg(msg);
            if (listingChanged) refreshExclusiveSegmentEntries();
        }
        if (pulseChanged || listingChanged || msg.NETWORKS) {
            updatePulseAdmin();
            refreshMonitorConnectionStatus();
        } else if (msg.sources || msg.config) {
            refreshMonitorConnectionStatus();
        }
    }

    function connectLocal() {
        if (ws && (ws.readyState === WebSocket.OPEN || ws.readyState === WebSocket.CONNECTING)) return;
        ws = new WebSocket(WS_URL);
        ws.onopen = () => {
            wsRetries = 0;
            // Local bridge is up; upstream Airband/Tetrapol status comes from updates.
            if (el.connectionStatus) el.connectionStatus.textContent = 'Bridge OK — attente sources…';
        };
        ws.onclose = () => {
            markAirbandOffline();
            tetrapolBridged = false;
            if (el.tetrapolStatus) setDot(el.tetrapolStatus, false);
            if (el.connectionStatus) {
                el.connectionStatus.textContent = 'Monitor déconnecté';
                el.connectionStatus.style.color = '#c66';
            }
            if (wsRetries < 10) {
                wsRetries++;
                setTimeout(connectLocal, 1000 * wsRetries);
            }
        };
        ws.onerror = (err) => {
            console.warn('WS error', err);
        };
        ws.onmessage = (ev) => {
            try {
                const msg = JSON.parse(ev.data);
                if (msg.type === 'update') {
                    onMonitorUpdate(msg);
                    return;
                }
                if (msg.type === 'patch') {
                    onMonitorPatch(msg);
                    return;
                }
                if (msg.event && handleTetrapolCellEvent(msg))
                    return;
            } catch (e) {
                console.warn('WS JSON parse failed', e);
            }
        };
    }

    function wireCollapsible() {
        document.querySelectorAll('.section-toggle').forEach(toggle => {
            toggle.addEventListener('click', () => {
                const expanded = toggle.getAttribute('aria-expanded') !== 'false';
                toggle.setAttribute('aria-expanded', expanded ? 'false' : 'true');
                const body = document.getElementById(toggle.getAttribute('aria-controls'));
                if (body) body.style.display = expanded ? 'none' : '';
            });
        });
    }

    function wireAudioControls() {
        if (el.activateAudio) {
            el.activateAudio.addEventListener('click', () => {
                audioEnabled = true;
                el.activateAudio.style.display = 'none';
                const buttonSection = el.activateAudio.closest('.section')
                    || document.getElementById('activateAudioSection');
                if (buttonSection) buttonSection.style.display = 'none';
                lastAirBtnSig = '';
                lastTpSig = '';
                lastScopeSig = '';
                updateScopeButtons();
                updateAirbandButtons();
                updateTetrapolListing();
                ensureStreamsSection();
                syncPlayersFromCatalog();
            });
        }
        if (!mseMpegSupported()) {
            exclusivePlaybackMode = false;
            if (el.exclusivePlaybackControl) el.exclusivePlaybackControl.style.display = 'none';
            if (el.exclusiveForceControl) el.exclusiveForceControl.style.display = 'none';
        } else if (el.exclusiveToggle) {
            el.exclusiveToggle.checked = exclusivePlaybackMode;
            if (el.exclusiveForceControl) el.exclusiveForceControl.style.display = exclusivePlaybackMode ? '' : 'none';
            el.exclusiveToggle.addEventListener('change', () => {
                exclusivePlaybackMode = el.exclusiveToggle.checked;
                localStorage.setItem('exclusivePlaybackMode', exclusivePlaybackMode);
                if (el.exclusiveForceControl) el.exclusiveForceControl.style.display = exclusivePlaybackMode ? '' : 'none';
                [...players.keys()].forEach(stopEntry);
                exclusiveQueue.forEach(cleanupExclusive);
                exclusiveQueue = [];
                exclusiveActive = null;
                if (audioEnabled) syncPlayersFromCatalog();
            });
        }
        if (el.exclusiveForceToggle) {
            el.exclusiveForceToggle.checked = forceBeforeFull;
            el.exclusiveForceToggle.addEventListener('change', () => {
                forceBeforeFull = el.exclusiveForceToggle.checked;
                localStorage.setItem('exclusiveForceBeforeBufferFull', forceBeforeFull);
            });
        }
        if (el.volumeSlider) {
            el.volumeSlider.value = globalVolume;
            if (el.volumeValue) el.volumeValue.textContent = globalVolume + '%';
            el.volumeSlider.addEventListener('input', () => {
                globalVolume = parseInt(el.volumeSlider.value, 10);
                localStorage.setItem('globalVolume', String(globalVolume));
                if (el.volumeValue) el.volumeValue.textContent = globalVolume + '%';
                players.forEach(p => { applyAudioOutput(getAudio(p)); });
                const allEx = [...exclusiveQueue];
                if (exclusiveActive) allEx.push(exclusiveActive);
                allEx.forEach((seg) => {
                    applyAudioOutput(seg.queueAudioEl);
                    if (seg.playbackPlayer?.audioElement
                        && seg.playbackPlayer.audioElement !== seg.queueAudioEl) {
                        applyAudioOutput(seg.playbackPlayer.audioElement);
                    }
                });
            });
        }
        if (el.playbackRateFastToggle) {
            el.playbackRateFastToggle.checked = playbackRateFast;
            el.playbackRateFastToggle.addEventListener('change', () => {
                playbackRateFast = !!el.playbackRateFastToggle.checked;
                localStorage.setItem('playbackRateFast', playbackRateFast ? 'true' : 'false');
                applyPlaybackRateToAll();
            });
        }
    }

    async function seedNetworksFromLabels() {
        try {
            const r = await fetch('/labels.json', { cache: 'no-store' });
            if (!r.ok) return;
            const data = await r.json();
            if (!data || !data.NETWORKS) return;
            const nets = Object.keys(data.NETWORKS).map((id) => ({
                network_id: id,
                network_label: data.NETWORKS[id]
            }));
            applyNetworksPayload({ networks: nets });
            updatePulseNetworkButtons();
            lastScopeSig = '';
            updateScopeButtons();
        } catch (e) {
            console.warn('labels NETWORKS seed failed', e);
        }
    }

    async function loadConfig() {
        try {
            // Monitor-side public Icecast base (preferred) — then Airband config fallback.
            try {
                const mr = await fetch('/api/monitor/config');
                const mcfg = await mr.json();
                if (mcfg && mcfg.icecast_public_base) {
                    icecastPublicBase = String(mcfg.icecast_public_base);
                }
                window.__ICECAST_BROWSER_DIRECT__ = !!(mcfg && mcfg.icecast_browser_direct);
            } catch (_) { /* optional */ }

            const r = await fetch('/api/airband/config');
            const cfg = await r.json();
            if (!icecastPublicBase) icecastPublicBase = cfg.icecast_public_base || '';
            tetrapolWsUrl = cfg.tetrapol_ws_url || '';
            tetrapolBridged = !!cfg.tetrapol_bridged;
            if (tetrapolWsUrl && el.tetrapolSection) el.tetrapolSection.style.display = 'block';
            const tpTitle = document.getElementById('tetrapolGroupButtonsTitle');
            if (tetrapolWsUrl && tpTitle) tpTitle.style.display = '';
            await seedNetworksFromLabels();
        } catch (e) {
            console.warn('config fetch failed', e);
        }
    }

    function loadIcecastLib(cb) {
        if (typeof IcecastMetadataPlayer !== 'undefined' || isAdminPage) return cb();
        const s = document.createElement('script');
        s.src = 'https://cdn.jsdelivr.net/npm/icecast-metadata-player@1.17.13/build/icecast-metadata-player-1.17.13.main.min.js';
        s.onload = cb;
        s.onerror = cb;
        document.head.appendChild(s);
    }

    document.addEventListener('DOMContentLoaded', () => {
        wireCollapsible();
        wireAudioControls();
        setInterval(checkCellInfoTimeouts, 5000);
        loadDepartments();
        if (window.AdminPanel) {
            AdminPanel.mount().then(() => wirePulseAdminControls()).catch(() => {});
        }
        document.addEventListener('admin-panel-ready', () => {
            wirePulseAdminControls();
            wireAdminReloadControls();
            updatePulseNetworkButtons();
        });
        window.addEventListener('hashchange', () => {
            checkAdminMode();
            refreshDebugMode();
        });
        checkAdminMode();
        refreshDebugMode();
        loadConfig().then(() => {
            updateTetrapolListing();
            syncDisabledNetworksToMixer();
            loadIcecastLib(() => connectLocal());
        });
    });
})();
