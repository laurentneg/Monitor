/**
 * Filtre de logs console pour le moniteur Tetrapol.
 *
 * Syntaxe URL (fragments multiples) :
 *   #LOG_ERROR#LOG_PROMOT#LOG_REPLAY#LOG_RECORD#LOG_WS
 *   #LOG_DEBUG  (niveau debug)
 *   #LOG_0 … #LOG_3  (alias niveaux)
 *   #LOG_ALL
 *
 * Défaut (aucun #LOG_*) : ERROR + catégories PROMOT + REPLAY.
 */
(function (global) {
    'use strict';

    const LEVEL_ERROR = 0;
    const LEVEL_WARN = 1;
    const LEVEL_INFO = 2;
    const LEVEL_DEBUG = 3;

    const LEVEL_NAMES = ['ERROR', 'WARN', 'INFO', 'DEBUG'];

    const LEVEL_TOKENS = {
        LOG_ERROR: LEVEL_ERROR,
        LOG_WARN: LEVEL_WARN,
        LOG_INFO: LEVEL_INFO,
        LOG_DEBUG: LEVEL_DEBUG,
        LOG_0: LEVEL_ERROR,
        LOG_1: LEVEL_WARN,
        LOG_2: LEVEL_INFO,
        LOG_3: LEVEL_DEBUG
    };

    const CATEGORY_TOKENS = new Set([
        'LOG_PROMOT',
        'LOG_REPLAY',
        'LOG_RECORD',
        'LOG_WS',
        'LOG_QUEUE',
        'LOG_EVENT',
        'LOG_ICECAST',
        'LOG_SYS',
        'LOG_ALL'
    ]);

    const CATEGORY_ALIASES = {
        promot: 'PROMOT',
        promotion: 'PROMOT',
        replay: 'REPLAY',
        record: 'RECORD',
        ws: 'WS',
        websocket: 'WS',
        queue: 'QUEUE',
        event: 'EVENT',
        icecast: 'ICECAST',
        sys: 'SYS',
        error: 'ERROR'
    };

    const CATEGORY_STYLES = {
        PROMOT: 'background:#e65100;color:#fff;padding:2px 6px;border-radius:3px;font-weight:bold',
        REPLAY: 'background:#1565c0;color:#fff;padding:2px 6px;border-radius:3px;font-weight:bold',
        RECORD: 'background:#6a1b9a;color:#fff;padding:2px 6px;border-radius:3px;font-weight:bold',
        WS: 'background:#283593;color:#fff;padding:2px 6px;border-radius:3px;font-weight:bold',
        QUEUE: 'background:#2e7d32;color:#fff;padding:2px 6px;border-radius:3px;font-weight:bold',
        EVENT: 'background:#455a64;color:#fff;padding:2px 6px;border-radius:3px;font-weight:bold',
        ICECAST: 'background:#00838f;color:#fff;padding:2px 6px;border-radius:3px;font-weight:bold',
        SYS: 'background:#5d4037;color:#fff;padding:2px 6px;border-radius:3px;font-weight:bold',
        ERROR: 'background:#c62828;color:#fff;padding:2px 6px;border-radius:3px;font-weight:bold'
    };

    const DEFAULT_CATEGORIES = new Set(['PROMOT', 'REPLAY']);

    function parseHashTokens() {
        const hash = (global.location && global.location.hash) || '';
        const tokens = new Set();
        hash.split('#').forEach((part) => {
            const t = part.trim().toUpperCase();
            if (t.startsWith('LOG_')) {
                tokens.add(t);
            } else if (t === 'DEBUG') {
                // #debug → logs StreamTitle + niveau debug (ICECAST/QUEUE)
                tokens.add('LOG_DEBUG');
                tokens.add('LOG_ICECAST');
                tokens.add('LOG_QUEUE');
                try { global.__MONITOR_DEBUG__ = true; } catch (_) { /* ignore */ }
            }
        });
        return tokens;
    }

    function buildConfig() {
        const tokens = parseHashTokens();
        const hasLogTokens = tokens.size > 0;

        const config = {
            hasLogTokens,
            maxLevel: null,
            categories: new Set(),
            allCategories: false,
            useDefault: !hasLogTokens,
            label: ''
        };

        if (!hasLogTokens) {
            config.categories = new Set(DEFAULT_CATEGORIES);
            config.label = 'ERROR + PROMOT + REPLAY (défaut)';
            return config;
        }

        let maxLevel = null;
        tokens.forEach((token) => {
            if (token === 'LOG_ALL') {
                config.allCategories = true;
                maxLevel = LEVEL_DEBUG;
            } else if (LEVEL_TOKENS[token] != null) {
                const lv = LEVEL_TOKENS[token];
                maxLevel = maxLevel == null ? lv : Math.max(maxLevel, lv);
            } else if (CATEGORY_TOKENS.has(token)) {
                config.categories.add(token.replace(/^LOG_/, ''));
            }
        });

        config.maxLevel = maxLevel;

        const parts = [];
        if (config.allCategories) {
            parts.push('ALL');
        } else if (config.categories.size) {
            parts.push([...config.categories].sort().join(' + '));
        }
        if (maxLevel != null) {
            parts.push(`≤ ${LEVEL_NAMES[maxLevel]}`);
        }
        if (!config.allCategories && !config.categories.size && maxLevel == null) {
            config.categories = new Set(DEFAULT_CATEGORIES);
            parts.push('PROMOT + REPLAY (défaut)');
        }
        parts.unshift('ERROR toujours');
        config.label = parts.join(' | ');

        return config;
    }

    function normalizeCategory(cat) {
        const key = String(cat || 'sys').toLowerCase();
        return CATEGORY_ALIASES[key] || key.toUpperCase();
    }

    function shouldShow(level, category, config) {
        const cat = normalizeCategory(category);
        const isError = level === LEVEL_ERROR;

        if (config.useDefault) {
            if (isError) return true;
            return DEFAULT_CATEGORIES.has(cat);
        }

        if (isError) return true;

        let categoryOk = false;
        if (config.allCategories) {
            categoryOk = true;
        } else if (config.categories.size > 0) {
            categoryOk = config.categories.has(cat);
        } else {
            categoryOk = true;
        }

        let levelOk = true;
        if (config.maxLevel != null) {
            levelOk = level <= config.maxLevel;
        }

        if (config.categories.size > 0 || config.allCategories) {
            if (config.maxLevel != null) {
                return categoryOk && levelOk;
            }
            return categoryOk;
        }

        if (config.maxLevel != null) {
            return levelOk;
        }

        return DEFAULT_CATEGORIES.has(cat);
    }

    function badgeStyle(category) {
        return CATEGORY_STYLES[category] || CATEGORY_STYLES.SYS;
    }

    function emit(level, category, message, args) {
        const config = MonitorLog._config;
        const cat = normalizeCategory(category);
        if (!shouldShow(level, cat, config)) {
            return;
        }

        const badge = `[${cat}]`;
        const badgeCss = badgeStyle(cat);
        const textCss = 'color:inherit';
        const fn = level === LEVEL_ERROR
            ? console.error
            : (level === LEVEL_WARN ? console.warn : console.log);

        if (typeof message === 'string' && message.includes('\n')) {
            fn(`%c${badge}%c ${message.split('\n')[0]}…`, badgeCss, textCss, ...args);
            message.split('\n').slice(1).forEach((line) => {
                if (line.trim()) fn(`  ${line}`);
            });
        } else {
            fn(`%c${badge}%c`, badgeCss, textCss, message, ...args);
        }
    }

    const MonitorLog = {
        _config: buildConfig(),
        LEVEL_ERROR,
        LEVEL_WARN,
        LEVEL_INFO,
        LEVEL_DEBUG,

        refresh() {
            this._config = buildConfig();
            this._updateBanner();
            return this._config;
        },

        getConfigLabel() {
            return this._config.label;
        },

        hasExplicitLogConfig() {
            return this._config.hasLogTokens;
        },

        _updateBanner() {
            const id = 'logFilterBanner';
            let banner = global.document && global.document.getElementById(id);
            if (!this._config.hasLogTokens) {
                if (banner) banner.style.display = 'none';
                return;
            }
            if (!banner && global.document) {
                banner = global.document.createElement('div');
                banner.id = id;
                banner.className = 'log-filter-banner';
                global.document.body.insertBefore(banner, global.document.body.firstChild);
            }
            if (banner) {
                banner.textContent = `Logs filtrés : ${this._config.label}`;
                banner.style.display = 'block';
            }
        },

        shouldShow(level, category) {
            return shouldShow(level, normalizeCategory(category), this._config);
        },

        error(category, message, ...args) {
            emit(LEVEL_ERROR, category, message, args);
        },

        warn(category, message, ...args) {
            emit(LEVEL_WARN, category, message, args);
        },

        info(category, message, ...args) {
            emit(LEVEL_INFO, category, message, args);
        },

        debug(category, message, ...args) {
            emit(LEVEL_DEBUG, category, message, args);
        },

        promot(message, ...args) {
            this.info('promot', message, ...args);
        },

        replay(message, ...args) {
            this.info('replay', message, ...args);
        },

        record(message, ...args) {
            this.info('record', message, ...args);
        },

        ws(message, ...args) {
            this.info('ws', message, ...args);
        },

        queue(message, ...args) {
            this.info('queue', message, ...args);
        },

        event(message, ...args) {
            this.info('event', message, ...args);
        },

        icecast(message, ...args) {
            this.info('icecast', message, ...args);
        },

        sys(message, ...args) {
            this.info('sys', message, ...args);
        },

        groupCollapsed(category, title, fn) {
            const cat = normalizeCategory(category);
            if (!shouldShow(LEVEL_INFO, cat, this._config)) {
                return;
            }
            const badge = `[${cat}]`;
            const badgeCss = badgeStyle(cat);
            console.groupCollapsed(`%c${badge}%c ${title}`, badgeCss, 'color:inherit');
            try {
                fn();
            } finally {
                console.groupEnd();
            }
        }
    };

    global.MonitorLog = MonitorLog;

    if (global.addEventListener) {
        global.addEventListener('hashchange', () => {
            MonitorLog.refresh();
            MonitorLog.debug('sys', 'Configuration logs mise à jour:', MonitorLog.getConfigLabel());
        });
        global.addEventListener('DOMContentLoaded', () => MonitorLog._updateBanner());
    }

    MonitorLog._updateBanner();
})(typeof window !== 'undefined' ? window : globalThis);
