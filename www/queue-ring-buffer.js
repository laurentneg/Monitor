/**
 * Pipeline buffer circulaire Icecast (POC TestIcecastRingBuffer).
 * Ingestion fetch + IcecastMetadataReader, lecture MSE depuis le ring buffer uniquement.
 *
 * PAS de Blob audio / MediaRecorder / Worker(Blob).
 * createObjectURL n'est utilisé que pour attacher un MediaSource (exigence navigateur),
 * jamais pour un enregistrement figé — le rattrapage reste dans le ring + seek MSE soft.
 */
import { IcecastMetadataReader } from 'https://esm.sh/icecast-metadata-js@1.2.9';

export const QUEUE_BUFFER_CONFIG = {
    bufferDurationSec: 120,
    // Prébuffer court : démarrer la voix rapidement (évite d'attendre trop longtemps).
    minBufferBeforePlaySec: 0.15,
    // Après StreamTitle vide : encore blankMarginSec d'audio liés à l'ancien titre.
    blankMarginSec: 0.7,
    // Puis blankStoreSec de blanc encore écrit dans le ring, ensuite plus de stockage.
    blankStoreSec: 2.0,
    // À la reprise : dernière seconde (preRoll) écrite sous le nouveau StreamTitle.
    resumePreRollSec: 1.0,
    blankHoldSec: 7.0,
    fallbackBitrate: 64000,
    minValidMetaInt: 1,
    feedLoopIntervalMs: 100,
    targetLeadSec: 10,
    sourceBufferCleanupBehindSec: 30,
    reconnectBaseDelayMs: 400,
    reconnectMaxDelayMs: 4000,
    // Garde-fou: si aucun StreamTitle n'arrive, ne pas bloquer la lecture indéfiniment.
    firstTitleWaitMaxMs: 3500,
    // Seuil d'affichage « au direct » (label carte, bouton Direct).
    liveWaitingThresholdSec: 2.5,
    // Seuil strict pour fin de comm / compte à rebours (évite coupure en rattrapage).
    liveEndThresholdSec: 0.5,
};

export class AudioRingBuffer {
    constructor(maxDurationSec, bitrate, config = QUEUE_BUFFER_CONFIG) {
        this.config = config;
        this.maxDurationSec = maxDurationSec;
        this.bitrate = bitrate;
        this.maxBytes = Math.floor((bitrate / 8) * maxDurationSec);
        this.chunks = [];
        this.titleChanges = [];
        this.lastTitle = null;
        this.totalBytesWritten = 0;
        this.readByteOffset = 0;
        this.inBlank = false;
        this.blankTailRemaining = 0;
        this.preRoll = [];
        this.preRollBytes = 0;
        this._brSample = null;
        this._brLocked = false;
        this.droppedBlankBytes = 0;
        this._playbackPaused = false;
        /** Tant que true : aucun audio stocké tant qu'aucun StreamTitle non vide. */
        this.awaitingFirstTitle = false;
        this.awaitingFirstTitleSinceMs = 0;
        this.blankStartedAtMs = 0;
        this.blankStartByte = 0;
        this.currentBlankDroppedBytes = 0;
        /** StreamTitle vide : encore ~blankMarginSec enregistrés sous l'ancien titre. */
        this.blankGraceActive = false;
        this.blankGraceStartedAtMs = 0;
        this.blankGraceStartByte = 0;
        /** Tetrapol : false si on_air ↓ — les titres ICY collés ne relancent pas la com. */
        this._liveSpeech = true;
        /** Titre ICY arrivé juste avant on_air ↑ (course live-speech-off). */
        this._heldTitleWhileSpeechOff = null;
        /** Dernier titre non vide avant un blanc (détection ICY collé pendant la grâce). */
        this._titleBeforeBlank = '';
    }

    setPlaybackPaused(paused) {
        this._playbackPaused = !!paused;
    }

    /**
     * Tetrapol : _liveSpeech suit le listing on_air pour ignorer les titres collés.
     * Ne JAMAIS injecter un StreamTitle vide ici — seul Icecast peut blanchir le ring
     * (sinon on corrompt titleChanges alors que le mount a encore un talker).
     */
    setLiveSpeech(on) {
        const next = !!on;
        if (this._liveSpeech === next) return;
        this._liveSpeech = next;
        if (next) {
            this._resumeFromLiveSpeech();
            const held = this._heldTitleWhileSpeechOff;
            this._heldTitleWhileSpeechOff = null;
            if (held && held.title && (performance.now() - held.at) < 1500) {
                this.addTitleChange({ StreamTitle: held.title });
            }
        }
    }

    get liveSpeech() {
        return !!this._liveSpeech;
    }

    /**
     * on_air ↑ : recommencer à stocker tout de suite, sans attendre un StreamTitle.
     * Marqueur liveResume vide = parole sans label (pas l'ancien talker).
     * Le StreamTitle ICY remplira le marqueur ; tant qu'il n'arrive pas → rien.
     */
    _resumeFromLiveSpeech() {
        if (this.awaitingFirstTitle) return false;
        if (this.blankGraceActive) {
            this._cancelBlankGrace();
            this.lastTitle = '';
            this.titleChanges.push({
                byteOffset: this.totalBytesWritten,
                title: '',
                liveResume: true,
                droppedBlankSec: 0,
                blankWallMs: 0,
            });
            this._pruneTitles();
            return true;
        }
        if (!this.inBlank) return false;
        this._resumeFromBlank('');
        return true;
    }

    setBitrate(bitrate) {
        this.bitrate = bitrate;
        this.maxBytes = Math.floor((bitrate / 8) * this.maxDurationSec);
        this._prune();
    }

    get oldestByteOffset() {
        return this.chunks.length > 0 ? this.chunks[0].startByte : this.totalBytesWritten;
    }

    get bytesInBuffer() {
        return this.totalBytesWritten - this.oldestByteOffset;
    }

    get bufferedSeconds() {
        return this.bytesInBuffer / (this.bitrate / 8);
    }

    get unreadBytes() {
        return Math.max(0, this.totalBytesWritten - this.readByteOffset);
    }

    get unreadSeconds() {
        return this.unreadBytes / (this.bitrate / 8);
    }

    get fillRatio() {
        return Math.min(1, this.bytesInBuffer / this.maxBytes);
    }

    append(data) {
        const chunk = data instanceof Uint8Array ? data : new Uint8Array(data);
        if (chunk.length === 0) return;
        if (this.awaitingFirstTitle) {
            const waitMs = performance.now() - (this.awaitingFirstTitleSinceMs || 0);
            if (waitMs >= (this.config.firstTitleWaitMaxMs || 3500)) {
                // Pas de StreamTitle exploitable : rester en blanc (ne pas stocker
                // le silence Icecast comme une com — sinon la carte reste « active »).
                this.awaitingFirstTitle = false;
                this.awaitingFirstTitleSinceMs = 0;
                this.lastTitle = '';
                this.inBlank = true;
                this.blankTailRemaining = 0;
                if (!this.blankStartedAtMs) {
                    this.blankStartedAtMs = performance.now();
                    this.blankStartByte = this.totalBytesWritten;
                    this.currentBlankDroppedBytes = 0;
                }
                this._handleBlankAudio(chunk);
                return;
            }
            this.droppedBlankBytes += chunk.length;
            return;
        }
        if (this.inBlank) {
            this._handleBlankAudio(chunk);
            return;
        }
        this._store(chunk);
        // Après blankMarginSec d'audio encore sous l'ancien titre : confirmer le blanc.
        if (this.blankGraceActive) this._maybeConfirmBlankGrace();
    }

    _store(chunk) {
        if (this._playbackPaused) {
            const unread = this.totalBytesWritten - this.readByteOffset;
            if (unread + chunk.length > this.maxBytes) {
                return;
            }
        }
        this.chunks.push({ data: chunk, startByte: this.totalBytesWritten });
        this.totalBytesWritten += chunk.length;
        this._updateBitrateEstimate();
        this._prune();
    }

    _dropBlankChunk(chunk) {
        if (!chunk || chunk.length === 0) return;
        this.droppedBlankBytes += chunk.length;
        this.currentBlankDroppedBytes += chunk.length;
        this._pushPreRoll(chunk);
    }

    _handleBlankAudio(chunk) {
        // Carte active : continuer à stocker jusqu'à la fin du hold 7 s
        // (sinon la lecture s'arrête pendant « En attente »).
        if (!this._playbackPaused) {
            this._store(chunk);
            this._updateOpenBlankMarker();
            return;
        }
        // File : encore blankStoreSec écrits, puis drop + preRoll 1 s.
        if (this.blankTailRemaining > 0) {
            const take = Math.min(chunk.length, this.blankTailRemaining);
            const before = this.totalBytesWritten;
            this._store(chunk.subarray(0, take));
            const stored = this.totalBytesWritten - before;
            this.blankTailRemaining = Math.max(0, this.blankTailRemaining - stored);
            if (stored < take) {
                this.blankTailRemaining = 0;
                this._dropBlankChunk(chunk.subarray(stored));
            } else if (take < chunk.length) {
                this._dropBlankChunk(chunk.subarray(take));
            }
            this._updateOpenBlankMarker();
            return;
        }
        this._dropBlankChunk(chunk);
        this._updateOpenBlankMarker();
    }

    /**
     * Confirme le blanc après blankMarginSec (~0,7 s) d'audio encore attribués
     * à l'ancien StreamTitle. Le marqueur vide est ancré en fin de fenêtre
     * (pas au début) pour que ces octets restent sous le talker précédent.
     */
    _maybeConfirmBlankGrace() {
        if (!this.blankGraceActive || this.inBlank) return;
        const marginSec = this.config.blankMarginSec || 0.7;
        const marginBytes = Math.floor((this.bitrate / 8) * marginSec);
        const heldBytes = Math.max(0, this.totalBytesWritten - this.blankGraceStartByte);
        const elapsedSec = (performance.now() - this.blankGraceStartedAtMs) / 1000;
        // Audio tenu OU horloge (flux figé) : ne pas rester en grâce indéfiniment.
        if (heldBytes < marginBytes && elapsedSec < marginSec) return;

        const blankAt = this.totalBytesWritten;

        this.blankGraceActive = false;
        this.inBlank = true;
        this.lastTitle = '';
        this.blankTailRemaining = this._blankStoreBytes();
        this.blankStartedAtMs = performance.now();
        this.blankStartByte = blankAt;
        this.currentBlankDroppedBytes = 0;

        this.titleChanges.push({
            byteOffset: blankAt,
            title: '',
            droppedBlankSec: 0,
            blankWallMs: 0,
            titleHoldSec: Math.min(marginSec, Math.max(elapsedSec, heldBytes / Math.max(1, this.bitrate / 8))),
        });
        this.titleChanges.sort((a, b) => a.byteOffset - b.byteOffset);
        this._pruneTitles();
    }

    _cancelBlankGrace() {
        this.blankGraceActive = false;
        this.blankGraceStartedAtMs = 0;
        this.blankGraceStartByte = 0;
    }

    /** À appeler périodiquement (UI tick) si le flux n'envoie plus de chunks. */
    pollBlankGrace() {
        if (this.blankGraceActive) this._maybeConfirmBlankGrace();
    }

    _pushPreRoll(chunk) {
        this.preRoll.push(chunk);
        this.preRollBytes += chunk.length;
        const max = this._marginBytes();
        while (this.preRoll.length > 1 && this.preRollBytes - this.preRoll[0].length >= max) {
            this.preRollBytes -= this.preRoll.shift().length;
        }
        if (this.preRoll.length && this.preRollBytes > max) {
            const drop = this.preRollBytes - max;
            this.preRoll[0] = this.preRoll[0].subarray(drop);
            this.preRollBytes -= drop;
        }
    }

    _flushPreRoll() {
        const max = this._marginBytes();
        while (this.preRoll.length > 1 && this.preRollBytes - this.preRoll[0].length >= max) {
            this.preRollBytes -= this.preRoll.shift().length;
        }
        if (this.preRoll.length && this.preRollBytes > max) {
            const drop = this.preRollBytes - max;
            this.preRoll[0] = this.preRoll[0].subarray(drop);
            this.preRollBytes -= drop;
        }
        for (const c of this.preRoll) this._store(c);
        this.preRoll = [];
        this.preRollBytes = 0;
    }

    _updateBitrateEstimate() {
        if (this.inBlank) {
            this._brSample = null;
            return;
        }
        const now = performance.now();
        if (!this._brSample) {
            this._brSample = { t: now, bytes: this.totalBytesWritten };
            return;
        }
        const elapsed = (now - this._brSample.t) / 1000;
        if (elapsed < 3) return;
        const bytes = this.totalBytesWritten - this._brSample.bytes;
        this._brSample = { t: now, bytes: this.totalBytesWritten };
        if (bytes <= 0) return;
        const bps = (bytes * 8) / elapsed;
        if (bps < 16000 || bps > 320000) return;
        const blended = this._brLocked ? this.bitrate * 0.8 + bps * 0.2 : bps;
        this._brLocked = true;
        this.setBitrate(Math.round(blended));
    }

    addTitleChange(metadata) {
        const rawTitle = metadata?.StreamTitle ?? metadata?.streamTitle ?? metadata?.title ?? '';
        let title = String(rawTitle ?? '').trim();
        let isEmpty = !title;
        let coerceReason = '';

        // Icecast recolle souvent le dernier talker juste après un blanc / on_air ↓.
        // Sticky uniquement tant que la parole listing est encore off : un vrai
        // on_air↑ (+ même RFSI) doit pouvoir sortir de la grâce.
        if (!isEmpty && !this._liveSpeech && title === this._titleBeforeBlank
            && (this.blankGraceActive || this.inBlank)) {
            const holdMs = (this.config.blankHoldSec || 7) * 1000;
            const t0 = this.blankGraceStartedAtMs || this.blankStartedAtMs || 0;
            const age = t0 ? (performance.now() - t0) : 0;
            if (this.blankGraceActive || age < holdMs) {
                isEmpty = true;
                title = '';
                coerceReason = this.blankGraceActive ? 'sticky-during-grace' : 'sticky-while-blank';
            }
        }
        if (!isEmpty && !this._liveSpeech) {
            // Listing off pendant un vrai blanc / grâce : retenir (titre collé), ne pas écrire.
            if (this.inBlank || this.blankGraceActive) {
                this._heldTitleWhileSpeechOff = { title, at: performance.now() };
                if (typeof window !== 'undefined' && window.__MONITOR_DEBUG__) {
                    console.log(
                        '%c[debug:ring]%c addTitleChange',
                        'background:#2e7d32;color:#fff;padding:1px 6px;border-radius:3px',
                        'color:inherit',
                        {
                            t: new Date().toISOString().slice(11, 23),
                            StreamTitle: '(held)',
                            raw: rawTitle,
                            coerceReason: 'live-speech-off',
                            liveSpeech: this._liveSpeech,
                            awaitingFirstTitle: this.awaitingFirstTitle,
                            inBlank: this.inBlank,
                            blankGraceActive: this.blankGraceActive,
                            lastTitle: this.lastTitle,
                            bytesWritten: this.totalBytesWritten,
                        }
                    );
                }
                return false;
            }
            // Listing déjà off mais le mount envoie encore un talker (com courte :
            // on_air↓ avant l'ICY) — faire confiance au StreamTitle, ne pas le jeter.
            coerceReason = coerceReason || 'listing-lag-accept';
        }

        if (typeof window !== 'undefined' && window.__MONITOR_DEBUG__) {
            console.log(
                '%c[debug:ring]%c addTitleChange',
                'background:#2e7d32;color:#fff;padding:1px 6px;border-radius:3px',
                'color:inherit',
                {
                    t: new Date().toISOString().slice(11, 23),
                    StreamTitle: isEmpty ? '(empty)' : String(title),
                    raw: rawTitle,
                    coerceReason: coerceReason || null,
                    liveSpeech: this._liveSpeech,
                    metaKeys: metadata && typeof metadata === 'object' ? Object.keys(metadata) : [],
                    awaitingFirstTitle: this.awaitingFirstTitle,
                    inBlank: this.inBlank,
                    blankGraceActive: this.blankGraceActive,
                    lastTitle: this.lastTitle,
                    bytesWritten: this.totalBytesWritten,
                }
            );
        }

        // Attente du premier titre : ignorer les métadonnées vides (blanc Icecast au connect).
        if (this.awaitingFirstTitle) {
            if (isEmpty) return false;
            this.awaitingFirstTitle = false;
            this.awaitingFirstTitleSinceMs = 0;
            this.inBlank = false;
            this.blankTailRemaining = 0;
            this._cancelBlankGrace();
            this.preRoll = [];
            this.preRollBytes = 0;
            this.lastTitle = title;
            this._titleBeforeBlank = '';
            this._recordTitle(title);
            return true;
        }

        // lastTitle === null : jamais de titre réel (ex. après timeout 1er titre).
        // Ne pas traiter '' comme doublon de null — il faut entrer / rester en blanc.
        const prevTitle = this.lastTitle == null ? null : String(this.lastTitle).trim();
        if (prevTitle !== null && title === prevTitle) {
            // Métadonnée blanc répétée (silence prolongé) : ré-ancrer le marqueur
            // après rotation du tampon pour ne pas laisser un vieux label couvrir le blanc.
            if (isEmpty && this.blankGraceActive) {
                this._maybeConfirmBlankGrace();
                return false;
            }
            // lastTitle déjà '' mais grâce annulée (faux on_air↑) : ré-armer le blanc.
            if (isEmpty && !this.inBlank && !this.blankGraceActive) {
                if (!this._titleBeforeBlank) {
                    for (let i = this.titleChanges.length - 1; i >= 0; i--) {
                        const t = String(this.titleChanges[i].title || '').trim();
                        if (t) {
                            this._titleBeforeBlank = t;
                            break;
                        }
                    }
                }
                this.blankGraceActive = true;
                this.blankGraceStartedAtMs = performance.now();
                this.blankGraceStartByte = this.totalBytesWritten;
                return true;
            }
            if (!(isEmpty && this.inBlank)) return false;
            const last = this.titleChanges[this.titleChanges.length - 1];
            if (last && last.title === '' && this.totalBytesWritten <= last.byteOffset) {
                this._updateOpenBlankMarker();
                return false;
            }
            // Ré-ancrer un StreamTitle vide au bout courant du ring.
            this._recordTitle('');
            this._updateOpenBlankMarker();
            return true;
        }

        if (isEmpty) {
            const prev = String(this.lastTitle || '').trim();
            if (prev) this._titleBeforeBlank = prev;
        } else {
            this._titleBeforeBlank = '';
        }

        if (!isEmpty) {
            this.lastTitle = title;
            // Reprise pendant la marge : annuler, l'audio a déjà été gardé sous l'ancien titre.
            this._cancelBlankGrace();
            if (this.inBlank) {
                this._resumeFromBlank(title);
            } else {
                const last = this.titleChanges[this.titleChanges.length - 1];
                if (last?.liveResume && !String(last.title || '').trim()) {
                    last.title = title;
                } else {
                    this._recordTitle(title);
                }
            }
        } else if (this.inBlank) {
            this.lastTitle = '';
            this._recordTitle('');
            this._updateOpenBlankMarker();
        } else if (!this.blankGraceActive) {
            // Début de grâce : lastTitle reste le talker ; encore blankMarginSec
            // d'audio stockés sous ce titre, puis marqueur vide en fin de fenêtre.
            this.blankGraceActive = true;
            this.blankGraceStartedAtMs = performance.now();
            this.blankGraceStartByte = this.totalBytesWritten;
        } else {
            this._maybeConfirmBlankGrace();
        }
        return true;
    }

    _recordTitle(title) {
        this._recordTitleAt(title, this.totalBytesWritten);
    }

    _recordTitleAt(title, byteOffset) {
        const off = Math.max(
            this.oldestByteOffset,
            Math.min(byteOffset, this.totalBytesWritten)
        );
        this.titleChanges.push({
            byteOffset: off,
            title: String(title ?? ''),
            droppedBlankSec: 0,
            blankWallMs: 0,
        });
        this.titleChanges.sort((a, b) => a.byteOffset - b.byteOffset);
        this._pruneTitles();
    }

    /**
     * Sortie de blanc : dernière seconde sous le nouveau titre (ou liveResume).
     * Encore dans les 2 s stockées → marqueur à now-1s (borné au début du blanc).
     * Déjà en drop → flush du preRoll 1 s puis marqueur au début du flush.
     */
    _resumeFromBlank(title) {
        const oneSec = this._resumePreRollBytes();
        const blankStart = this.blankStartByte || this.oldestByteOffset;
        const wasStoringTail = this.blankTailRemaining > 0;
        this._finalizeBlankMarker();
        this.inBlank = false;
        this.blankTailRemaining = 0;
        const labeled = String(title || '').trim();
        // Reprise sans ICY : lastTitle vide → pas d'affichage de l'ancien talker.
        this.lastTitle = labeled;
        if (wasStoringTail) {
            this.preRoll = [];
            this.preRollBytes = 0;
            const resumeAt = Math.max(blankStart, this.totalBytesWritten - oneSec);
            if (labeled) {
                this._recordTitleAt(labeled, resumeAt);
            } else {
                this.titleChanges.push({
                    byteOffset: resumeAt,
                    title: '',
                    liveResume: true,
                    droppedBlankSec: 0,
                    blankWallMs: 0,
                });
                this._pruneTitles();
            }
            return;
        }
        const resumeAt = this.totalBytesWritten;
        this._flushPreRoll();
        if (labeled) {
            this._recordTitleAt(labeled, resumeAt);
        } else {
            this.titleChanges.push({
                byteOffset: resumeAt,
                title: '',
                liveResume: true,
                droppedBlankSec: 0,
                blankWallMs: 0,
            });
            this._pruneTitles();
        }
    }

    _updateOpenBlankMarker() {
        const last = this.titleChanges[this.titleChanges.length - 1];
        if (!last || String(last.title || '').trim()) return;
        const bps = Math.max(1, this.bitrate / 8);
        last.droppedBlankSec = this.currentBlankDroppedBytes / bps;
        if (this.blankStartedAtMs) {
            last.blankWallMs = performance.now() - this.blankStartedAtMs;
        }
    }

    _finalizeBlankMarker() {
        this._updateOpenBlankMarker();
        this.currentBlankDroppedBytes = 0;
        this.blankStartedAtMs = 0;
        this.blankStartByte = 0;
    }

    // null = pas encore de métadonnée ICY avant bytePos ; '' = blanc explicite.
    titleForByte(bytePos) {
        let found = false;
        let current = '';
        for (const e of this.titleChanges) {
            if (e.byteOffset <= bytePos) {
                current = e.title ?? '';
                found = true;
            } else {
                break;
            }
        }
        return found ? current : null;
    }

    _markerAtByte(bytePos) {
        let cur = null;
        for (const e of this.titleChanges) {
            if (e.byteOffset <= bytePos) cur = e;
            else break;
        }
        return cur;
    }

    _entryIsSpeech(e) {
        if (!e) return false;
        if (String(e.title || '').trim()) return true;
        // liveResume vide = parole en cours sans StreamTitle encore (pas un blanc).
        return !!e.liveResume;
    }

    isBlankAtByte(bytePos) {
        const e = this._markerAtByte(bytePos);
        if (!e) return false;
        if (this._entryIsSpeech(e)) return false;
        return !String(e.title || '').trim();
    }

    /**
     * Label ICY à bytePos uniquement.
     * liveResume vide → rien (pas lastTitle / ancien talker).
     */
    displayTitleAtByte(bytePos) {
        const e = this._markerAtByte(bytePos);
        if (!e) return '';
        const own = String(e.title || '').trim();
        if (own) return own;
        if (!e.liveResume) return '';
        for (const n of this.titleChanges) {
            if (n.byteOffset > e.byteOffset) {
                const t = String(n.title || '').trim();
                if (t) return t;
            }
        }
        return '';
    }

    /** Infos sur le blanc couvrant bytePos (durée sautée à l'ingest, etc.). */
    blankInfoAtByte(bytePos) {
        let blank = null;
        for (const e of this.titleChanges) {
            if (e.byteOffset <= bytePos) blank = e;
            else break;
        }
        if (!blank || this._entryIsSpeech(blank)) {
            return { isBlank: false, droppedBlankSec: 0, blankWallMs: 0 };
        }
        let dropped = blank.droppedBlankSec || 0;
        let wallMs = blank.blankWallMs || 0;
        // Blanc encore ouvert à l'ingest : durée live (pas seulement le dernier marqueur figé).
        if (this.inBlank) {
            const bps = Math.max(1, this.bitrate / 8);
            dropped = this.currentBlankDroppedBytes / bps;
            wallMs = this.blankStartedAtMs ? (performance.now() - this.blankStartedAtMs) : wallMs;
        }
        return { isBlank: true, droppedBlankSec: dropped, blankWallMs: wallMs };
    }

    /**
     * Secondes de blanc réellement écoulées à bytePos :
     * marge conservée sous l'ancien titre (~0,7 s) + silence sauté + wall clock.
     */
    effectiveBlankSecAt(bytePos) {
        const info = this.blankInfoAtByte(bytePos);
        if (!info.isBlank) return 0;
        const kept = this.config.blankMarginSec || 0.7;
        const wallSec = (info.blankWallMs || 0) / 1000;
        const fromDropped = info.droppedBlankSec + Math.min(kept, wallSec);
        return Math.max(fromDropped, wallSec);
    }

    /** Prochaine parole (titre ICY ou reprise on_air) strictement après bytePos. */
    nextTitleAfterByte(bytePos) {
        for (const e of this.titleChanges) {
            if (e.byteOffset > bytePos && this._entryIsSpeech(e)) return e;
        }
        return null;
    }

    /** Parole à bytePos ou après (reprise déjà sous la tête). */
    nextTitleAtOrAfterByte(bytePos) {
        for (const e of this.titleChanges) {
            if (e.byteOffset >= bytePos && this._entryIsSpeech(e)) return e;
        }
        return null;
    }

    hasTitleAheadOfByte(bytePos) {
        return this.nextTitleAfterByte(bytePos) != null;
    }

    // Garde les marqueurs de blanc même si le byte exact a été élagué : on
    // ancre le plus ancien événement encore utile sur oldestByteOffset.
    _pruneTitles() {
        const cutoff = this.oldestByteOffset;
        if (this.titleChanges.length === 0) return;
        let lastBefore = null;
        const kept = [];
        for (const e of this.titleChanges) {
            if (e.byteOffset < cutoff) {
                lastBefore = e;
            } else {
                kept.push(e);
            }
        }
        if (lastBefore) {
            // Conserver le titre/blanc actif au début de la fenêtre stockée.
            kept.unshift({
                byteOffset: cutoff,
                title: lastBefore.title ?? '',
                droppedBlankSec: lastBefore.droppedBlankSec || 0,
                blankWallMs: lastBefore.blankWallMs || 0,
                liveResume: !!lastBefore.liveResume,
            });
        }
        this.titleChanges = kept;
    }

    _prune() {
        if (this._playbackPaused) {
            while (this.chunks.length > 0) {
                const chunk = this.chunks[0];
                const chunkEnd = chunk.startByte + chunk.data.length;
                if (chunkEnd <= this.readByteOffset) {
                    this.chunks.shift();
                } else {
                    break;
                }
            }
            this._pruneTitles();
            return;
        }
        const cutoff = this.totalBytesWritten - this.maxBytes;
        while (this.chunks.length > 0 && this.chunks[0].startByte < cutoff) {
            const removed = this.chunks.shift();
            const removedEnd = removed.startByte + removed.data.length;
            if (this.readByteOffset >= removed.startByte && this.readByteOffset < removedEnd) {
                this.readByteOffset = removedEnd;
            } else if (this.readByteOffset < removed.startByte) {
                this.readByteOffset = removedEnd;
            }
        }
        this._pruneTitles();
    }

    peekNextUnread() {
        if (this.readByteOffset >= this.totalBytesWritten) return null;
        for (const chunk of this.chunks) {
            const chunkEnd = chunk.startByte + chunk.data.length;
            if (chunkEnd <= this.readByteOffset) continue;
            const offsetInChunk = Math.max(0, this.readByteOffset - chunk.startByte);
            if (offsetInChunk < chunk.data.length) {
                return chunk.data.subarray(offsetInChunk);
            }
        }
        return null;
    }

    advanceRead(bytes) {
        this.readByteOffset = Math.min(this.readByteOffset + bytes, this.totalBytesWritten);
    }

    seekNearLive(prebufferSec) {
        const back = Math.floor((this.bitrate / 8) * prebufferSec);
        this.readByteOffset = Math.max(this.oldestByteOffset, this.totalBytesWritten - back);
    }

    _blankStoreBytes() {
        const sec = this.config.blankStoreSec || 2.0;
        return Math.floor((this.bitrate / 8) * sec);
    }

    _resumePreRollBytes() {
        const sec = this.config.resumePreRollSec || 1.0;
        return Math.floor((this.bitrate / 8) * sec);
    }

    _marginBytes() {
        return this._resumePreRollBytes();
    }

    /**
     * Ne pas enregistrer le blanc initial (Analogique) : ignorer l'audio jusqu'au
     * premier StreamTitle non vide, puis enregistrer normalement à partir de là.
     */
    beginAwaitingStreamTitle() {
        this.awaitingFirstTitle = true;
        this.awaitingFirstTitleSinceMs = performance.now();
        this.inBlank = true;
        this.blankTailRemaining = 0;
        this.lastTitle = null;
        this.preRoll = [];
        this.preRollBytes = 0;
        this.blankStartedAtMs = 0;
        this.blankStartByte = 0;
        this.currentBlankDroppedBytes = 0;
        this._cancelBlankGrace();
        this._liveSpeech = true;
        this._heldTitleWhileSpeechOff = null;
        this._titleBeforeBlank = '';
    }

    reset() {
        this.chunks = [];
        this.titleChanges = [];
        this.lastTitle = null;
        this.totalBytesWritten = 0;
        this.readByteOffset = 0;
        this.inBlank = false;
        this.blankTailRemaining = 0;
        this.preRoll = [];
        this.preRollBytes = 0;
        this.droppedBlankBytes = 0;
        this._brSample = null;
        this._brLocked = false;
        this._playbackPaused = false;
        this.awaitingFirstTitle = false;
        this.awaitingFirstTitleSinceMs = 0;
        this.blankStartedAtMs = 0;
        this.blankStartByte = 0;
        this.currentBlankDroppedBytes = 0;
        this._cancelBlankGrace();
        this._liveSpeech = true;
        this._heldTitleWhileSpeechOff = null;
        this._titleBeforeBlank = '';
    }
}

export class StreamIngestor {
    constructor(ringBuffer, streamUrl, config, callbacks) {
        this.ringBuffer = ringBuffer;
        this.streamUrl = streamUrl;
        this.config = config;
        this.callbacks = callbacks;
        this.running = false;
        this.abortController = null;
        this.reader = null;
        this.icyBr = config.fallbackBitrate;
        this.icyMetaInt = undefined;
        this.reconnectDelay = config.reconnectBaseDelayMs;
    }

    async start() {
        if (this.running) return;
        this.running = true;
        this.reconnectDelay = this.config.reconnectBaseDelayMs;
        while (this.running) {
            try {
                await this._connectOnce();
                this.reconnectDelay = this.config.reconnectBaseDelayMs;
            } catch (err) {
                if (!this.running) break;
                this.callbacks.onError?.(err);
                const msg = String(err && err.message ? err.message : err);
                const transient = /HTTP 50[234]|HTTP 404|Flux terminé|Failed to fetch|NetworkError|Load failed/i.test(msg);
                const wait = transient
                    ? Math.min(this.reconnectDelay, 800)
                    : this.reconnectDelay;
                await this._sleep(wait);
                this.reconnectDelay = Math.min(
                    this.reconnectDelay * 2,
                    this.config.reconnectMaxDelayMs,
                );
            }
        }
    }

    stop() {
        this.running = false;
        this.abortController?.abort();
    }

    async _connectOnce() {
        this.abortController = new AbortController();
        // Même en-tête que tetrapol-kit / Icecast : Icy-MetaData (D majuscule).
        const response = await fetch(this.streamUrl, {
            method: 'GET',
            headers: { 'Icy-MetaData': '1' },
            credentials: 'same-origin',
            signal: this.abortController.signal,
        });

        if (!response.ok) {
            throw new Error(`HTTP ${response.status} ${response.statusText}`);
        }

        // Lire MetaInt sous toutes les casse possibles (proxy / navigateurs).
        const icyMetaIntHeader = response.headers.get('icy-metaint')
            || response.headers.get('Icy-MetaInt')
            || response.headers.get('Icy-Metaint');
        const icyBrHeader = response.headers.get('icy-br')
            || response.headers.get('Icy-Br');

        this.icyMetaInt = undefined;
        if (icyMetaIntHeader) {
            const parsed = parseInt(icyMetaIntHeader, 10);
            if (Number.isFinite(parsed) && parsed >= this.config.minValidMetaInt) {
                this.icyMetaInt = parsed;
            }
        }
        if (icyBrHeader) {
            this.icyBr = parseInt(icyBrHeader, 10) || this.config.fallbackBitrate;
            this.ringBuffer.setBitrate(this.icyBr);
        }

        this.callbacks.onConnected?.({
            icyMetaInt: this.icyMetaInt ?? null,
            icyBr: this.icyBr,
            icyMetaIntHeader: icyMetaIntHeader || null,
        });

        const readerOpts = {
            metadataTypes: ['icy'],
            enableLogging: !!(typeof window !== 'undefined' && window.__MONITOR_DEBUG__),
            onStream: async ({ stream }) => {
                this.ringBuffer.append(stream);
                this.callbacks.onStream?.(stream.length);
            },
            onMetadata: async (value) => {
                const changed = this.ringBuffer.addTitleChange(value.metadata);
                this.callbacks.onMetadata?.(value, changed);
            },
            onError: (msg) => {
                const text = String(msg || '');
                // Message informatif de icecast-metadata-js : ce n'est pas une erreur fatale.
                if (/icy-metaint is invalid|attempting to detect/i.test(text)) {
                    if (typeof window !== 'undefined' && window.__MONITOR_DEBUG__) {
                        console.info('[debug:ring] ICY MetaInt auto-detect:', text);
                    }
                    return;
                }
                this.callbacks.onError?.(new Error(text));
            },
        };
        // Ne passer icyMetaInt que s'il est valide — sinon laisser la détection auto.
        if (this.icyMetaInt != null) readerOpts.icyMetaInt = this.icyMetaInt;

        this.reader = new IcecastMetadataReader(readerOpts);

        const bodyReader = response.body.getReader();
        while (this.running) {
            const { done, value } = await bodyReader.read();
            if (done) break;
            if (value?.length) this.reader.readAll(value);
        }
        if (this.running) throw new Error('Flux terminé — reconnexion…');
    }

    _sleep(ms) {
        return new Promise((resolve) => setTimeout(resolve, ms));
    }
}

export class BufferPlayer {
    constructor(audioEl, ringBuffer, config = QUEUE_BUFFER_CONFIG) {
        this.audioEl = audioEl;
        this.ringBuffer = ringBuffer;
        this.config = config;
        this.mediaSource = null;
        this.sourceBuffer = null;
        this.objectUrl = null;
        this._mseViaSrcObject = false;
        this.isPlaying = false;
        this.isReady = false;
        this.hasStarted = false;
        this.feedTimer = null;
        this.pendingAppend = false;
        this.mseTimeline = [];
        this._pendingChunkStartByte = 0;
        this._pendingChunkLen = 0;
        this._pendingT0 = 0;
        this._playbackStartByte = 0;
        this._recovering = false;
        this._lastRecoverAt = 0;
        this._pausedInPlace = false;
        this._needsAudioPlay = false;
        this._intentionalPause = false;
        this.onLog = null;
        this._onUpdateEnd = this._onUpdateEnd.bind(this);
        this._onAudioPause = this._onAudioPause.bind(this);
        this._onAudioWaiting = this._onAudioWaiting.bind(this);
        this._onAudioTimeUpdate = this._onAudioTimeUpdate.bind(this);
        this._bindAudioLifecycle();
    }

    _bindAudioLifecycle() {
        const el = this.audioEl;
        if (!el || el.dataset.ringLifecycleBound === '1') return;
        el.dataset.ringLifecycleBound = '1';
        // playsinline / pas de mute : aide mobile + certains navigateurs en arrière-plan.
        el.setAttribute('playsinline', '');
        el.setAttribute('webkit-playsinline', '');
        el.preload = 'auto';
        el.addEventListener('pause', this._onAudioPause);
        el.addEventListener('waiting', this._onAudioWaiting);
        el.addEventListener('stalled', this._onAudioWaiting);
        el.addEventListener('timeupdate', this._onAudioTimeUpdate);
    }

    // Pause inattendue (changement d'onglet / politique navigateur) : relancer
    // immédiatement tant que le BufferPlayer doit jouer.
    _onAudioPause() {
        if (!this.isPlaying || this._intentionalPause || this._pausedInPlace) return;
        this._needsAudioPlay = true;
        this.onLog?.('Pause audio inattendue — relance (arrière-plan)…');
        this._ensureAudioRunning();
    }

    _onAudioWaiting() {
        if (!this.isPlaying) return;
        this._feedTick();
        this._ensureAudioRunning();
    }

    // Les timers du thread principal sont freinés en onglet caché ; timeupdate
    // continue tant que le média joue et sert de filet d'alimentation MSE.
    _onAudioTimeUpdate() {
        if (!this.isPlaying) return;
        if (typeof document !== 'undefined' && document.hidden) {
            this._feedTick();
        }
    }

    async init() {
        let lastErr;
        for (let attempt = 0; attempt < 2; attempt++) {
            try {
                return await this._initOnce();
            } catch (err) {
                lastErr = err;
                if (attempt === 0 && String(err?.message || '').includes('MediaSource timeout')) {
                    this._teardownMse();
                    try {
                        this.audioEl.pause();
                        this._detachMediaElement();
                        this.audioEl.load();
                    } catch { /* ignore */ }
                    await new Promise((r) => setTimeout(r, 80));
                    continue;
                }
                throw err;
            }
        }
        throw lastErr;
    }

    /** Attache MediaSource. Jamais de Blob audio / enregistrement figé. */
    _attachMediaSource(ms) {
        this.objectUrl = null;
        this._mseViaSrcObject = false;
        // Safari : MediaSource via srcObject. Chrome/Firefox : object URL MediaSource
        // (mécanisme MSE standard — ce n'est PAS un Blob d'enregistrement audio).
        try {
            if ('srcObject' in this.audioEl) {
                try { this.audioEl.removeAttribute('src'); } catch { /* ignore */ }
                this.audioEl.srcObject = ms;
                this._mseViaSrcObject = true;
                return;
            }
        } catch {
            // TypeError : navigateur n'accepte que MediaStream sur srcObject.
        }
        this.objectUrl = URL.createObjectURL(ms);
        try { this.audioEl.srcObject = null; } catch { /* ignore */ }
        this.audioEl.src = this.objectUrl;
    }

    _detachMediaElement() {
        try {
            if (this._mseViaSrcObject) this.audioEl.srcObject = null;
        } catch { /* ignore */ }
        this._mseViaSrcObject = false;
        try { this.audioEl.removeAttribute('src'); } catch { /* ignore */ }
        if (this.objectUrl) {
            try { URL.revokeObjectURL(this.objectUrl); } catch { /* ignore */ }
            this.objectUrl = null;
        }
    }

    async _initOnce() {
        if (typeof MediaSource === 'undefined'
            || typeof MediaSource.isTypeSupported !== 'function'
            || !MediaSource.isTypeSupported('audio/mpeg')) {
            throw new Error('MSE audio/mpeg non supporté');
        }
        this._teardownMse();
        this.mediaSource = new MediaSource();
        await new Promise((resolve, reject) => {
            const timeout = setTimeout(() => reject(new Error('MediaSource timeout')), 8000);
            this.mediaSource.addEventListener('sourceopen', () => {
                clearTimeout(timeout);
                try {
                    this.sourceBuffer = this.mediaSource.addSourceBuffer('audio/mpeg');
                    this.sourceBuffer.mode = 'sequence';
                    this.sourceBuffer.addEventListener('updateend', this._onUpdateEnd);
                    this.isReady = true;
                    resolve();
                } catch (err) {
                    reject(err);
                }
            }, { once: true });
            this._attachMediaSource(this.mediaSource);
        });
    }

    startFeedLoop() {
        // Pas de Worker(Blob) : setInterval + timeupdate (onglet caché) suffisent.
        if (this.feedTimer) return;
        this.feedTimer = setInterval(() => this._feedTick(), this.config.feedLoopIntervalMs);
    }

    stopFeedLoop() {
        if (this.feedTimer) {
            clearInterval(this.feedTimer);
            this.feedTimer = null;
        }
    }

    async play(options = {}) {
        if (!this.isReady) await this.init();
        if (!this.hasStarted) {
            if (options.fromStart) {
                this.ringBuffer.readByteOffset = this.ringBuffer.oldestByteOffset;
            } else {
                this.ringBuffer.seekNearLive(this.config.minBufferBeforePlaySec);
            }
            this._playbackStartByte = this.ringBuffer.readByteOffset;
            this.hasStarted = true;
        } else if (!this._canResumeInPlace()) {
            await this._resumeAfterPause();
        }
        this._pausedInPlace = false;
        this._intentionalPause = false;
        this.ringBuffer.setPlaybackPaused(false);
        this.isPlaying = true;
        this.startFeedLoop();
        await this._feedTick();
        await this._safeAudioPlay();
    }

    async _safeAudioPlay() {
        try {
            this._intentionalPause = false;
            await this.audioEl.play();
            this._needsAudioPlay = false;
        } catch (err) {
            // Onglet en arrière-plan / autoplay bloqué : garder isPlaying pour
            // continuer à alimenter le MSE et retenter play() (pause/waiting,
            // worker, reprise focus).
            if (this._isAutoplayBlocked(err)) {
                this._needsAudioPlay = true;
                this.onLog?.('Lecture différée (onglet non focalisé)…');
                return;
            }
            if (!this._isPlayInterrupted(err) || !this.isPlaying) {
                this.isPlaying = false;
                throw err;
            }
            await new Promise((r) => setTimeout(r, 120));
            if (!this.isPlaying) return;
            try {
                await this.audioEl.play();
                this._needsAudioPlay = false;
            } catch (retryErr) {
                if (this._isAutoplayBlocked(retryErr)) {
                    this._needsAudioPlay = true;
                    return;
                }
                if (this._isPlayInterrupted(retryErr)) {
                    this._needsAudioPlay = true;
                    return;
                }
                this.isPlaying = false;
                throw retryErr;
            }
        }
    }

    _ensureAudioRunning() {
        if (!this.isPlaying || !this.isReady) return;
        if (!this.audioEl.paused && !this._needsAudioPlay) return;
        if (this.bufferedAhead() <= 0.02 && this.ringBuffer.unreadBytes <= 0) return;
        this._needsAudioPlay = false;
        this._safeAudioPlay().catch(() => {
            this._needsAudioPlay = true;
        });
    }

    _isAutoplayBlocked(err) {
        const msg = String(err?.message || err || '').toLowerCase();
        return err?.name === 'NotAllowedError'
            || msg.includes('not allowed')
            || msg.includes('user gesture')
            || msg.includes('user denied')
            || msg.includes('document is not focused');
    }

    _isPlayInterrupted(err) {
        const msg = String(err?.message || err || '').toLowerCase();
        return err?.name === 'AbortError'
            || msg.includes('interrupted')
            || msg.includes('pause()');
    }

    _canResumeInPlace() {
        return this._pausedInPlace
            && this.isReady
            && this.mediaSource
            && this.mediaSource.readyState === 'open'
            && this.sourceBuffer;
    }

    pause() {
        this.isPlaying = false;
        this._needsAudioPlay = false;
        this._pausedInPlace = true;
        this._intentionalPause = true;
        this.ringBuffer.setPlaybackPaused(true);
        this.audioEl.pause();
        this.stopFeedLoop();
    }

    reset() {
        this._pausedInPlace = false;
        this._intentionalPause = true;
        this.ringBuffer.setPlaybackPaused(false);
        this.isPlaying = false;
        this.audioEl.pause();
        this.stopFeedLoop();
        this.pendingAppend = false;
        this.isReady = false;
        this.hasStarted = false;
        this.mseTimeline = [];
        this._playbackStartByte = 0;
        this._pendingChunkLen = 0;
        this._teardownMse();
        this._detachMediaElement();
        this.audioEl.load();
    }

    _teardownMse() {
        if (this.sourceBuffer) {
            this.sourceBuffer.removeEventListener('updateend', this._onUpdateEnd);
            this.sourceBuffer = null;
        }
        if (this.objectUrl) {
            try { URL.revokeObjectURL(this.objectUrl); } catch { /* ignore */ }
            this.objectUrl = null;
        }
        this.mediaSource = null;
    }

    _bufferedEndTime() {
        if (!this.audioEl.buffered.length) return this.audioEl.currentTime || 0;
        return this.audioEl.buffered.end(this.audioEl.buffered.length - 1);
    }

    _recordTimelineSegment() {
        if (this._pendingChunkLen <= 0) return;
        const mseEnd = this._bufferedEndTime();
        this.mseTimeline.push({
            mseStart: this._pendingT0,
            mseEnd,
            byteStart: this._pendingChunkStartByte,
            byteLen: this._pendingChunkLen,
        });
        this._pendingChunkLen = 0;
        const ct = this.audioEl.currentTime;
        this.mseTimeline = this.mseTimeline.filter((s) => s.mseEnd > ct - 2);
        if (this.mseTimeline.length > 300) {
            this.mseTimeline.splice(0, this.mseTimeline.length - 300);
        }
    }

    playbackByteOffset() {
        const ct = this.audioEl.currentTime;
        if (this.mseTimeline.length === 0) return this._playbackStartByte;
        for (let i = this.mseTimeline.length - 1; i >= 0; i--) {
            const seg = this.mseTimeline[i];
            if (ct < seg.mseStart) continue;
            if (ct >= seg.mseEnd) return seg.byteStart + seg.byteLen;
            const dur = seg.mseEnd - seg.mseStart;
            const frac = dur > 0 ? (ct - seg.mseStart) / dur : 0;
            return seg.byteStart + frac * seg.byteLen;
        }
        return this.mseTimeline[0].byteStart;
    }

    _onUpdateEnd() {
        this.pendingAppend = false;
        this._recordTimelineSegment();
        if (this.isPlaying) {
            this._cleanupSourceBuffer();
            this._ensureAudioRunning();
            this._feedTick();
        }
    }

    _cleanupSourceBuffer() {
        if (!this.sourceBuffer || this.sourceBuffer.updating) return;
        if (!this.audioEl.buffered.length) return;
        const currentTime = this.audioEl.currentTime;
        const removeEnd = currentTime - this.config.sourceBufferCleanupBehindSec;
        if (removeEnd <= 0) return;
        try {
            const bufferedStart = this.audioEl.buffered.start(0);
            if (removeEnd > bufferedStart) this.sourceBuffer.remove(bufferedStart, removeEnd);
        } catch {
            // ignore
        }
    }

    bufferedAhead() {
        if (!this.audioEl.buffered.length) return 0;
        const end = this.audioEl.buffered.end(this.audioEl.buffered.length - 1);
        return Math.max(0, end - this.audioEl.currentTime);
    }

    waitingSeconds() {
        return this.ringBuffer.unreadSeconds + this.bufferedAhead();
    }

    async _resumeAfterPause() {
        if (this.ringBuffer.readByteOffset < this.ringBuffer.oldestByteOffset) {
            this.ringBuffer.readByteOffset = this.ringBuffer.oldestByteOffset;
        }
        this.mseTimeline = [];
        this._playbackStartByte = this.ringBuffer.readByteOffset;
        this.stopFeedLoop();
        this._teardownMse();
        this._detachMediaElement();
        this.isReady = false;
        await this.init();
    }

    // Retour au direct : seek soft dans le tampon MSE déjà chargé (fluide, sans
    // coupure). Repli hard uniquement si le buffer MSE est inutilisable.
    // Aucun Blob audio : le ring reste la seule source, MSE continue d'être alimenté.
    async jumpToLive() {
        this._pausedInPlace = false;
        this.ringBuffer.setPlaybackPaused(false);
        const pre = Math.max(0.15, this.config.minBufferBeforePlaySec || 0.5);

        if (this.isReady && this.hasStarted && this.audioEl?.buffered?.length) {
            try {
                const buf = this.audioEl.buffered;
                const end = buf.end(buf.length - 1);
                const start = buf.start(0);
                const behind = end - (this.audioEl.currentTime || 0);
                if (behind > 0.35 && Number.isFinite(end)) {
                    this.audioEl.currentTime = Math.max(start, end - pre);
                    this.ringBuffer.seekNearLive(pre);
                    this._playbackStartByte = this.ringBuffer.readByteOffset;
                    const ct = this.audioEl.currentTime;
                    this.mseTimeline = this.mseTimeline.filter((s) => s.mseEnd > ct - 0.5);
                    if (this.isPlaying) {
                        this.startFeedLoop();
                        await this._feedTick();
                        await this._safeAudioPlay();
                    }
                    return;
                }
            } catch {
                // hard rebuild below
            }
        }

        this.ringBuffer.seekNearLive(pre);
        this.mseTimeline = [];
        this._playbackStartByte = this.ringBuffer.readByteOffset;
        this.stopFeedLoop();
        this._teardownMse();
        this._detachMediaElement();
        this.isReady = false;
        await this.init();
        if (this.isPlaying) {
            this.startFeedLoop();
            await this._feedTick();
            await this._safeAudioPlay();
        }
    }

    /** Sauter à un offset byte du ring (ex. début d'une nouvelle transmission). */
    async seekToByte(byteOffset) {
        this._pausedInPlace = false;
        this.ringBuffer.setPlaybackPaused(false);
        const target = Math.max(
            this.ringBuffer.oldestByteOffset,
            Math.min(byteOffset, this.ringBuffer.totalBytesWritten)
        );
        this.ringBuffer.readByteOffset = target;
        this.mseTimeline = [];
        this._playbackStartByte = target;
        this.stopFeedLoop();
        this._teardownMse();
        this._detachMediaElement();
        this.isReady = false;
        this.hasStarted = true;
        await this.init();
        this.isPlaying = true;
        this.startFeedLoop();
        await this._feedTick();
        await this._safeAudioPlay();
    }

    async _feedTick() {
        if (!this.isReady || !this.sourceBuffer || !this.isPlaying) return;
        this._ensureAudioRunning();
        if (this.audioEl.error) {
            this._handleMediaError(`média code ${this.audioEl.error.code}`);
            return;
        }
        if (!this.mediaSource || this.mediaSource.readyState !== 'open') return;
        if (this.sourceBuffer.updating || this.pendingAppend) return;
        if (this.bufferedAhead() >= this.config.targetLeadSec) return;

        const next = this.ringBuffer.peekNextUnread();
        if (!next || next.length === 0) return;

        const maxChunk = 64 * 1024;
        const toAppend = next.length > maxChunk ? next.subarray(0, maxChunk) : next;
        try {
            this.pendingAppend = true;
            this._pendingChunkStartByte = this.ringBuffer.readByteOffset;
            this._pendingChunkLen = toAppend.length;
            this._pendingT0 = this._bufferedEndTime();
            this.sourceBuffer.appendBuffer(toAppend);
            this.ringBuffer.advanceRead(toAppend.length);
        } catch (err) {
            this.pendingAppend = false;
            this._pendingChunkLen = 0;
            this._handleMediaError(`appendBuffer: ${err.message}`);
        }
    }

    canPlay() {
        return this.ringBuffer.unreadSeconds + this.bufferedAhead() >= this.config.minBufferBeforePlaySec;
    }

    async recover() {
        this._pausedInPlace = false;
        this.ringBuffer.setPlaybackPaused(false);
        this.stopFeedLoop();
        this.pendingAppend = false;
        this._pendingChunkLen = 0;
        const wasPlaying = this.isPlaying;
        this.isPlaying = false;
        this._teardownMse();
        this._detachMediaElement();
        this.isReady = false;
        await this.init();
        const now = Date.now();
        const looping = now - this._lastRecoverAt < 4000;
        this._lastRecoverAt = now;
        if (looping) {
            this.ringBuffer.seekNearLive(this.config.minBufferBeforePlaySec);
            this.onLog?.('Erreurs média répétées — saut près du direct.');
        }
        this.mseTimeline = [];
        this._playbackStartByte = this.ringBuffer.readByteOffset;
        if (wasPlaying) {
            this.isPlaying = true;
            this.startFeedLoop();
            await this._feedTick();
            await this._safeAudioPlay();
        }
    }

    async _handleMediaError(reason) {
        if (this._recovering || !this.hasStarted) return;
        this._recovering = true;
        this.onLog?.(`Erreur média (${reason}) — récupération…`);
        try {
            await this.recover();
            this.onLog?.('Lecteur récupéré.');
        } catch (err) {
            this.onLog?.(`Échec récupération: ${err.message}`);
        } finally {
            this._recovering = false;
        }
    }
}

window.QueueRingBufferKit = {
    QUEUE_BUFFER_CONFIG,
    AudioRingBuffer,
    StreamIngestor,
    BufferPlayer,
};
window.dispatchEvent(new CustomEvent('queue-ring-buffer-ready'));
try {
    (window.MonitorLog || console).info?.('queue', '✅ QueueRingBufferKit chargé');
} catch (e) {
    console.log('✅ QueueRingBufferKit chargé');
}

