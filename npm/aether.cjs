/*
 * aether.js -- zero-dependency browser client for Aether-Core.
 *
 * Offline-first storage model:
 *   - On construction, the client synchronously hydrates its state
 *     Map from localStorage BEFORE attempting any WebSocket
 *     connection. ready() resolves immediately if a cache exists,
 *     so the UI can render instantly even when the relay is offline.
 *   - On every state mutation (local, remote-via-WebSocket, or
 *     cross-tab via BroadcastChannel) the state is persisted back
 *     to localStorage, debounced to ~12Hz so a 60Hz drag does not
 *     hammer the synchronous storage API.
 *
 * Three transport tiers, in priority order:
 *   1. localStorage    -- per-tab, survives refresh, survives server outage.
 *   2. BroadcastChannel -- same-origin sibling tabs talk directly,
 *                          bypassing the gateway entirely.
 *   3. WebSocket       -- the Python relay (federation + durability).
 *
 * Public API:
 *
 *     const aether = new Aether('ws://localhost:8211');
 *     await aether.ready();           // resolves after the first snapshot
 *                                     // (or immediately if cache is hot)
 *
 *     aether.set('counter', 42);
 *     aether.get('counter');           // -> 42
 *     aether.on('counter', (newValue, oldValue) => { ... });
 *
 * The client is intentionally dumb: it does not stamp HLCs, run CRDT
 * math, or talk to peers. The Python gateway handles all of that.
 * This file is plain vanilla JavaScript -- no build step, no imports,
 * no framework.
 */
(function (root) {
    'use strict';

    // Cache schema version. Bumped only on breaking changes to the
    // on-disk shape. Caches with a different version are ignored
    // (UI falls back to waiting for the gateway snapshot).
    var CACHE_VERSION = 1;

    // localStorage write debounce in ms. Coalesces drag-storm writes
    // so the synchronous storage API is hit at most once per ~80ms
    // (~12Hz). Tuned to be invisible to the user but small enough
    // that a refresh during a drag loses at most a few hundred ms.
    var CACHE_DEBOUNCE_MS = 80;

    // How long a local write stays "pending" for the purposes of the
    // onSupersede notification. After this window we assume the gateway
    // has either echoed us back or we have moved on. Set to 10s -- long
    // enough that a slow gateway round-trip on a flaky mobile connection
    // is covered, short enough that the pending map cannot bloat. The
    // map is also bounded by MAX_PENDING_LOCAL below.
    var SUPERSEDE_WINDOW_MS = 10_000;

    // Hard cap on the number of in-flight pending local writes we will
    // track. If the user batches more than this many keys in a single
    // tick (e.g. a bulk import), the oldest tracked entries are evicted
    // FIFO. We still apply them locally and ship them to the gateway;
    // we just stop offering supersede notifications for the evicted
    // ones. This is a memory bound, not a correctness bound.
    var MAX_PENDING_LOCAL = 1024;

    function _hasLocalStorage() {
        try {
            return typeof window !== 'undefined'
                && typeof window.localStorage !== 'undefined'
                && window.localStorage !== null;
        } catch (_) {
            return false;
        }
    }

    class Aether {
        /**
         * @param {string} url - The gateway WebSocket URL, e.g. 'ws://localhost:8211'.
         * @param {object} [opts]
         * @param {boolean} [opts.autoReconnect=true]
         * @param {number}  [opts.maxReconnectDelayMs=5000]
         * @param {boolean} [opts.persist=true] - Persist state to localStorage.
         * @param {string}  [opts.cacheKey] - Override the default cache key.
         * @param {string}  [opts.authToken] - Shared secret. When provided,
         *   the token is delivered to the gateway in TWO ways for resilience:
         *   appended to the WebSocket URL as ``?auth_token=...`` (the fast
         *   path), AND sent as a first-message ``{type:'auth',token:...}``
         *   frame (works when the URL query was stripped by an intermediary).
         *   The gateway accepts whichever arrives first; both modes are
         *   constant-time-compared against the configured secret.
         */
        constructor(url, opts) {
            opts = opts || {};
            this.url = url;
            // Auth token is opaque to us; the gateway is the only party
            // that knows whether the value is right. We just deliver it.
            this._authToken = (typeof opts.authToken === 'string' && opts.authToken)
                ? opts.authToken
                : null;
            // Compute the effective URL that includes the query parameter.
            // We append, never overwrite -- callers may have other
            // query params already (e.g. /?room=lobby).
            this._wsUrl = this._authToken
                ? this._appendAuthQuery(url, this._authToken)
                : url;

            this._autoReconnect      = opts.autoReconnect !== false;
            this._maxReconnectDelay  = opts.maxReconnectDelayMs || 5000;
            this._baseReconnectDelay = 200;
            this._reconnectDelay     = this._baseReconnectDelay;

            // Local shadow of the shared state.
            this._state = new Map();
            // Per-key watchers: key -> Set<cb>
            this._watchers = new Map();
            // Global watchers (any key).
            this._anyWatchers = new Set();
            // Connection state watchers.
            this._statusWatchers = new Set();
            // Supersede watchers -- fire when a local set is overwritten
            // by a remote write before the user noticed. This is the
            // honest answer to "LWW silently drops writes": the math
            // doesn't drop them, the *application* needs to be told
            // when one of its writes lost the race so the UI can react
            // (e.g. show a "synced as: ..." toast or merge a draft).
            this._supersedeWatchers = new Set();   // cb(key, attempted, actual)
            // Pending local writes keyed by `key`: { value, ts, applied }
            // When a remote 'set' or 'delete' arrives for a tracked key
            // and the new value differs from the value we tried to write,
            // we fire onSupersede before applying the remote value. Old
            // entries (>SUPERSEDE_WINDOW_MS) are expired so the map
            // doesn't grow.
            this._pendingLocal = new Map();
            // setTimeout handle for the supersede-window sweep. Lazily
            // created on first _trackPending() so a client that never
            // writes never sets a timer.
            this._supersedeSweepTimer = null;

            this._ws = null;
            this._connected = false;
            this._hasSnapshot = false;
            this._readyResolvers = [];
            this._stopped = false;
            // Outbox for messages produced before the socket is open.
            this._pendingSend = [];

            // Ephemeral presence state (cursors). Lives only in memory
            // and in transit -- never persisted, never CRDT-merged.
            this._myId       = null;   // server-issued via 'hello'
            this._myColor    = null;
            this._presenceWatchers = new Set();   // cb(id, x, y, color)
            this._leaveWatchers    = new Set();   // cb(id)

            // ---- Cross-tab sync via BroadcastChannel ----------------
            // Same-origin browser tabs can talk to each other directly
            // without a WebSocket round-trip. This makes cross-tab sync
            // instantaneous AND survives gateway outages.
            //
            // Each Aether instance gets a per-tab _tabId. Outbound BC
            // messages are tagged with it; inbound messages with our
            // own _tabId are ignored. This prevents the echo storm
            // that would otherwise happen when we re-broadcast our
            // own local apply.
            this._tabId = (
                (typeof crypto !== 'undefined' && crypto.randomUUID)
                    ? crypto.randomUUID()
                    : 'tab-' + Math.random().toString(36).slice(2, 10)
            );
            this._bc = null;
            try {
                if (typeof BroadcastChannel !== 'undefined') {
                    // Channel name keyed on the gateway URL so pages
                    // talking to different gateways do not bleed state.
                    this._bc = new BroadcastChannel('aether::' + this.url);
                    this._bc.onmessage = (ev) => this._receiveLocal(ev.data);
                }
            } catch (_) {
                // BroadcastChannel unavailable (very old browsers, some
                // privacy modes). Degrade silently to WebSocket-only.
                this._bc = null;
            }

            // ---- Offline-first localStorage persistence --------------
            // Cache key is namespaced per-gateway-URL so different apps
            // sharing an origin do not collide.
            this._persist = opts.persist !== false;
            this._cacheKey = opts.cacheKey
                || ('aether::state::' + this.url);
            this._saveTimer = null;
            this._storageOk = this._persist && _hasLocalStorage();

            // *** This is the critical line for offline-first UX ***
            // Hydrate from cache BEFORE _connect() so ready() can
            // resolve immediately and the UI renders on first paint,
            // not on first network round-trip.
            this._loadFromStorage();

            // Now (and only now) open the WebSocket. If the cache
            // populated the state, ready() has already resolved.
            // Otherwise, ready() waits for the gateway snapshot.
            this._connect();

            // If any sibling tab already has the snapshot, get it
            // from them right away instead of waiting for our own
            // WebSocket round-trip. They reply via 'snapshot' on the
            // channel. (Harmless if we already hydrated from cache --
            // _applySnapshot will diff and apply only the deltas.)
            if (this._bc) {
                try {
                    this._bc.postMessage({
                        type: 'snapshot-request', from: this._tabId,
                    });
                } catch (_) {}
            }
        }

        // ---------------------------------------------------------------
        // Public API
        // ---------------------------------------------------------------

        /**
         * Write a value. Fires when the gateway echoes the resolved
         * mutation back, so all clients see the same final value.
         *
         * If a concurrent writer's value wins by HLC tiebreak, your
         * onSupersede subscribers will be told (key, attempted, actual)
         * the next time a remote echo overwrites your attempt. This is
         * the visibility layer for the LWW math: the math doesn't lose
         * writes, but it does mean YOUR write may not be the final
         * value -- you need to know when that happens.
         */
        set(key, value) {
            const k = String(key);
            this._trackPending(k, value);
            this._send({ type: 'set', key: k, value: value });
            // Mirror to sibling tabs immediately via BroadcastChannel.
            // The gateway will eventually echo this back too, but the
            // BC fanout is instant and works even when the gateway is
            // unreachable.
            this._broadcastLocal({ type: 'set', key: k, value: value });
            // Apply locally right away so the originating tab sees the
            // change without waiting for the gateway echo. The eventual
            // gateway echo is a no-op because _applySet checks equality.
            this._applySet(k, value, 'local');
        }

        /**
         * Delete a key. The Python side records a tombstone so the
         * delete is durable and order-resistant.
         */
        delete(key) {
            const k = String(key);
            // A delete is also a "write" for the purposes of supersede
            // tracking: if a peer's set with a higher HLC overrides our
            // delete, the application wants to know.
            this._trackPending(k, undefined);
            this._send({ type: 'delete', key: k });
            this._broadcastLocal({ type: 'delete', key: k });
            this._applyDelete(k, 'local');
        }

        /** Read the local cached value, or undefined if absent. */
        get(key) {
            return this._state.get(key);
        }

        /** Has-check against the local cache. */
        has(key) {
            return this._state.has(key);
        }

        /** Returns a fresh array of currently-live keys. */
        keys() {
            return Array.from(this._state.keys());
        }

        /** Returns a plain-object copy of the current state. */
        snapshot() {
            const out = {};
            for (const [k, v] of this._state) out[k] = v;
            return out;
        }

        /**
         * Register a watcher for a single key. Returns an unsubscribe
         * function. The callback receives ``(newValue, oldValue)`` --
         * ``newValue`` is ``undefined`` when the key is deleted.
         */
        on(key, callback) {
            if (typeof callback !== 'function') {
                throw new TypeError('callback must be a function');
            }
            let set = this._watchers.get(key);
            if (!set) {
                set = new Set();
                this._watchers.set(key, set);
            }
            set.add(callback);
            return () => set.delete(callback);
        }

        /**
         * Watch every change. Callback signature:
         * ``(key, newValue, oldValue) => void``.
         */
        onAny(callback) {
            if (typeof callback !== 'function') {
                throw new TypeError('callback must be a function');
            }
            this._anyWatchers.add(callback);
            return () => this._anyWatchers.delete(callback);
        }

        /**
         * Watch connection status changes. Callback signature:
         * ``(connected: boolean) => void``. Fires immediately with the
         * current state for convenience.
         */
        onStatus(callback) {
            this._statusWatchers.add(callback);
            try { callback(this._connected); } catch (_) {}
            return () => this._statusWatchers.delete(callback);
        }

        /**
         * Subscribe to "your write was superseded" notifications.
         *
         * Callback signature: ``(key, attemptedValue, actualValue) => void``.
         * Fires when a write you made via ``set(k, v)`` (or ``delete(k)``,
         * in which case ``attemptedValue`` is ``undefined``) loses the
         * LWW race against a concurrent writer's higher-HLC operation
         * for the same key, within a 10-second window.
         *
         * This is the visibility layer for the LWW math: the math
         * doesn't lose writes -- every operation is recorded and
         * federated -- but it does mean YOUR write may not be the
         * final value if someone else wrote the same key at almost
         * the same moment. Subscribe here when your UI needs to react
         * (show a toast, merge a draft, prompt the user, etc).
         *
         * Returns an unsubscribe function.
         *
         * Example::
         *
         *     aether.onSupersede((key, attempted, actual) => {
         *         console.warn(
         *             `My write to ${key} lost. ` +
         *             `Tried ${attempted}, ended up ${actual}.`
         *         );
         *     });
         */
        onSupersede(callback) {
            if (typeof callback !== 'function') {
                throw new TypeError('callback must be a function');
            }
            this._supersedeWatchers.add(callback);
            return () => this._supersedeWatchers.delete(callback);
        }

        /**
         * Promise that resolves once the client has *any* usable state
         * to show: either the gateway has delivered a snapshot, OR
         * the offline cache hydrated successfully. After this, ``get``
         * reads are authoritative against either the server or the
         * last-known-good cache, whichever applied first.
         */
        ready() {
            if (this._hasSnapshot) return Promise.resolve();
            return new Promise((resolve) => this._readyResolvers.push(resolve));
        }

        /** Whether the WebSocket is currently open. */
        get connected() {
            return this._connected;
        }

        /** Tear down the connection and stop auto-reconnect. */
        close() {
            this._stopped = true;
            this._autoReconnect = false;
            if (this._ws) {
                try { this._ws.close(); } catch (_) {}
            }
            if (this._bc) {
                try { this._bc.close(); } catch (_) {}
                this._bc = null;
            }
            // Flush any pending cache write so nothing is lost on close.
            if (this._saveTimer !== null) {
                clearTimeout(this._saveTimer);
                this._saveTimer = null;
                this._saveNow();
            }
            // Cancel any supersede sweep so the timer doesn't keep the
            // event loop alive in Node / jest environments.
            if (this._supersedeSweepTimer !== null) {
                clearTimeout(this._supersedeSweepTimer);
                this._supersedeSweepTimer = null;
            }
            this._pendingLocal.clear();
        }

        /**
         * Wipe the offline cache for this gateway. Useful for "log out"
         * or "reset everything" flows. Does NOT clear in-memory state;
         * call ``location.reload()`` afterwards if a full reset is wanted.
         */
        clearCache() {
            if (!this._storageOk) return;
            try {
                window.localStorage.removeItem(this._cacheKey);
            } catch (_) {}
        }

        // ---------------------------------------------------------------
        // Ephemeral presence (cursor sharing)
        // ---------------------------------------------------------------

        /**
         * Send the local cursor position to the gateway. The gateway
         * relays this to other connected clients ONLY -- it never lands
         * in the CRDT or the ledger.
         */
        sendPresence(x, y) {
            const xi = x | 0, yi = y | 0;
            // Tiny, hot-path message -- skip JSON.stringify overhead by
            // building the string directly. The integer coercion mirrors
            // what the server does.
            if (this._ws && this._ws.readyState === WebSocket.OPEN) {
                this._ws.send(
                    '{"type":"presence","x":' + xi + ',"y":' + yi + '}'
                );
            }
            // Fan out to sibling tabs immediately so they render our
            // cursor without the WebSocket round-trip. We tag with our
            // server-issued client id (if hello has arrived yet) so
            // siblings render us under the same identity.
            if (this._bc && this._myId) {
                this._broadcastLocal({
                    type:  'presence',
                    id:    this._myId,
                    color: this._myColor,
                    x:     xi,
                    y:     yi,
                });
            }
            // No outbox queueing -- if we're disconnected, just drop
            // the cursor update. Old positions are useless.
        }

        /**
         * Subscribe to remote cursors. Callback signature:
         *   (id, x, y, color) => void
         * Returns an unsubscribe function.
         */
        onPresence(callback) {
            if (typeof callback !== 'function') {
                throw new TypeError('callback must be a function');
            }
            this._presenceWatchers.add(callback);
            return () => this._presenceWatchers.delete(callback);
        }

        /**
         * Subscribe to "client left" events. Callback signature: (id) => void.
         */
        onPresenceLeave(callback) {
            if (typeof callback !== 'function') {
                throw new TypeError('callback must be a function');
            }
            this._leaveWatchers.add(callback);
            return () => this._leaveWatchers.delete(callback);
        }

        /** Our own server-issued client id (available after first 'hello'). */
        get clientId()    { return this._myId; }
        /** Our own server-issued cursor colour. */
        get clientColor() { return this._myColor; }

        // ---------------------------------------------------------------
        // Internals -- networking
        // ---------------------------------------------------------------

        _appendAuthQuery(url, token) {
            // Append ``?auth_token=<token>`` (or ``&auth_token=...`` when
            // the URL already has a query string), URL-encoding the token
            // value so non-ASCII or punctuation-bearing secrets stay
            // intact on the wire. Falls back to a naive concatenation
            // if the URL is not parseable as a real URL (some test
            // doubles pass mock strings).
            const enc = (typeof encodeURIComponent === 'function')
                ? encodeURIComponent(token) : token;
            // Find the position of an existing query or fragment so we
            // can splice in the param before the fragment if present.
            const fragIdx = url.indexOf('#');
            const head    = fragIdx >= 0 ? url.slice(0, fragIdx) : url;
            const frag    = fragIdx >= 0 ? url.slice(fragIdx)    : '';
            const joiner  = head.indexOf('?') >= 0 ? '&' : '?';
            return head + joiner + 'auth_token=' + enc + frag;
        }

        _connect() {
            if (this._stopped) return;
            let ws;
            try {
                // Use the URL with the auth_token query parameter appended
                // (computed once at construction). When no token is
                // configured _wsUrl === url, so this is a no-op cost.
                ws = new WebSocket(this._wsUrl);
            } catch (_) {
                // Invalid URL or browser blocked the constructor.
                // Schedule a retry just like onclose would.
                if (this._autoReconnect && !this._stopped) {
                    setTimeout(() => this._connect(), this._reconnectDelay);
                    this._reconnectDelay = Math.min(
                        this._reconnectDelay * 2,
                        this._maxReconnectDelay
                    );
                }
                return;
            }
            this._ws = ws;

            ws.onopen = () => {
                this._connected = true;
                this._reconnectDelay = this._baseReconnectDelay;
                this._notifyStatus(true);
                // Auth handshake: if a token was configured, send a
                // first-frame {type:'auth', token:...} before anything
                // else. The gateway accepts EITHER this or the URL
                // query parameter (we send both for resilience -- the
                // URL form is the fast path, this frame is the safety
                // net for environments where the URL query is stripped
                // by a proxy or rewritten by the page's WS shim).
                if (this._authToken) {
                    try {
                        ws.send(JSON.stringify({
                            type:  'auth',
                            token: this._authToken,
                        }));
                    } catch (_) { /* will be retried on next reconnect */ }
                }
                // Flush anything queued during disconnect.
                if (this._pendingSend.length) {
                    const pending = this._pendingSend.splice(0);
                    for (const m of pending) {
                        try { ws.send(m); } catch (_) {}
                    }
                }
            };

            ws.onmessage = (ev) => this._receive(ev.data);

            ws.onclose = () => {
                this._connected   = false;
                // Note: we do NOT reset _hasSnapshot on disconnect. The
                // offline cache (or the last server snapshot) is still
                // valid; ready() should keep returning the resolved
                // promise so re-mounts of the UI don't suddenly block.
                this._myId        = null;   // server will mint a new id
                this._myColor     = null;
                this._notifyStatus(false);
                if (this._autoReconnect && !this._stopped) {
                    setTimeout(() => this._connect(), this._reconnectDelay);
                    this._reconnectDelay = Math.min(
                        this._reconnectDelay * 2,
                        this._maxReconnectDelay
                    );
                }
            };

            // onerror is followed by onclose; let the latter handle reconnect.
            ws.onerror = () => {};
        }

        _send(obj) {
            let raw;
            try {
                raw = JSON.stringify(obj);
            } catch (_) {
                // Unserialisable value (circular reference, BigInt, etc).
                // The CRDT layer cannot accept it anyway; drop silently.
                return;
            }
            if (this._ws && this._ws.readyState === WebSocket.OPEN) {
                try {
                    this._ws.send(raw);
                    return;
                } catch (_) {
                    // fall through and queue
                }
            }
            this._pendingSend.push(raw);
        }

        _receive(raw) {
            let msg;
            try { msg = JSON.parse(raw); } catch (_) { return; }
            if (!msg || typeof msg !== 'object') return;

            switch (msg.type) {
                case 'hello':
                    this._myId    = msg.id;
                    this._myColor = msg.color;
                    break;
                case 'snapshot':
                    this._applySnapshot(msg.data || {});
                    // Share the freshly received snapshot with sibling
                    // tabs (e.g. a sibling that opened slightly later
                    // and asked us for state).
                    this._broadcastLocal({
                        type: 'snapshot', data: this.snapshot(),
                    });
                    break;
                case 'set':
                    this._applySet(msg.key, msg.value, 'server');
                    // Mirror gateway-originated changes to siblings so
                    // they don't have to wait for their own WS to
                    // deliver the same message.
                    this._broadcastLocal({
                        type: 'set', key: msg.key, value: msg.value,
                    });
                    break;
                case 'delete':
                    this._applyDelete(msg.key, 'server');
                    this._broadcastLocal({ type: 'delete', key: msg.key });
                    break;
                case 'presence':
                    for (const h of this._presenceWatchers) {
                        try { h(msg.id, msg.x, msg.y, msg.color); }
                        catch (e) { console.error('aether presence watcher error:', e); }
                    }
                    this._broadcastLocal({
                        type:  'presence',
                        id:    msg.id,
                        color: msg.color,
                        x:     msg.x,
                        y:     msg.y,
                    });
                    break;
                case 'presence-leave':
                    for (const h of this._leaveWatchers) {
                        try { h(msg.id); }
                        catch (e) { console.error('aether leave watcher error:', e); }
                    }
                    this._broadcastLocal({
                        type: 'presence-leave', id: msg.id,
                    });
                    break;
                // Unknown types: ignore for forward-compatibility.
            }
        }

        _receiveLocal(msg) {
            // Inbound from a sibling tab via BroadcastChannel. We must
            // apply locally but NEVER re-broadcast (echo storm) and
            // NEVER forward to the WebSocket (the originating tab
            // already did or will).
            if (!msg || typeof msg !== 'object') return;
            if (msg.from === this._tabId) return;   // our own echo

            switch (msg.type) {
                case 'set':
                    this._applySet(msg.key, msg.value, 'peer');
                    break;
                case 'delete':
                    this._applyDelete(msg.key, 'peer');
                    break;
                case 'presence':
                    for (const h of this._presenceWatchers) {
                        try { h(msg.id, msg.x, msg.y, msg.color); }
                        catch (e) { console.error('aether presence watcher error:', e); }
                    }
                    break;
                case 'presence-leave':
                    for (const h of this._leaveWatchers) {
                        try { h(msg.id); }
                        catch (e) { console.error('aether leave watcher error:', e); }
                    }
                    break;
                case 'snapshot-request':
                    // A sibling tab just opened and has no state yet.
                    // Share what we have so they don't have to wait
                    // for the gateway round-trip. We respond if we
                    // either have an authoritative snapshot OR have
                    // any local state from optimistic writes / cache.
                    if ((this._hasSnapshot || this._state.size > 0) && this._bc) {
                        try {
                            this._bc.postMessage({
                                type: 'snapshot',
                                from: this._tabId,
                                data: this.snapshot(),
                            });
                        } catch (_) {}
                    }
                    break;
                case 'snapshot':
                    // Accept a sibling's snapshot if we don't yet have
                    // an authoritative one. (Both server-side and
                    // cache-hydrated counts as "authoritative" here --
                    // _hasSnapshot is set by both paths.)
                    if (!this._hasSnapshot) {
                        this._applySnapshot(msg.data || {});
                    }
                    break;
            }
        }

        _broadcastLocal(payload) {
            // Tag with our tab id so we can recognise (and skip) our
            // own echo when the BC fans the message back to us.
            if (!this._bc) return;
            try {
                this._bc.postMessage(
                    Object.assign({ from: this._tabId }, payload)
                );
            } catch (_) {
                // postMessage can fail with DataCloneError on weird
                // values (functions, etc.). Aether values are JSON-safe
                // by contract, so this should never fire in practice.
            }
        }

        // ---------------------------------------------------------------
        // Internals -- state application
        // ---------------------------------------------------------------

        _applySnapshot(data) {
            const incoming = new Map(Object.entries(data));
            const changed = [];

            // Detect removals.
            for (const [k, oldV] of this._state) {
                if (!incoming.has(k)) {
                    this._state.delete(k);
                    changed.push([k, undefined, oldV]);
                }
            }
            // Detect adds + updates.
            for (const [k, v] of incoming) {
                const oldV = this._state.get(k);
                if (!this._state.has(k) || !this._equal(oldV, v)) {
                    this._state.set(k, v);
                    changed.push([k, v, oldV]);
                }
            }

            for (const [k, nv, ov] of changed) this._fire(k, nv, ov);

            this._hasSnapshot = true;
            const resolvers = this._readyResolvers.splice(0);
            for (const r of resolvers) r();

            // Persist the now-authoritative state.
            this._scheduleSave();
        }

        _applySet(key, value, origin) {
            if (typeof key !== 'string') return;
            // origin = 'local' (own optimistic), 'server' (gateway echo,
            // authoritative), or 'peer' (sibling tab via BroadcastChannel,
            // a hint but not yet authoritative). Default 'server' so a
            // missing argument is the conservative choice.
            origin = origin || 'server';
            const oldV = this._state.get(key);
            if (this._equal(oldV, value)) {
                // Even on a no-op, if THIS apply matches a pending local
                // write AND is authoritative, we can clear pending: the
                // gateway has echoed the value we tried to write.
                if (origin === 'server') this._resolvePending(key, value);
                return;  // no-op
            }
            // Authoritative remote change that differs from current. If
            // we have a pending local write for this key and the incoming
            // value disagrees with what we tried, the optimistic write
            // just lost the HLC race -- fire supersede BEFORE we clobber
            // the value so subscribers see both (attempted, actual).
            if (origin === 'server') this._maybeFireSupersede(key, value);
            this._state.set(key, value);
            this._fire(key, value, oldV);
            this._scheduleSave();
        }

        _applyDelete(key, origin) {
            if (typeof key !== 'string') return;
            origin = origin || 'server';
            if (!this._state.has(key)) {
                if (origin === 'server') this._resolvePending(key, undefined);
                return;
            }
            const oldV = this._state.get(key);
            if (origin === 'server') this._maybeFireSupersede(key, undefined);
            this._state.delete(key);
            this._fire(key, undefined, oldV);
            this._scheduleSave();
        }

        // ---------------------------------------------------------------
        // Internals -- supersede tracking ("did my write win?")
        // ---------------------------------------------------------------

        _trackPending(key, attempted) {
            const now = (typeof performance !== 'undefined' && performance.now)
                ? performance.now() : Date.now();
            // Drop oldest if we are at the cap. Map preserves insertion
            // order, so the first key is the oldest.
            if (this._pendingLocal.size >= MAX_PENDING_LOCAL) {
                const oldest = this._pendingLocal.keys().next().value;
                if (oldest !== undefined) this._pendingLocal.delete(oldest);
            }
            this._pendingLocal.set(key, { value: attempted, ts: now });
            this._scheduleSupersedeSweep();
        }

        _resolvePending(key, finalValue) {
            const pending = this._pendingLocal.get(key);
            if (!pending) return;
            // The arriving value matches what we tried to write, OR the
            // caller (a remote apply or a delete-on-empty) wants to clear
            // the slot. Either way, the optimistic write is settled.
            this._pendingLocal.delete(key);
            // (We don't fire a "your write was accepted" event yet --
            // the API surface stays small until someone asks for it.)
            void pending;
        }

        _maybeFireSupersede(key, finalValue) {
            const pending = this._pendingLocal.get(key);
            if (!pending) return;
            // The remote-originated value differs from what we have right
            // now. If it also differs from what we *tried* to write, our
            // write effectively lost. (If it equals what we tried, the
            // gateway is just echoing our write back -- not a supersede,
            // resolve quietly.)
            if (this._equal(pending.value, finalValue)) {
                this._pendingLocal.delete(key);
                return;
            }
            // Clear the pending entry FIRST so a subscriber that calls
            // set() inside the handler does not re-enter and double-fire.
            const attempted = pending.value;
            this._pendingLocal.delete(key);
            for (const h of this._supersedeWatchers) {
                try { h(key, attempted, finalValue); }
                catch (e) { console.error('aether supersede watcher error:', e); }
            }
        }

        _scheduleSupersedeSweep() {
            if (this._supersedeSweepTimer != null) return;
            // Sweep once when the window closes; cheap and bounded.
            this._supersedeSweepTimer = setTimeout(() => {
                this._supersedeSweepTimer = null;
                this._sweepPending();
            }, SUPERSEDE_WINDOW_MS);
        }

        _sweepPending() {
            if (this._pendingLocal.size === 0) return;
            const now = (typeof performance !== 'undefined' && performance.now)
                ? performance.now() : Date.now();
            const cutoff = now - SUPERSEDE_WINDOW_MS;
            for (const [k, entry] of this._pendingLocal) {
                if (entry.ts < cutoff) {
                    // Expired without ever seeing a remote echo. We
                    // assume the write was accepted (the gateway would
                    // have echoed it back otherwise) and drop the entry.
                    this._pendingLocal.delete(k);
                }
            }
            // If anything remains, schedule another sweep so we eventually
            // drain the map.
            if (this._pendingLocal.size > 0) this._scheduleSupersedeSweep();
        }

        _fire(key, newValue, oldValue) {
            const handlers = this._watchers.get(key);
            if (handlers) {
                for (const h of handlers) {
                    try { h(newValue, oldValue); }
                    catch (e) { console.error('aether watcher error:', e); }
                }
            }
            for (const h of this._anyWatchers) {
                try { h(key, newValue, oldValue); }
                catch (e) { console.error('aether watcher error:', e); }
            }
        }

        _notifyStatus(connected) {
            for (const h of this._statusWatchers) {
                try { h(connected); }
                catch (e) { console.error('aether status watcher error:', e); }
            }
        }

        _equal(a, b) {
            if (a === b) return true;
            // Primitive fast path covered above; for objects do a shallow
            // JSON compare. Cheap and good enough for change detection.
            if (a === null || b === null) return false;
            if (typeof a !== 'object' || typeof b !== 'object') return false;
            try { return JSON.stringify(a) === JSON.stringify(b); }
            catch (_) { return false; }
        }

        // ---------------------------------------------------------------
        // Internals -- offline-first persistence
        // ---------------------------------------------------------------

        _loadFromStorage() {
            if (!this._storageOk) return;
            let raw;
            try {
                raw = window.localStorage.getItem(this._cacheKey);
            } catch (_) {
                // Access can throw in restrictive iframes or after
                // disabling site data mid-session.
                return;
            }
            if (!raw) return;

            let parsed;
            try {
                parsed = JSON.parse(raw);
            } catch (_) {
                // Cache corrupt -- drop and let the gateway resnapshot us.
                try { window.localStorage.removeItem(this._cacheKey); }
                catch (_) {}
                return;
            }
            if (!parsed || typeof parsed !== 'object') return;
            if (parsed.v !== CACHE_VERSION) return;
            if (!parsed.data || typeof parsed.data !== 'object') return;

            // Hydrate the state Map. Note: we do NOT fire watchers
            // here -- the constructor has not yet returned, so nobody
            // can have subscribed. Callers consume the loaded state
            // via aether.ready().then(initialRender), which pulls
            // values out by key.
            for (const k of Object.keys(parsed.data)) {
                if (typeof k !== 'string') continue;
                this._state.set(k, parsed.data[k]);
            }

            // Mark as "we have something showable". This makes
            // ready() resolve immediately even if the WS is down.
            // The eventual gateway snapshot will diff against this
            // state and patch any deltas via _fire().
            this._hasSnapshot = true;
            // (No resolvers to fire yet -- ready() hasn't been called.)
        }

        _scheduleSave() {
            if (!this._storageOk) return;
            if (this._saveTimer !== null) return;
            this._saveTimer = setTimeout(() => {
                this._saveTimer = null;
                this._saveNow();
            }, CACHE_DEBOUNCE_MS);
        }

        _saveNow() {
            if (!this._storageOk) return;
            const data = {};
            for (const [k, v] of this._state) data[k] = v;
            let serialized;
            try {
                serialized = JSON.stringify({ v: CACHE_VERSION, data: data });
            } catch (_) {
                // Unserialisable value somewhere in the state. We
                // should never get here because Aether values are
                // JSON by contract, but guard anyway.
                return;
            }
            try {
                window.localStorage.setItem(this._cacheKey, serialized);
            } catch (_) {
                // QuotaExceededError, SecurityError (private mode in
                // some browsers), etc. Drop on the floor; in-memory
                // state is still correct, only durability across
                // refresh is forfeit. Disable further attempts so
                // we do not retry on every mutation.
                this._storageOk = false;
            }
        }
    }

    // UMD-style export: window.Aether, CommonJS, and ESM-via-script-tag.
    if (typeof module !== 'undefined' && module.exports) {
        module.exports = Aether;
    } else {
        root.Aether = Aether;
    }
})(typeof window !== 'undefined' ? window : globalThis);
