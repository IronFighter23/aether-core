/*
 * aether.js -- zero-dependency browser client for Aether-Core.
 *
 * Connects to a ClientGateway over WebSocket, maintains a local Map
 * of the shared state, and exposes a tiny three-method API:
 *
 *     const aether = new Aether('ws://localhost:8011');
 *     await aether.ready();           // resolves after the first snapshot
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

    class Aether {
        /**
         * @param {string} url - The gateway WebSocket URL, e.g. 'ws://localhost:8011'.
         * @param {object} [opts]
         * @param {boolean} [opts.autoReconnect=true]
         * @param {number}  [opts.maxReconnectDelayMs=5000]
         */
        constructor(url, opts) {
            opts = opts || {};
            this.url = url;
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
            // own _tabId are ignored. This prevents the echo storm that
            // would otherwise happen when we re-broadcast our own
            // local apply.
            this._tabId = (
                (typeof crypto !== 'undefined' && crypto.randomUUID)
                    ? crypto.randomUUID()
                    : 'tab-' + Math.random().toString(36).slice(2, 10)
            );
            this._bc = null;
            try {
                if (typeof BroadcastChannel !== 'undefined') {
                    // Channel name keyed on the gateway URL so pages
                    // talking to different gateways don't bleed state.
                    this._bc = new BroadcastChannel('aether::' + this.url);
                    this._bc.onmessage = (ev) => this._receiveLocal(ev.data);
                }
            } catch (_) {
                // BroadcastChannel unavailable (very old browsers, some
                // privacy modes). Degrade silently to WebSocket-only.
                this._bc = null;
            }

            // ---- Cross-tab sync via BroadcastChannel ----------------
            // Same-origin browser tabs can talk to each other directly
            // without a WebSocket round-trip. This makes cross-tab sync
            // instantaneous AND survives gateway outages.
            //
            // Each Aether instance gets a per-tab _tabId. Outbound BC
            // messages are tagged with it; inbound messages with our
            // own _tabId are ignored. This prevents the echo storm that
            // would otherwise happen when we re-broadcast our own
            // local apply.
            this._tabId = (
                (typeof crypto !== 'undefined' && crypto.randomUUID)
                    ? crypto.randomUUID()
                    : 'tab-' + Math.random().toString(36).slice(2, 10)
            );
            this._bc = null;
            try {
                if (typeof BroadcastChannel !== 'undefined') {
                    // The channel name is keyed on the gateway URL so
                    // pages talking to different gateways don't bleed
                    // state into each other.
                    this._bc = new BroadcastChannel('aether::' + this.url);
                    this._bc.onmessage = (ev) => this._receiveLocal(ev.data);
                }
            } catch (_) {
                // BroadcastChannel unavailable (very old browsers, some
                // privacy modes). Degrade silently to WebSocket-only.
                this._bc = null;
            }

            this._connect();

            // If any sibling tab already has the snapshot, get it from
            // them right away instead of waiting for our own WebSocket
            // round-trip. They reply via 'snapshot' on the channel.
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
         */
        set(key, value) {
            const k = String(key);
            this._send({ type: 'set', key: k, value: value });
            // Mirror to sibling tabs immediately via BroadcastChannel.
            // The gateway will eventually echo this back too, but the
            // BC fanout is instant and works even when the gateway is
            // unreachable.
            this._broadcastLocal({ type: 'set', key: k, value: value });
            // Apply locally right away so the originating tab sees the
            // change without waiting for the gateway echo. The eventual
            // gateway echo is a no-op because _applySet checks equality.
            this._applySet(k, value);
        }

        /**
         * Delete a key. The Python side records a tombstone so the
         * delete is durable and order-resistant.
         */
        delete(key) {
            const k = String(key);
            this._send({ type: 'delete', key: k });
            this._broadcastLocal({ type: 'delete', key: k });
            this._applyDelete(k);
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
         * Promise that resolves once the gateway has delivered the
         * initial snapshot. After this, ``get`` reads are authoritative.
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
        // Internals
        // ---------------------------------------------------------------

        _connect() {
            if (this._stopped) return;
            const ws = new WebSocket(this.url);
            this._ws = ws;

            ws.onopen = () => {
                this._connected = true;
                this._reconnectDelay = this._baseReconnectDelay;
                this._notifyStatus(true);
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
                this._hasSnapshot = false;
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
            const raw = JSON.stringify(obj);
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
                    this._applySet(msg.key, msg.value);
                    // Mirror gateway-originated changes to siblings so
                    // they don't have to wait for their own WS to
                    // deliver the same message.
                    this._broadcastLocal({
                        type: 'set', key: msg.key, value: msg.value,
                    });
                    break;
                case 'delete':
                    this._applyDelete(msg.key);
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
                    this._applySet(msg.key, msg.value);
                    break;
                case 'delete':
                    this._applyDelete(msg.key);
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
                    // any local state from optimistic writes -- so
                    // siblings can sync even when the gateway has
                    // never been reachable.
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
                    // Only accept a sibling's snapshot if we don't yet
                    // have one from the gateway. Once the gateway
                    // delivers our own snapshot, it is authoritative.
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
        }

        _applySet(key, value) {
            if (typeof key !== 'string') return;
            const oldV = this._state.get(key);
            if (this._equal(oldV, value)) return;  // no-op
            this._state.set(key, value);
            this._fire(key, value, oldV);
        }

        _applyDelete(key) {
            if (typeof key !== 'string') return;
            if (!this._state.has(key)) return;
            const oldV = this._state.get(key);
            this._state.delete(key);
            this._fire(key, undefined, oldV);
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
    }

    // UMD-style export: window.Aether, CommonJS, and ESM-via-script-tag.
    if (typeof module !== 'undefined' && module.exports) {
        module.exports = Aether;
    } else {
        root.Aether = Aether;
    }
})(typeof window !== 'undefined' ? window : globalThis);
