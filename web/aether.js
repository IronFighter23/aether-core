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

            this._connect();
        }

        // ---------------------------------------------------------------
        // Public API
        // ---------------------------------------------------------------

        /**
         * Write a value. Fires when the gateway echoes the resolved
         * mutation back, so all clients see the same final value.
         */
        set(key, value) {
            this._send({ type: 'set', key: String(key), value: value });
        }

        /**
         * Delete a key. The Python side records a tombstone so the
         * delete is durable and order-resistant.
         */
        delete(key) {
            this._send({ type: 'delete', key: String(key) });
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
        }

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
                this._connected = false;
                this._hasSnapshot = false;
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

            if (msg.type === 'snapshot') {
                this._applySnapshot(msg.data || {});
            } else if (msg.type === 'set') {
                this._applySet(msg.key, msg.value);
            } else if (msg.type === 'delete') {
                this._applyDelete(msg.key);
            }
            // Unknown types: ignore for forward-compatibility.
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
