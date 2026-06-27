/*
 * aether-react.js -- official React bindings for Aether-Core.
 * ===========================================================
 *
 * Two hooks:
 *
 *   - useAether(key, defaultValue, options)
 *       Returns ``[value, setValue, meta]`` for a single key. Re-renders
 *       only when that one key changes. Use this for a counter, a
 *       text input, a single boolean toggle, etc.
 *
 *   - useAetherSnapshot(options)
 *       Returns the full ``{ key: value }`` snapshot plus a memoised
 *       ``setKey(k, v)`` / ``deleteKey(k)`` pair. Re-renders on ANY
 *       change. Use this for a kanban board, a list of users, a
 *       drawing canvas, etc.
 *
 * Both hooks share a single per-URL Aether client instance, created
 * lazily and cleaned up only on full HMR unmount. This means two
 * components that read the same key share one WebSocket and one
 * localStorage cache -- the expensive thing.
 *
 * Usage::
 *
 *     import { useAether } from '@ironfighter23/aether-core/react';
 *
 *     function Counter() {
 *       const [count, setCount] = useAether('count', 0, {
 *         url: 'ws://localhost:8211',
 *       });
 *       return (
 *         <button onClick={() => setCount((count ?? 0) + 1)}>
 *           clicked {count ?? 0} times
 *         </button>
 *       );
 *     }
 *
 * Or, supply ``url`` once via ``configureAether``::
 *
 *     import { configureAether, useAether } from '@ironfighter23/aether-core/react';
 *     configureAether({ url: 'ws://localhost:8211' });
 *     // ...components can now omit { url } from every useAether call.
 *
 * Zero external deps beyond React (>=16.8 for hooks). The Aether class
 * itself is dynamically resolved -- this file is bundler-agnostic so
 * it works equally well in CRA, Vite, Next, and plain ESM imports.
 */
import { useEffect, useRef, useState, useCallback, useSyncExternalStore } from 'react';
import Aether from './aether.js';

// Module-level singleton registry: URL -> Aether instance. Multiple
// useAether() calls with the same URL share one WebSocket and one
// localStorage cache. Cleanup is deferred to page unload because
// React StrictMode aggressively double-mounts components in dev,
// and closing/reopening the WS on every double-mount would be both
// expensive and noisy on the gateway side.
const _clients = new Map();
let _defaultConfig = null;

/**
 * Set defaults for every useAether() call in this app. ``url`` is the
 * only required field; everything else maps onto AetherOptions. Call
 * this once near your app entry point (before any component mounts).
 */
export function configureAether(config) {
    if (!config || typeof config.url !== 'string') {
        throw new TypeError('configureAether: { url } is required');
    }
    _defaultConfig = Object.assign({}, config);
}

function _resolveConfig(opts) {
    const config = Object.assign({}, _defaultConfig || {}, opts || {});
    if (typeof config.url !== 'string' || !config.url) {
        throw new TypeError(
            'useAether: pass { url } in opts, or call configureAether({ url }) once.'
        );
    }
    return config;
}

function _getClient(config) {
    // Cache key is the URL plus any auth_token -- a different token
    // means a different identity, so we must NOT share one socket
    // across two distinct identities.
    const cacheKey = config.url + '|' + (config.authToken || '');
    let client = _clients.get(cacheKey);
    if (!client) {
        const { url, ...opts } = config;
        client = new Aether(url, opts);
        _clients.set(cacheKey, client);
    }
    return client;
}

/**
 * Imperatively grab the singleton Aether client. Useful in event
 * handlers or async callbacks where you don't want to subscribe to
 * re-renders. Idempotent within a single config: calling it twice
 * returns the same client.
 */
export function getAether(opts) {
    return _getClient(_resolveConfig(opts));
}

/**
 * useAether(key, defaultValue, options)
 *
 * @param {string} key      The CRDT key to bind to.
 * @param {*}      [defaultValue]  Value returned while the gateway has
 *   not yet delivered a snapshot AND the offline cache is empty.
 * @param {object} [options]      AetherOptions; ``url`` is required
 *   unless previously set via ``configureAether``.
 * @returns {[value, setValue, meta]} where:
 *   - ``value`` is the current value or ``defaultValue`` while loading.
 *   - ``setValue`` accepts a new value OR a ``(prev) => next`` updater.
 *     Calling with ``undefined`` performs a CRDT delete.
 *   - ``meta`` is ``{ ready, connected, client }``.
 */
export function useAether(key, defaultValue, options) {
    const config = _resolveConfig(options);
    const client = _getClient(config);

    // useSyncExternalStore gives us tear-free reads against the
    // mutable Aether instance. The subscribe() returns an
    // unsubscribe; getSnapshot() returns the *cached* current value.
    // We must memoise getSnapshot so that React's same-reference
    // check works on re-renders that don't change the key.
    const subscribe = useCallback((onChange) => {
        return client.on(key, onChange);
    }, [client, key]);

    const getSnapshot = useCallback(() => {
        // Aether.get() returns ``undefined`` for both "absent" and
        // "deleted" -- we treat both the same way and surface the
        // defaultValue. We do NOT cache defaultValue inside the
        // store because then a parent passing a different default
        // wouldn't see the change.
        return client.get(key);
    }, [client, key]);

    // On the server (SSR) we cannot subscribe; fall back to the
    // default so the markup hydrates without throwing.
    const getServerSnapshot = useCallback(() => undefined, []);

    const raw = useSyncExternalStore(subscribe, getSnapshot, getServerSnapshot);
    const value = raw === undefined ? defaultValue : raw;

    // Connection / ready state. We mirror them through a small piece
    // of local state so meta object identity stays stable until
    // something actually changes.
    const [connected, setConnected] = useState(() => client.connected);
    const [ready, setReady] = useState(false);

    useEffect(() => {
        let cancelled = false;
        client.ready().then(() => { if (!cancelled) setReady(true); });
        const off = client.onStatus(setConnected);
        return () => { cancelled = true; off(); };
    }, [client]);

    // setValue accepts either a raw value or an updater (a la useState).
    // Passing ``undefined`` deletes the key, matching React's "set to
    // undefined = remove from state" idiom in many libraries.
    const setValue = useCallback((next) => {
        const resolved = typeof next === 'function'
            ? next(client.get(key))
            : next;
        if (resolved === undefined) {
            client.delete(key);
        } else {
            client.set(key, resolved);
        }
    }, [client, key]);

    return [value, setValue, { ready, connected, client }];
}

/**
 * useAetherSnapshot(options)
 *
 * Returns the entire ``{ key: value }`` snapshot. Re-renders on every
 * key mutation -- use sparingly, prefer per-key ``useAether`` for
 * isolated reads. The companion ``setKey``/``deleteKey`` callbacks
 * are stable across renders (memoised on the client identity).
 */
export function useAetherSnapshot(options) {
    const config = _resolveConfig(options);
    const client = _getClient(config);

    const versionRef = useRef(0);
    // Forces a re-render by bumping a counter on any change. Using a
    // counter instead of the snapshot object itself avoids stale
    // closures inside getSnapshot and lets useSyncExternalStore use
    // its built-in === check on the bumped integer.
    const subscribe = useCallback((onChange) => {
        const off = client.onAny(() => {
            versionRef.current += 1;
            onChange();
        });
        return off;
    }, [client]);

    const getSnapshot = useCallback(() => versionRef.current, []);
    const getServerSnapshot = useCallback(() => 0, []);

    // Subscribe; the version integer drives re-renders.
    useSyncExternalStore(subscribe, getSnapshot, getServerSnapshot);

    // Read the actual data AFTER the subscription has fired; a fresh
    // object is created on every render which is exactly what callers
    // want for "give me the current state".
    const data = client.snapshot();

    const [connected, setConnected] = useState(() => client.connected);
    const [ready, setReady] = useState(false);

    useEffect(() => {
        let cancelled = false;
        client.ready().then(() => { if (!cancelled) setReady(true); });
        const off = client.onStatus(setConnected);
        return () => { cancelled = true; off(); };
    }, [client]);

    const setKey    = useCallback((k, v) => client.set(k, v), [client]);
    const deleteKey = useCallback((k)    => client.delete(k), [client]);

    return { data, setKey, deleteKey, ready, connected, client };
}

/**
 * useAetherSupersede(callback, deps)
 *
 * Subscribe to "your write lost the LWW race" events for the current
 * Aether singleton. ``callback(key, attempted, actual)`` is invoked
 * whenever a write you made is superseded by a higher-HLC remote
 * write within the supersede window. ``deps`` works like the deps
 * array for useEffect.
 */
export function useAetherSupersede(callback, deps, options) {
    const config = _resolveConfig(options);
    const client = _getClient(config);
    // eslint-disable-next-line react-hooks/exhaustive-deps
    useEffect(() => client.onSupersede(callback), [client, ...(deps || [])]);
}

export default useAether;
