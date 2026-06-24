/*
 * Headless browser-environment simulator for aether.js.
 *
 * Stubs out window, WebSocket, localStorage and BroadcastChannel so we
 * can exercise the offline-first cache paths under Node without a real
 * browser or gateway.
 *
 * Scenarios covered:
 *   1. First-ever load (no cache, no server) -> ready() does NOT resolve.
 *   2. First-ever load with mock server      -> snapshot arrives, ready() resolves,
 *                                                cache written.
 *   3. Refresh with cache (no server)        -> ready() resolves IMMEDIATELY
 *                                                from cache.
 *   4. Mutation while disconnected           -> writes to cache; survives refresh.
 *   5. Cache corruption                      -> handled gracefully, falls back.
 *   6. Cache version mismatch                -> handled gracefully, falls back.
 *   7. localStorage quota exceeded           -> degrades silently, no throw.
 *   8. Cache hot + server snapshot arrives   -> diff applied, watchers fire.
 *   9. Per-gateway-URL cache namespacing     -> two instances on different
 *                                                gateways do not bleed state.
 */
'use strict';

// ----------------------------------------------------------------------
// Stub the browser globals aether.js expects
// ----------------------------------------------------------------------

class MockLocalStorage {
    constructor(opts) {
        opts = opts || {};
        this._store = new Map();
        this._quotaBytes = opts.quotaBytes || Infinity;
        this._currentBytes = 0;
        this._throwOnAccess = !!opts.throwOnAccess;
    }
    get length() { return this._store.size; }
    key(i) { return Array.from(this._store.keys())[i] || null; }
    getItem(k) {
        if (this._throwOnAccess) throw new Error('storage disabled');
        return this._store.has(k) ? this._store.get(k) : null;
    }
    setItem(k, v) {
        if (this._throwOnAccess) throw new Error('storage disabled');
        const sv = String(v);
        const newBytes = (this._store.has(k)
            ? this._currentBytes - this._store.get(k).length
            : this._currentBytes) + sv.length;
        if (newBytes > this._quotaBytes) {
            const err = new Error('QuotaExceededError'); err.name = 'QuotaExceededError'; throw err;
        }
        this._currentBytes = newBytes;
        this._store.set(k, sv);
    }
    removeItem(k) {
        if (this._throwOnAccess) throw new Error('storage disabled');
        if (this._store.has(k)) {
            this._currentBytes -= this._store.get(k).length;
            this._store.delete(k);
        }
    }
    clear() {
        this._store.clear(); this._currentBytes = 0;
    }
}

class MockWebSocket {
    constructor(url) {
        this.url = url;
        this.readyState = 0;   // CONNECTING
        this.onopen = null;
        this.onmessage = null;
        this.onclose = null;
        this.onerror = null;
        this.sent = [];
        // The harness arms this list; each constructed socket pops the
        // next behavior. 'open' opens after a tick; 'fail' fires close
        // after a tick.
        const behavior = MockWebSocket._behaviors.shift() || 'fail';
        setImmediate(() => {
            if (behavior === 'open') {
                this.readyState = 1;
                if (this.onopen) this.onopen({});
                // Optionally deliver a snapshot the harness queued.
                const snap = MockWebSocket._pendingSnapshots.shift();
                if (snap && this.onmessage) {
                    this.onmessage({ data: JSON.stringify({ type: 'hello', id: 'srv', color: 'hsl(0,0%,50%)' }) });
                    this.onmessage({ data: JSON.stringify({ type: 'snapshot', data: snap }) });
                }
            } else {
                this.readyState = 3;
                if (this.onclose) this.onclose({});
            }
        });
    }
    send(raw) { this.sent.push(raw); }
    close() {
        if (this.readyState !== 3) {
            this.readyState = 3;
            if (this.onclose) this.onclose({});
        }
    }
}
MockWebSocket.CONNECTING = 0;
MockWebSocket.OPEN = 1;
MockWebSocket.CLOSING = 2;
MockWebSocket.CLOSED = 3;
MockWebSocket._behaviors = [];
MockWebSocket._pendingSnapshots = [];

function withMockEnv(opts, fn) {
    const ls = new MockLocalStorage(opts.localStorage || {});
    global.window = {
        localStorage: ls,
        WebSocket:    MockWebSocket,
        BroadcastChannel: undefined,
    };
    global.localStorage = ls;
    global.WebSocket    = MockWebSocket;
    // Node 19+ defines `crypto` as a read-only global getter; we use
    // defineProperty to install a writable shim. If it is already
    // writable (older Node), this is also fine.
    try {
        Object.defineProperty(global, 'crypto', {
            value: { randomUUID: () => 'tab-' + Math.random().toString(36).slice(2,10) },
            writable: true, configurable: true,
        });
    } catch (_) { /* already writable; ignore */ }
    MockWebSocket._behaviors = (opts.ws || []).slice();
    MockWebSocket._pendingSnapshots = (opts.snapshots || []).slice();
    delete require.cache[require.resolve('./web/aether.js')];
    const Aether = require('./web/aether.js');
    return fn(Aether, ls);
}

const sleep = (ms) => new Promise(r => setTimeout(r, ms));

// ----------------------------------------------------------------------
// Tests
// ----------------------------------------------------------------------

async function assert(cond, msg) {
    if (!cond) {
        console.error('  FAIL:', msg);
        process.exit(1);
    } else {
        console.log('  ok  :', msg);
    }
}

async function scenario1_first_ever_load_no_server() {
    console.log('\n[1] first-ever load, no cache, server down -> ready() must NOT resolve');
    await withMockEnv({ ws: ['fail', 'fail'] }, async (Aether, ls) => {
        const a = new Aether('ws://localhost:8211');
        let resolved = false;
        a.ready().then(() => { resolved = true; });
        await sleep(50);
        await assert(!resolved, 'ready() correctly stays pending with no cache + no server');
        await assert(a.snapshot && Object.keys(a.snapshot()).length === 0, 'state is empty');
        a.close();
    });
}

async function scenario2_first_load_with_server() {
    console.log('\n[2] first-ever load, no cache, server delivers snapshot -> cache must populate');
    await withMockEnv({
        ws:        ['open'],
        snapshots: [{ 'node:abc:type': 'firewall', 'node:abc:label': 'Edge FW' }],
    }, async (Aether, ls) => {
        const a = new Aether('ws://localhost:8211');
        await a.ready();
        await sleep(120);   // allow debounced save to flush
        await assert(a.get('node:abc:type') === 'firewall', 'state populated by snapshot');
        const cached = ls.getItem('aether::state::ws://localhost:8211');
        await assert(cached !== null, 'cache file written');
        const parsed = JSON.parse(cached);
        await assert(parsed.v === 1, 'cache has version 1');
        await assert(parsed.data['node:abc:type'] === 'firewall', 'cache contains snapshot');
        a.close();
    });
}

async function scenario3_refresh_with_cache_no_server() {
    console.log('\n[3] refresh with cache, server down -> ready() resolves INSTANTLY from cache');
    // Pre-seed localStorage to simulate a previous session.
    await withMockEnv({
        ws: ['fail'],
        localStorage: {},
    }, async (Aether, ls) => {
        ls.setItem('aether::state::ws://localhost:8211', JSON.stringify({
            v: 1,
            data: {
                'node:xyz:type':   'switch',
                'node:xyz:label':  'Core Switch',
                'node:xyz:coords': { x: 500, y: 300 },
            },
        }));
        const t0 = Date.now();
        const a = new Aether('ws://localhost:8211');
        await a.ready();   // must resolve immediately, not wait for WS
        const elapsed = Date.now() - t0;
        await assert(elapsed < 20, 'ready() resolved in <20ms (was ' + elapsed + 'ms) from cache');
        await assert(a.get('node:xyz:type') === 'switch', 'cached node:type loaded');
        await assert(a.get('node:xyz:label') === 'Core Switch', 'cached node:label loaded');
        const coords = a.get('node:xyz:coords');
        await assert(coords && coords.x === 500 && coords.y === 300, 'cached node:coords loaded');
        a.close();
    });
}

async function scenario4_mutation_while_disconnected_survives_refresh() {
    console.log('\n[4] mutate while server down -> survives refresh -> cache hot');
    let cacheBlob = null;
    await withMockEnv({ ws: ['fail'] }, async (Aether, ls) => {
        const a = new Aether('ws://localhost:8211');
        // No cache, no server -> ready does not resolve. But we can
        // still mutate; the writes apply locally and persist.
        a.set('node:new:type', 'router');
        a.set('node:new:coords', { x: 100, y: 100 });
        // Move the node a bunch of times (simulating a drag burst).
        for (let i = 0; i < 20; i++) {
            a.set('node:new:coords', { x: 100 + i, y: 100 + i });
        }
        await sleep(150);   // wait for debounced save
        cacheBlob = ls.getItem('aether::state::ws://localhost:8211');
        await assert(cacheBlob !== null, 'cache written despite server being down');
        const parsed = JSON.parse(cacheBlob);
        await assert(parsed.data['node:new:type'] === 'router', 'router type persisted');
        await assert(parsed.data['node:new:coords'].x === 119, 'final drag position persisted (not intermediate)');
        a.close();
    });
    // Simulate refresh: new aether instance, same localStorage backing.
    await withMockEnv({ ws: ['fail'] }, async (Aether, ls) => {
        ls.setItem('aether::state::ws://localhost:8211', cacheBlob);
        const a = new Aether('ws://localhost:8211');
        await a.ready();
        await assert(a.get('node:new:type') === 'router', 'router survived refresh');
        const coords = a.get('node:new:coords');
        await assert(coords && coords.x === 119, 'final position survived refresh');
        a.close();
    });
}

async function scenario5_corrupt_cache() {
    console.log('\n[5] corrupt cache JSON -> graceful fallback, no throw');
    await withMockEnv({ ws: ['fail'] }, async (Aether, ls) => {
        ls.setItem('aether::state::ws://localhost:8211', 'not json {{');
        const a = new Aether('ws://localhost:8211');   // must not throw
        let resolved = false;
        a.ready().then(() => { resolved = true; });
        await sleep(50);
        await assert(!resolved, 'cache rejected, ready() pending (no fallback state)');
        await assert(ls.getItem('aether::state::ws://localhost:8211') === null,
                     'corrupt cache was removed');
        a.close();
    });
}

async function scenario6_version_mismatch() {
    console.log('\n[6] cache version mismatch -> ignored, ready() waits for server');
    await withMockEnv({ ws: ['fail'] }, async (Aether, ls) => {
        ls.setItem('aether::state::ws://localhost:8211', JSON.stringify({
            v: 99, data: { 'node:old:type': 'firewall' },
        }));
        const a = new Aether('ws://localhost:8211');
        let resolved = false;
        a.ready().then(() => { resolved = true; });
        await sleep(50);
        await assert(!resolved, 'wrong-version cache ignored, ready() pending');
        await assert(a.get('node:old:type') === undefined, 'old cache values not loaded');
        a.close();
    });
}

async function scenario7_quota_exceeded() {
    console.log('\n[7] quota exceeded -> writes silently dropped, in-memory state ok');
    await withMockEnv({
        ws: ['fail'],
        localStorage: { quotaBytes: 50 },   // tiny quota
    }, async (Aether, ls) => {
        const a = new Aether('ws://localhost:8211');
        // Set a value that will easily exceed 50 bytes when wrapped in
        // the cache envelope.
        a.set('node:big:label', 'a'.repeat(200));
        await sleep(120);
        // In-memory state still has the value.
        await assert(a.get('node:big:label').length === 200,
                     'in-memory state survived quota error');
        // Storage either has nothing or an empty/old value -- no throw.
        a.close();
    });
}

async function scenario8_cache_hot_then_snapshot_arrives() {
    console.log('\n[8] cache hot + server snapshot arrives -> diff applied, watchers fire');
    await withMockEnv({
        ws:        ['open'],
        snapshots: [{
            'node:hot:type':  'switch',          // was 'router' in cache
            'node:hot:label': 'Updated Switch',  // was 'Old Router' in cache
            // 'node:gone:*' is missing from snapshot -> must be removed
        }],
    }, async (Aether, ls) => {
        ls.setItem('aether::state::ws://localhost:8211', JSON.stringify({
            v: 1,
            data: {
                'node:hot:type':   'router',
                'node:hot:label':  'Old Router',
                'node:gone:type':  'nas',
                'node:gone:label': 'Will Vanish',
            },
        }));
        const a = new Aether('ws://localhost:8211');
        await a.ready();
        // ready() must resolve from cache (synchronous load).
        await assert(a.get('node:hot:type') === 'router', 'cache loaded first');
        const fired = [];
        a.onAny((key, nv, ov) => fired.push([key, nv, ov]));
        // Now let the mock WS deliver the snapshot.
        await sleep(50);
        await assert(a.get('node:hot:type') === 'switch', 'snapshot updated type');
        await assert(a.get('node:hot:label') === 'Updated Switch', 'snapshot updated label');
        await assert(a.get('node:gone:type') === undefined, 'absent-from-snapshot key was removed');
        await assert(fired.length >= 3, 'watchers fired for diffs (got ' + fired.length + ')');
        // Cache is now refreshed.
        await sleep(120);
        const refreshed = JSON.parse(ls.getItem('aether::state::ws://localhost:8211'));
        await assert(refreshed.data['node:hot:type'] === 'switch', 'cache updated by snapshot');
        await assert(!('node:gone:type' in refreshed.data), 'removed key gone from cache');
        a.close();
    });
}

async function scenario9_per_gateway_isolation() {
    console.log('\n[9] two gateways on same origin -> isolated cache namespaces');
    await withMockEnv({ ws: ['fail', 'fail'] }, async (Aether, ls) => {
        ls.setItem('aether::state::ws://gw1:8211', JSON.stringify({
            v: 1, data: { 'node:gw1:type': 'firewall' },
        }));
        ls.setItem('aether::state::ws://gw2:8211', JSON.stringify({
            v: 1, data: { 'node:gw2:type': 'switch' },
        }));
        const a1 = new Aether('ws://gw1:8211');
        const a2 = new Aether('ws://gw2:8211');
        await a1.ready(); await a2.ready();
        await assert(a1.get('node:gw1:type') === 'firewall', 'gw1 sees only its own cache');
        await assert(a1.get('node:gw2:type') === undefined, 'gw1 does not see gw2 cache');
        await assert(a2.get('node:gw2:type') === 'switch', 'gw2 sees only its own cache');
        await assert(a2.get('node:gw1:type') === undefined, 'gw2 does not see gw1 cache');
        a1.close(); a2.close();
    });
}

(async () => {
    console.log('========================================================================');
    console.log('Aether-Core :: aether.js offline-first persistence :: self-test');
    console.log('========================================================================');

    await scenario1_first_ever_load_no_server();
    await scenario2_first_load_with_server();
    await scenario3_refresh_with_cache_no_server();
    await scenario4_mutation_while_disconnected_survives_refresh();
    await scenario5_corrupt_cache();
    await scenario6_version_mismatch();
    await scenario7_quota_exceeded();
    await scenario8_cache_hot_then_snapshot_arrives();
    await scenario9_per_gateway_isolation();

    console.log('\n========================================================================');
    console.log('OFFLINE-FIRST PERSISTENCE: PROVEN');
    console.log('========================================================================');
})().catch(e => { console.error(e); process.exit(1); });
