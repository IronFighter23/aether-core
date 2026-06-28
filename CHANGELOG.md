# Changelog

All notable changes to this project will be documented in this file.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/)
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.4.0] — 2026-06-27

Follow-up to v0.3.0 (security hardening). This release closes the
most-asked-for gaps that remained after 0.3.0 shipped: no
authentication, the two unbounded in-memory structures, no
first-class React story, and no clear answer to "what happens to my
write when LWW picks someone else's value?" It is
**backward-compatible** — every prior API keeps working, every new
field is opt-in.

### Added

- **Shared-secret authentication** (`AuthConfig`). Optional and
  fail-closed. When configured on a `ClientGateway` it is required
  before the gateway sends `hello` or `snapshot` — a rejected client
  never sees a single byte of state. When configured on a `MeshNode`
  both peers must present matching tokens during their `hello`
  handshake or the federation channel is refused. Token comparison
  runs through `hmac.compare_digest` so a timing oracle cannot
  recover the secret. Empty string and `None` are both treated as
  "no auth" so a misconfigured env var does not silently produce a
  one-credential-only gateway.
- **TLS / `wss://` support.** Both `ClientGateway` and `MeshNode`
  accept an `ssl_context` parameter that is passed through to
  `websockets.serve()` and `connect()`. The `url` property of the
  gateway flips from `ws://` to `wss://` automatically.
- **Bounded HLC dedup cache (`SeenStampCache`).** `MeshNode._seen`
  is now FIFO-bounded by `SecurityLimits.max_seen_stamps` (default
  100 000). Long-running federations no longer leak memory in
  proportion to operation count. CRDT idempotence guarantees that
  evicted-then-reseen stamps remain a no-op for correctness.
- **Bounded operation log.** `Node.oplog` is now backed by
  `collections.deque(maxlen=...)` with a default cap of 10 000
  entries (configurable via `SecurityLimits.max_oplog_size` or the
  `max_oplog_size` parameter on `Node`). Durability still lives in
  the `ChronoLedger`, not the in-memory oplog. Pass
  `max_oplog_size=None` to opt back into the legacy unbounded list
  for short test runs.
- **`onSupersede` callback (JavaScript).** Subscribe to be told
  when one of your local writes lost the LWW race to a concurrent
  writer's higher-HLC operation within a 10-second window. The
  callback receives `(key, attemptedValue, actualValue)`. This is
  the visibility layer for LWW conflict resolution — the math
  doesn't drop writes, but it does sometimes pick someone else's
  as the final value, and now your UI can react.
- **`authToken` option (JavaScript).** New constructor option;
  delivered to the gateway both as a `?auth_token=...` URL query
  parameter (the fast path) AND as a first-frame
  `{type:"auth",token:"..."}` message (the safety net for proxies
  that strip query strings).
- **Official React bindings** (`@nishantbhatte/aether-core/react`):
    - `useAether(key, defaultValue, options)` — `useState`-shaped
      hook bound to a single CRDT key. Re-renders only when that
      one key changes.
    - `useAetherSnapshot(options)` — whole-state hook with
      memoised `setKey`/`deleteKey`. Re-renders on any change.
    - `useAetherSupersede(callback, deps)` — subscribe to the
      supersede callback from inside a component.
    - `configureAether(config)` / `getAether(opts)` — app-wide
      default URL/auth and imperative access for event handlers.
- **`@nishantbhatte/aether-core` npm package.** Dual ESM/CJS
  entries, React subpath under `./react`, TypeScript declarations
  for both. `npm pack --dry-run` produces a clean ~20 kB tarball.
- **Two copy-paste examples** under `examples/`:
    - `vanilla-counter/` — 20-line single HTML file.
    - `react-counter/` — 25-line React component.
- **Tests.** `tests/test_auth.py` (16 cases covering token
  semantics, gateway URL-query auth, gateway first-message auth,
  state-leakage prevention, mesh peer auth, asymmetric configs).
  `tests/test_bounded_caches.py` (12 cases covering FIFO eviction,
  default caps, idempotence under eviction). The protocol-conformance
  suite is unchanged and still passes.

### Changed

- `MeshNode._seen` type: `set[HybridLogicalClock]` → `SeenStampCache`.
  External code that called `.add(stamp)` or `stamp in mesh._seen`
  keeps working; `len()` and `clear()` keep working; iteration
  switches from insertion-set order to insertion-FIFO order.
- `ClientGateway.url` returns `wss://...` when `ssl_context=` is
  configured.
- `SecurityLimits` gains two new fields: `max_seen_stamps` (default
  100 000) and `max_oplog_size` (default 10 000).
- Test count: 17 → 45 (16 new auth tests, 12 new bounded-cache tests).

### Fixed

- `MeshNode._seen` no longer grows unbounded. Previously, every
  federated operation added a stamp that was never evicted; a node
  in a busy mesh would OOM in proportion to total operation count.
- `Node.oplog` no longer grows unbounded. Same root cause, same
  shape of fix.

### Security

- New `secure_compare(a, b)` public helper (constant-time string
  comparison via `hmac.compare_digest`). Used internally by
  `AuthConfig.verify` and exposed for downstream code that needs
  the same primitive.
- Gateway authentication runs **before** the snapshot is sent. A
  client that fails auth cannot read a single byte of state. A test
  (`test_gateway_auth_does_not_leak_snapshot_to_rejected_client`)
  pins this property.

### Migration from 0.3.x

No code changes required. Every new feature is opt-in via
additional constructor parameters:

```python
# 0.3.x — unchanged, still works
gateway = ClientGateway(mesh, port=8211)

# 0.4.0 — opt into auth + TLS
gateway = ClientGateway(
    mesh, port=8211,
    auth=AuthConfig(token=os.environ["AETHER_TOKEN"]),
    ssl_context=my_ssl_context,
)
```

```js
// 0.3.x — unchanged, still works
const aether = new Aether('ws://localhost:8211');

// 0.4.0 — opt into auth + supersede visibility
const aether = new Aether('wss://your.host:8211', {
    authToken: process.env.AETHER_TOKEN,
});
aether.onSupersede((key, attempted, actual) => { /* ... */ });
```

## [0.3.0] — 2026-06-26

Security hardening, conformance tests, benchmarks, TypeScript types,
and "killer demos". See the
[GitHub release notes](https://github.com/IronFighter23/aether-core/releases/tag/v0.3.0)
for the full feature list. Headlines:

- New `aether_core/_security.py` with token-bucket rate limiting,
  payload-size caps, connection caps, slow-loris timeout.
- Both `ClientGateway` (browser-facing) and `WebSocketMeshPubSub`
  (peer-facing) enforce the same `SecurityLimits` envelope.
- Documented threat model in `SECURITY.md` with every applied
  mitigation cross-referenced to its enforcement code.
- Conformance test suite pinning the wire protocol so the README
  and code cannot drift.
- Real benchmark numbers in `BENCHMARKS.md`.

## [0.2.0] — earlier

Initial Beta. CRDT + HLC + mesh + gateway + chrono-ledger + browser
client + network-topology demo. See git history for the per-commit
detail.

[0.4.0]: https://github.com/IronFighter23/aether-core/releases/tag/v0.4.0
[0.3.0]: https://github.com/IronFighter23/aether-core/releases/tag/v0.3.0
[0.2.0]: https://github.com/IronFighter23/aether-core/releases/tag/v0.2.0
