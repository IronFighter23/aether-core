# Security Policy

This document describes the security posture of Aether-Core, the
protections built into the relay/ledger, the limits operators can tune
per deployment, and the process for reporting vulnerabilities.

The principle behind every decision below: **the Python side of
Aether-Core is a dumb relay**. It does not validate domain rules, run
queries, or interpret values. It has exactly three jobs — relay,
persist, replay — and the security posture is calibrated to make those
three jobs robust against hostile or buggy traffic on both interfaces
(browser ↔ server and server ↔ server).

---

## Threat model

Aether-Core trusts neither browsers nor federated peers. Both can be:

* **Hostile** — actively trying to crash the relay, exhaust memory,
  starve other clients, or smuggle malformed data into the ledger.
* **Buggy** — sending malformed JSON, oversized frames, presence
  updates with the wrong shape, or stuck half-open connections.
* **Slow** — saturating I/O capacity with legitimate but excessive
  traffic, e.g. a runaway client tab that re-sends every keystroke
  individually.

What we do **not** defend against:

* TLS termination — that's the operator's job (run the gateway behind
  nginx/caddy/traefik with a real certificate). Aether-Core itself
  speaks plain `ws://`.
* Authentication / authorisation — Aether-Core has no concept of
  users. If you need auth, terminate it in front of the gateway and
  pass through only authenticated connections.
* Server-side input *meaning*. Validating "is this string a valid
  email address" is a domain concern that lives in your application
  code, not the relay.
* Federated trust models more sophisticated than transport-layer
  acceptance. If you peer two nodes, you trust each other; we do not
  yet support partial-trust mesh topologies.

---

## Mitigations applied

Every protection below is enforced by code in `aether_core/_security.py`
and is unit-tested in `tests/test_protocol_conformance.py`.

### 1. Payload size caps

| Layer | Cap (default) | Enforced by |
|---|---|---|
| WebSocket frame body | 256 KiB | `websockets` library (`max_size=`) — frames over this are rejected before any bytes hit Python's heap |
| JSON message body | 64 KiB | `validate_payload()` — checked **after** UTF-8 decode, **before** `json.loads` |
| Individual CRDT key | 256 bytes (UTF-8) | `validate_key()` |
| Individual CRDT value | 32 KiB (post-serialise) | `validate_value()` |

These prevent a single oversized payload from amplifying into a
memory-exhaustion attack via the JSON parser, the LWWMap, or the
ledger writer.

### 2. Per-connection rate limiting

Each WebSocket connection (both browser-facing and peer-facing) gets
its own [token-bucket](https://en.wikipedia.org/wiki/Token_bucket)
rate limiter. The bucket starts with `messages_burst` tokens and
refills at `messages_per_second`. A message consumes one token.
**Connections that overrun are closed, not back-pressured** — this is
a deliberate choice to protect honest clients from a single noisy
peer.

Defaults: 100 msg/sec sustained, 200 msg burst. Plenty for an
interactive collaborative tool; tight enough that a runaway sender is
disconnected within ~2 seconds.

### 3. Connection caps

| Cap | Default | Why |
|---|---|---|
| Global concurrent connections | 256 | Prevent socket-table exhaustion |
| Per-source-IP connections | 32 | Prevent a single bad actor from monopolising all slots |

When the cap is hit, new connections receive WebSocket close code
**1013 (Try Again Later)**, not silent acceptance. Clients can be
configured to back off and retry.

### 4. Slow-loris timeout

A connection has `handshake_timeout_s` (default 5s) to send its first
message after the WebSocket upgrade completes. Sockets that stay
silent past the deadline are closed.

This applies symmetrically to both transports:
- **Browser → gateway**: the slow-loris guard wraps the first `recv()`
  in the client handler.
- **Peer ↔ peer**: applied to the `hello` exchange in both directions.

### 5. Malformed-input safety

The wire protocol is asserted in `tests/test_protocol_conformance.py`,
which runs a real server and validates that:

- Non-JSON payloads → dropped silently, connection stays open.
- JSON that isn't an object → dropped silently.
- Objects missing `type` → dropped silently.
- `set` missing `key` or with empty string `key` → dropped silently.
- `delete` with a non-string key → dropped silently.
- Unknown message types → dropped silently (forward-compat).
- Over-cap keys / values → rejected, but connection stays open.

The gateway never crashes on bad input. Worst case: the misbehaving
message is logged and dropped, and the offending connection is
eventually killed by the rate limiter if it continues.

### 6. Tombstones, not deletes

Every `delete` operation persists as a tombstone in the LWWMap with
its HLC stamp. A late-arriving stale write that says "key X = Y" is
correctly suppressed by a tombstone with a higher HLC, so deletes
cannot be silently reverted via gossip.

### 7. Crash-safe ledger

The ledger uses `O_APPEND + fsync` per record. POSIX guarantees
atomicity for `os.write()` of records smaller than `PIPE_BUF` (≥4 KiB
everywhere we run), and we cap individual records well under that. A
process killed mid-write leaves at most one torn final record, which
the boot path detects and truncates. **No fully-flushed record is
ever lost.**

---

## Tuning the limits

Every limit lives on `aether_core._security.SecurityLimits` and can be
overridden per deployment:

```python
from aether_core import ClientGateway, MeshNode
from aether_core._security import SecurityLimits

# Higher capacity for a real production cluster:
limits = SecurityLimits(
    messages_per_second        = 500.0,
    messages_burst             = 1_000,
    max_connections_total      = 2_048,
    max_connections_per_source = 128,
    max_frame_bytes            = 1_024 * 1024,  # 1 MiB
    max_message_bytes          = 256 * 1024,    # 256 KiB
    max_value_bytes            = 128 * 1024,
    handshake_timeout_s        = 10.0,
)

mesh    = MeshNode("prod-1", port=8201, limits=limits)
gateway = ClientGateway(mesh, host="0.0.0.0", port=8211, limits=limits)
```

---

## How limits interact with the offline-first cache

The browser-side `aether.js` cache (V2/Phase 3) is intentionally
**independent** of the gateway's limits. If the gateway rejects an
oversized value, the value is **also** not written to `localStorage`
because `_applySet` is never called for rejected mutations. The cache
therefore cannot be poisoned via the gateway.

What the cache *can* hold is whatever the originating tab wrote
optimistically — the same path that triggers `_send()`. If the
oversize payload was rejected, the optimistic local write is *not*
auto-reverted; the tab will fall back to the server's authoritative
state on the next snapshot. (This is the documented limitation noted
in the README Phase 3 section: full durable offline writes are a
future milestone.)

---

## Reporting a vulnerability

Please open a private security advisory on GitHub:

> https://github.com/IronFighter23/aether-core/security/advisories/new

Or, if you prefer email, contact the maintainer through the address
listed in the GitHub profile linked from the repository.

Do **not** open a public issue for security reports until a fix is
available. We aim to acknowledge reports within 72 hours and to ship
a patched release within two weeks for any high-severity issue.

---

## Coverage check

This document is **not** a substitute for the code that enforces these
protections. To verify what is actually implemented, see:

- `aether_core/_security.py` — primitives (token bucket, connection
  counter, payload validators)
- `aether_core/gateway.py` — browser-facing enforcement
- `aether_core/mesh.py` — peer-facing enforcement
- `tests/test_protocol_conformance.py` — assertions that the running
  server actually applies every limit documented above

Run `uv run pytest -v` to verify every protection is live.
