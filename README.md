# Aether-Core

> **Skip the APIs—your frontend variables are the database.**

[![tests](https://img.shields.io/badge/tests-45%20passing-brightgreen)](#tests)
[![security](https://img.shields.io/badge/security-auth%20%2B%20rate--limited%20%2B%20bounded-blue)](SECURITY.md)
[![benchmarks](https://img.shields.io/badge/p50%20roundtrip-0.21%20ms-brightgreen)](BENCHMARKS.md)
[![license](https://img.shields.io/badge/license-MIT-lightgrey)](LICENSE)
[![npm](https://img.shields.io/badge/npm-%40nishantbhatte%2Faether--core-cb3837?logo=npm)](https://www.npmjs.com/package/@nishantbhatte/aether-core)

## Install

```bash
pip install aether-zta                      # Python relay + CLI
npm install @nishantbhatte/aether-core      # Browser client + React hooks
```

Then point your client at the relay (`ws://localhost:8211`) — see [Quickstart](#quickstart) below.

---

Aether-Core eliminates backend business logic entirely. The application
lives in the browser: a local CRDT engine holds every piece of state as
ordinary JavaScript variables, and any code that wants to read or write
state just talks to those variables. There is no REST API, no ORM, no
query layer, and no server-side validation pipeline to maintain.

The Python side of Aether-Core is **strictly a dumb relay and ledger**.
It is *not* a backend in the conventional sense. It does not own state,
does not implement features, does not validate domain rules, does not
expose endpoints, and does not interpret payloads. It does exactly three
things:

1. **Relay** — forward CRDT operations between browser tabs and (optionally)
   between federated Python nodes.
2. **Persist** — append each operation to an immutable JSON-lines file
   so state survives restarts.
3. **Replay** — on boot, feed the ledger back into the same CRDT engine
   so every replica reconstructs identical state.

Adding a new feature to your application means adding a new variable
in your frontend — never a migration, an endpoint, a controller, a
DTO, or a serializer.

---

## Quickstart

The recommended quickstart uses [`uv`](https://docs.astral.sh/uv/) — no
global `pip install` required:

```bash
git clone https://github.com/IronFighter23/aether-core
cd aether-core
uv sync                       # creates .venv with locked deps
uv run aether-demo            # start the demos
```

Then open any of:

- <http://localhost:8080/> — **enterprise network topology** (drag-drop graph editor)
- <http://localhost:8080/demos/kanban.html> — **collaborative Kanban board**
- <http://localhost:8080/demos/markdown.html> — **paragraph-keyed Markdown editor with live preview**

Open any URL in two browser tabs to see real-time sync. Stop the
server, refresh — the page renders from `localStorage`.

If you can't (or don't want to) install `uv`:

```bash
python -m venv .venv && source .venv/bin/activate
pip install -e .
python run_demo.py
```

---

## New in 0.4.0

| Feature | What it does |
| --- | --- |
| `AuthConfig(token=...)` | Shared-secret auth for both `ClientGateway` (browsers) and `MeshNode` (federated peers). Constant-time comparison; fail-closed; verified **before** the snapshot is sent so a rejected client never sees any state. |
| `ssl_context=` | TLS / `wss://` for both browser-facing and peer-facing sockets. The gateway `url` switches scheme automatically. |
| `useAether(key, default)` | First-class React hook with the same shape as `useState`. Ships as `@nishantbhatte/aether-core/react` on npm. |
| `onSupersede(cb)` | JavaScript callback that fires when one of your writes lost an LWW race. The math doesn't drop writes; this tells you when one of *yours* wasn't the final value. |
| Bounded `_seen` + `oplog` | Both previously-unbounded dedup/log structures are now FIFO-evicted via `SecurityLimits.max_seen_stamps` (default 100 000) and `max_oplog_size` (default 10 000). Long-running federations no longer leak memory. |

Full list in [`CHANGELOG.md`](CHANGELOG.md). Migration: zero breaking changes — every new field is opt-in.

### React in 25 lines

```jsx
import { useAether } from '@nishantbhatte/aether-core/react';

export default function Counter() {
  const [count, setCount] = useAether('count', 0, {
    url: 'ws://localhost:8211',
  });
  return (
    <button onClick={() => setCount((n) => (n ?? 0) + 1)}>
      clicked {count ?? 0} times
    </button>
  );
}
```

Open in two tabs. They sync. Copy-paste examples live in [`examples/`](examples/).

---

## What the Python server is *not*

To prevent the confusion that has historically dogged "serverless"
pitches, here is an explicit list of things the Python server in
Aether-Core **does not do**:

* It does **not** expose a REST or GraphQL API.
* It does **not** know what a "user" or a "device" or a "kanban card"
  is. To the relay, every key is opaque bytes.
* It does **not** validate, transform, or interpret payload values
  beyond size/type sanity checks needed for DoS resistance.
* It does **not** run queries — there is no query language and no indexes.
* It does **not** enforce business rules. All invariants are enforced
  client-side, because the client is the only place that knows what the
  rules are.
* It does **not** require a database. The "database" is an append-only
  JSONL file.

What it *does* do, in code, fits in four small modules:

| Module | Purpose |
|---|---|
| `aether_core/mesh.py` | `MeshPubSub` driver: gossip CRDT ops between nodes |
| `aether_core/gateway.py` | `ClientGateway`: relay ops between browser tabs (rate-limited, payload-capped) |
| `aether_core/storage.py` | `ChronoLedger`: append-only JSONL persistence |
| `aether_core/_security.py` | Token bucket + connection counter + payload validators |

---

## Three "killer" demos

All three apps are built on the same engine. Open any of them in two
browser tabs to see real-time collaborative editing. Kill the server
and refresh to see offline-first persistence kick in.

### Topology diagrammer (`/`)
Drop firewalls, switches, NAS units, routers, load balancers, VAPT
endpoints, and proxies on a canvas. Wire them with typed links
(1G / 10G / Wi-Fi / dark fiber). Drag, rename, recolour, delete.
Every change syncs to every tab. State schema lives entirely in
JS variables.

### Kanban board (`/demos/kanban.html`)
Columns and cards. Drag cards between columns. Cycle card priority
between low/med/high with a click. Edit titles in place. Delete a
column and all its cards (tombstoned, not silently lost). All
operations are LWW-merged — concurrent edits on the same field
resolve deterministically by HLC.

### Markdown editor (`/demos/markdown.html`)
Each paragraph is a separate CRDT key. Concurrent edits in different
paragraphs **never** conflict; concurrent edits in the same paragraph
LWW-merge to the highest-HLC writer's text. Live HTML preview pane
beside the editor pane. The mini Markdown renderer is vendored and
HTML-escapes all input — hostile collaborators cannot inject scripts.

---

## Documentation

| Guide | Purpose |
|---|---|
| [**Getting Started**](docs/getting-started.md) | 10-minute tutorial: clone → working app → understand how state flowed |
| [**Concepts**](docs/concepts.md) | Mental model: CRDTs, HLCs, tombstones, snapshot vs ledger, what zero-transit means |
| [**JavaScript API**](docs/api-javascript.md) | Complete reference for the browser-side `Aether` class |
| [**Python API**](docs/api-python.md) | Complete reference for `MeshNode`, `ClientGateway`, `ChronoLedger`, `SecurityLimits` |
| [**Recipes**](docs/recipes.md) | Patterns: counters, lists, presence, undo, optimistic UI, schema versioning |
| [**Deployment**](docs/deployment.md) | Production: nginx/Caddy, Docker, systemd, multi-node federation, scaling |
| [**Troubleshooting**](docs/troubleshooting.md) | Common errors and fixes |
| [**SECURITY.md**](SECURITY.md) | Threat model and every applied mitigation |
| [**BENCHMARKS.md**](BENCHMARKS.md) | Real performance numbers with methodology |

---

## CRDT conflict resolution

Aether-Core is a Last-Writer-Wins map (`LWWMap`) keyed by Hybrid Logical
Clock (HLC) stamps. The rules:

* **Every write is stamped** with `(physical_time_ns, logical_counter, node_id)`.
* **Strict total order** — given any two stamps, exactly one is smaller.
  Tuples compare lexicographically; the `node_id` tiebreaker eliminates
  ambiguity even when two nodes' clocks read identical nanoseconds.
* **Merge is `(value, stamp) → max(by stamp)`** — commutative,
  associative, idempotent. Replays are safe. Out-of-order delivery is
  safe. Network partitions are safe.
* **Deletes are tombstones**, not removals. A stale write that says
  "key X = Y" with a stamp older than the delete is correctly
  suppressed — deletes cannot be silently reverted.

What this means in practice for the three demo apps:

* **Topology**: dragging the same node from two tabs simultaneously
  resolves to one final position (the higher HLC wins). The intermediate
  positions are not interleaved.
* **Kanban**: two people changing a card's title at the same time
  resolves to one title. Two people moving the same card to different
  columns simultaneously resolves to one column. Moving a card while
  someone else deletes it: the delete tombstone wins if its stamp is
  higher; otherwise the move wins and the card re-appears in the new
  column.
* **Markdown**: two people typing on **different lines** never
  conflict (each line is its own CRDT key). Two people typing on the
  **same line** at the same time resolves to one final text — the line
  is not character-merged. A character-level CRDT would do that, but
  is out of scope for this engine's "small and provable" charter.

The math is rigorously tested in `python -m aether_core.crdt`, which
hammers it with randomized concurrent operations and asserts all
replicas converge byte-for-byte.

---

## Security model

Aether-Core trusts neither browsers nor federated peers. The full
threat model and every mitigation is in [**SECURITY.md**](SECURITY.md).
In summary:

* **Per-connection rate limiting** (token bucket, default 100 msg/sec).
  Connections that overrun are closed, not back-pressured.
* **Hard payload caps**: 256 KiB WebSocket frame, 64 KiB JSON message,
  256 byte CRDT key, 32 KiB CRDT value.
* **Connection caps**: 256 global, 32 per source IP. Excess connections
  receive WebSocket close code 1013 (Try Again Later).
* **Slow-loris timeout**: 5 second deadline on the first message
  after connect.
* **Tombstones** for deletes — stale writes can't resurrect dead keys.
* **Crash-safe ledger** with `O_APPEND + fsync` per record and
  automatic truncation of torn final writes.

All limits are configurable via the `limits=` constructor parameter on
both `ClientGateway` and `MeshNode`. Defaults are sane for interactive
collaborative tools; production should re-tune.

---

## Wire protocol

```
Browser -> gateway:
    {"type": "set",      "key": "<str>", "value": <json>}
    {"type": "delete",   "key": "<str>"}
    {"type": "presence", "x": <int>, "y": <int>}        # ephemeral cursor

Gateway -> browser:
    {"type": "hello",          "id": "<uuid>", "color": "<hsl>"}    # on connect
    {"type": "snapshot",       "data": {"<key>": <json>, ...}}      # on connect
    {"type": "set",            "key": "<str>", "value": <json>}
    {"type": "delete",         "key": "<str>"}
    {"type": "presence",       "id": "<uuid>", "color": "<hsl>",
                               "x": <int>, "y": <int>}
    {"type": "presence-leave", "id": "<uuid>"}
```

This block is **automatically enforced** by
`tests/test_protocol_conformance.py`, which:

1. Asserts the same block exists verbatim in `aether_core/gateway.py`.
2. Spins up a real gateway and validates the running server emits and
   accepts every message shape, key, and field type the doc claims.

If you change the wire protocol, the conformance test fails. Doc
drift is impossible without breaking CI.

---

## Benchmarks (real numbers)

Captured on a Linux VM, Python 3.12.3, fsync ON. Full report and
methodology in [**BENCHMARKS.md**](BENCHMARKS.md). Reproduce with
`uv run aether-benchmark`.

| Dimension | Number |
|---|---|
| Gateway round-trip (loopback, p50) | **0.21 ms** |
| Gateway round-trip (loopback, p99) | **0.68 ms** |
| Ledger replay throughput | **~86,000 ops/sec** |
| Snapshot-boot speedup | **1.6 – 1.8×** |
| Mesh convergence (5-node ring, 100 writes) | **63 ms** |
| Ledger write throughput (with fsync) | **~1,400 ops/sec** |

---

## TypeScript support

A complete `.d.ts` declaration file ships at `web/aether.d.ts` covering
every method, callback signature, and option. Use via:

```ts
import type { Aether, AetherOptions } from './aether';

const aether: Aether = new Aether('ws://localhost:8211');
aether.set<{ x: number, y: number }>('cursor', { x: 100, y: 200 });
aether.on<number>('counter', (newV, oldV) => { /* ... */ });
```

The declarations pass `tsc --strict --noEmit` cleanly.

---

## Tests

26 automated tests across multiple test files. Run with:

```bash
uv run pytest             # 17 Python tests (in-module demos + protocol conformance)
node test_aether_offline.js   # 9 JS offline-first scenarios
```

Coverage:

* `tests/test_in_module_demos.py` — wraps the 5 in-module `_demo()`
  routines (CRDT convergence, mesh sync, ledger crash recovery,
  gateway snapshot+sync+malformed+oversize, log compaction).
* `tests/test_protocol_conformance.py` — 12 tests validating the wire
  protocol matches the documentation, plus rate-limit / oversize-key /
  oversize-value enforcement against a live server.
* `test_aether_offline.js` — 9 scenarios validating the
  browser-side offline-first persistence layer.

---

## Project layout

```
aether-core/
├── pyproject.toml             ← uv / hatch packaging
├── README.md
├── SECURITY.md                ← threat model + mitigations
├── BENCHMARKS.md              ← real numbers + methodology
├── LICENSE
├── run_demo.py                ← launches all three demos
├── test_aether_offline.js     ← JS offline-first tests
├── aether_core/
│   ├── __init__.py
│   ├── cli.py                 ← entry points: aether-demo, aether-benchmark
│   ├── _security.py           ← token bucket, conn counter, validators
│   ├── crdt.py                ← LWWMap + HLC
│   ├── mesh.py                ← MeshPubSub abstract + WebSocket impl
│   ├── gateway.py             ← browser-facing relay (hardened)
│   ├── storage.py             ← append-only ledger
│   └── compact.py             ← log compaction worker
├── benchmarks/
│   ├── run_benchmarks.py      ← reproducible benchmark suite
│   └── results.json           ← latest run output
├── docs/
│   ├── README.md              ← documentation index
│   ├── getting-started.md     ← 10-minute tutorial
│   ├── concepts.md            ← mental model
│   ├── api-javascript.md      ← browser client reference
│   ├── api-python.md          ← relay reference
│   ├── recipes.md             ← common patterns
│   ├── deployment.md          ← production guide
│   └── troubleshooting.md     ← common issues + fixes
├── tests/
│   ├── test_in_module_demos.py
│   └── test_protocol_conformance.py
└── web/
    ├── aether.js              ← browser client (offline-first cache)
    ├── aether.d.ts            ← TypeScript declarations
    ├── index.html             ← topology demo
    └── demos/
        ├── kanban.html        ← Kanban demo
        └── markdown.html      ← Markdown demo
```

---

## License

MIT — see [LICENSE](LICENSE).

Built by **Nishant Bhatte** · [@IronFighter23](https://github.com/IronFighter23)
