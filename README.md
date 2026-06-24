# Aether-Core

> **Skip the APIs—your frontend variables are the database.**

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

1. **Relay.** Forward CRDT operations between connected browser tabs
   and (optionally) between federated Python nodes.
2. **Persist.** Append each operation to an immutable JSON-lines file
   so state survives restarts.
3. **Replay.** On boot, feed the ledger back into the same CRDT engine
   so every replica reconstructs identical state.

That's the entire job. Adding a new feature to your application means
adding a new variable in your frontend — never a migration, an endpoint,
a controller, a DTO, or a serializer.

---

## What the Python server is *not*

To prevent the confusion that has historically dogged "serverless" pitches,
here is an explicit list of things the Python server in Aether-Core
**does not do**:

* It does **not** expose a REST or GraphQL API.
* It does **not** know what a "user" or a "device" or a "topology link"
  is. To the relay, every key is opaque bytes.
* It does **not** validate, transform, or interpret payload values.
* It does **not** run queries — there is no query language and no indexes.
* It does **not** enforce business rules. All invariants are enforced
  client-side, because the client is the only place that knows what the
  rules are.
* It does **not** require a database. The "database" is an append-only
  JSONL file.

What it *does* do, in code, fits in three small modules:

| Module                | What it does                                              | Lines |
|-----------------------|-----------------------------------------------------------|------:|
| `aether_core/mesh.py` | `MeshPubSub` driver: gossip CRDT ops between Python nodes | ~480  |
| `aether_core/gateway.py` | `ClientGateway`: relay CRDT ops between browser tabs    | ~480  |
| `aether_core/storage.py` | `ChronoLedger`: append-only JSONL persistence           | ~440  |

Everything else is supporting structure: the CRDT engine itself (math),
the log-compaction worker (cleanup), and the demo whiteboard application
(an example of building on the stack).

---

## Architecture — separation of concerns

V2 introduces a strict Adapter-pattern boundary between **client-facing**
traffic and **server-to-server federation**. The three relay/storage
components do not reach across each other's boundaries:

```
   ┌─────────────────────┐                ┌─────────────────────┐
   │   browser tab 1     │                │   browser tab 2     │
   │   (aether.js +      │  ws://*:8211   │   (aether.js +      │
   │    localStorage)    │ ──────┐  ┌──── │    localStorage)    │
   └─────────────────────┘       ▼  ▼     └─────────────────────┘

                       ┌──────────────────────┐
                       │   ClientGateway      │   ← browser <-> server ONLY
                       │   (gateway.py)       │
                       └──────────┬───────────┘
                                  │ on_op
                                  ▼
                       ┌──────────────────────┐
                       │   MeshNode           │   ← orchestrator:
                       │   (mesh.py)          │     - owns CRDT replica
                       └──────┬─────────┬─────┘     - HLC dedup
                              │         │           - epidemic relay
                              ▼         ▼
                     ┌────────────┐  ┌─────────────────────┐
                     │ChronoLedger│  │  MeshPubSub driver  │ ← server <-> server ONLY
                     │(storage.py)│  │  (WebSocketMeshPubSub
                     └─────┬──────┘  │   or any other      │
                           │ fsync   │   implementation)   │
                           ▼         └─────────┬───────────┘
                  ledger_demo.jsonl            │ ws://*:8201
                                               ▼
                                       (other federated nodes)
```

* **`ClientGateway`** speaks the browser protocol and only the browser
  protocol. It cannot dial other Python nodes.
* **`MeshPubSub`** is an abstract base class. The default driver,
  `WebSocketMeshPubSub`, speaks WebSocket gossip to other Python nodes.
  Swap it for a Redis-, NATS-, or in-process implementation without
  touching anything above.
* **`MeshNode`** is the only component that knows about CRDT semantics.
  It receives ops from either side, dedups by HLC, merges into the local
  replica, and fans the op back out to whichever side it didn't come from.
* **`ChronoLedger`** is a single-writer task that persists every op to
  disk asynchronously, with explicit thread-safety guarantees.

---

## Quick start

```bash
git clone https://github.com/IronFighter23/aether-core.git
cd aether-core
pip install -r requirements.txt

python run_demo.py
```

Open <http://localhost:8080/> in two browser tabs.

* Click a device in the left palette to add it to the canvas.
* Drag it around — the other tab follows in real time.
* Select a device, click **Connect**, then click another device to wire them.
* Change a link's speed in the right panel — both tabs update instantly.
* Kill the process (Ctrl+C), restart it, refresh the page — every device
  and link comes back. State is in `ledger_demo.jsonl`.
* **Kill the server with devices on screen, then refresh the tab** —
  the topology still renders from `localStorage`. The browser is now an
  offline-first cache; the server is the relay/durability tier.

---

## Layer-by-layer

### Phase 1 :: CRDT engine (`aether_core/crdt.py`)

The mathematical core. Three primitives:

* **`HybridLogicalClock`** — a triple `(physical_ns, logical, node_id)`
  forming a strict total order over distributed events. Wall-clock
  anchored, monotonic across clock skew, and unique per node-pair
  forever.
* **`LWWRegister[T]`** — a value plus its HLC stamp plus a tombstone
  flag. `merge(a, b) = other if other.stamp > self.stamp else self`.
  Commutative, associative, idempotent.
* **`LWWMap[K, V]`** — a dict of registers. Deletes are tombstoned,
  not removed, so late-arriving writes cannot resurrect dead keys.

Run the self-test: `python -m aether_core.crdt`

### Phase 2 :: Mesh PubSub (`aether_core/mesh.py`)

V2 splits this layer into two:

* **`MeshPubSub`** — abstract driver. Defines the federation contract:
  `start`, `stop`, `connect_to`, `publish`, `set_on_remote_op`,
  `peer_ids`. Knows nothing about CRDTs.
* **`WebSocketMeshPubSub`** — the default driver. Epidemic gossip over
  WebSockets with hello-handshake peer identification and duplicate-
  channel detection.
* **`MeshNode`** — the orchestrator that owns a CRDT `Node`, installs
  itself as the driver's `on_remote_op` handler, dedups by HLC,
  applies operations to the CRDT, fans out to subscribers, and
  re-publishes via the driver excluding the sender (epidemic gossip).

```python
import asyncio
from aether_core import MeshNode

async def main():
    alpha = MeshNode("alpha", port=8001)
    beta  = MeshNode("beta",  port=8002)
    await alpha.start(); await beta.start()
    await alpha.connect_to("127.0.0.1", 8002)

    await alpha.set("key", "value")
    await asyncio.sleep(0.1)
    assert beta.get("key") == "value"

asyncio.run(main())
```

To plug in a different transport (e.g. a future Redis driver):

```python
from aether_core import MeshNode, MeshPubSub

class RedisPubSub(MeshPubSub):
    async def start(self): ...
    async def stop(self): ...
    async def connect_to(self, host, port): ...
    async def publish(self, op, *, exclude_peer=None): ...
    @property
    def peer_ids(self): ...
    @property
    def host(self): ...
    @property
    def port(self): ...

node = MeshNode("alpha", pubsub=RedisPubSub(...))
```

Run the self-test (linear topology `alpha ↔ beta ↔ gamma`, no direct
alpha-gamma link, relay through beta proven, driver-isolation
asserted): `python -m aether_core.mesh`

### Phase 3 :: Chrono-Vector Storage (`aether_core/storage.py`)

An append-only event ledger. Each operation lands on disk as a single
JSON line via `os.write()` on an `O_APPEND` file descriptor — atomic
per record under POSIX guarantees, followed by `fsync()` for durability.

**Thread / task safety in V2:**

* `boot()` and `close()` are guarded by an `asyncio.Lock` so the fd
  and writer task are managed as one atomic unit. You cannot observe
  a half-initialised ledger.
* Exactly **one** writer task. FIFO ordering of disk writes is
  guaranteed by the underlying `asyncio.Queue`.
* `os.write` / `os.fsync` run on a worker thread via
  `asyncio.to_thread` so the event loop never blocks on disk.
* `on_op()` is fire-and-forget from the caller's perspective. Calling
  it after `close()` is a safe no-op, never raises.
* `close()` is idempotent. A second call returns immediately.

**Crash recovery:**

* A torn final line (process killed mid-write) is detected on boot
  and truncated to the last clean newline. Every fully-flushed
  record before the crash survives.
* Mid-file corruption (a non-parseable JSON line in the middle of
  the ledger) is logged and skipped; replay continues.

Run the self-test (writes, cold-boot, simulated crash recovery,
post-close safety): `python -m aether_core.storage`

### Phase 4 :: Client Gateway (`aether_core/gateway.py` + `web/aether.js`)

The browser-facing WebSocket endpoint. V2 makes the boundary explicit:
this file handles browser ↔ server traffic **only**. It cannot speak
the federation protocol.

* **Snapshot on connect** — new clients immediately receive the
  current state so they start synchronized, not blank.
* **Malformed-input safety** — invalid JSON, missing keys, wrong
  types, all silently dropped. The gateway is a public endpoint and
  must never crash on bad input. (V2 also catches exceptions from
  the mesh write path so a bad mutation cannot terminate the client
  socket.)
* **Auto-reconnect with exponential backoff** in the JS SDK.
* **Ephemeral presence relay** — cursor coordinates fly through the
  gateway but never touch the CRDT or the ledger.

The JS API:

```js
const aether = new Aether('ws://localhost:8211');
await aether.ready();                              // first snapshot delivered

aether.set('counter', 42);
aether.get('counter');                             // -> 42
aether.delete('counter');

const off = aether.on('counter', (newV, oldV) => { /* ... */ });
off();                                             // unsubscribe

aether.onAny((key, newV, oldV) => { /* ... */ });
aether.onStatus(connected => { /* ... */ });
```

Run the self-test (snapshot delivery, bidirectional sync, late-joiner
snapshot, delete propagation, malformed-input survival, end-to-end
persistence): `python -m aether_core.gateway`

### Phase 5 :: Network topology whiteboard (`web/index.html`)

A real application built on the stack. Open in two tabs and you have
a collaborative editor for enterprise network diagrams — add firewalls,
switches, NAS units, application servers, routers, load balancers,
VAPT endpoints and proxies from a palette, drag them around, wire them
together, label them, change link speeds. Every change propagates live
to every other tab, persists in the ledger, **and persists to
`localStorage` for offline-first re-renders**.

State schema:

```
node:<uuid>:type     "firewall" | "switch" | "nas" | "server" | "router" | "lb" | "vapt" | "proxy"
node:<uuid>:label    user-editable display name
node:<uuid>:coords   {x: number, y: number}

link:<uuid>          {from: <uuid>, to: <uuid>, speed: <string>}
```

No frameworks. No build step. Plain HTML + CSS + vanilla JS.

### Phase 6 :: Log compaction (`aether_core/compact.py`)

The append-only ledger grows forever. After tens of thousands of
operations, boot-time replay slows down. `compact.py` is an offline
worker that folds the ledger into a condensed
`<ledger>.snapshot.json` containing the final value (or tombstone)
for each key, plus the HLC stamp that produced it.

```bash
python -m aether_core.compact ledger_demo.jsonl
python -m aether_core.compact ledger_demo.jsonl --rotate
```

`ChronoLedger.boot()` auto-detects the snapshot and replays only ops
newer than its `max_stamp`. Boot time becomes
O(records-since-last-compact) instead of O(records-ever-written).

---

## Project layout

```
aether-core/
├── aether_core/          Python package
│   ├── __init__.py       (public API)
│   ├── crdt.py           CRDT math
│   ├── mesh.py           MeshPubSub driver + MeshNode orchestrator
│   ├── storage.py        Append-only ledger (snapshot-aware)
│   ├── gateway.py        ClientGateway (browser bridge)
│   └── compact.py        Log compaction worker
├── web/                  Browser-facing assets
│   ├── aether.js         Vanilla JS SDK with localStorage cache
│   └── index.html        Topology whiteboard
├── run_demo.py           End-to-end launcher
├── requirements.txt
├── LICENSE
└── README.md
```

---

## Running the tests

```bash
python -m aether_core.crdt       # CRDT convergence
python -m aether_core.mesh       # mesh sync + driver isolation
python -m aether_core.storage    # ledger + crash recovery + post-close safety
python -m aether_core.gateway    # gateway + malformed-input survival
python -m aether_core.compact    # log compaction + snapshot boot
```

All five exit 0 on success.

---

## Design constraints, honoured

* **No external databases.** Not SQLite, not Postgres, not Redis. State
  is variables; history is an `os.write()`-ed JSON-lines file.
* **No centralized server.** Every Python node is a peer; every browser
  tab is a thin client of a peer.
* **No JS frameworks.** No React, Vue, Tailwind, jQuery, or build step.
* **Pure stdlib + websockets.** Python depends only on `websockets>=11.0.3`.

---

## License

MIT — see [`LICENSE`](LICENSE).

---

Built by **Nishant Bhatte** · [@IronFighter23](https://github.com/IronFighter23)
