# Aether-Core

> A Zero-Transit Architecture engine. No databases. No APIs. No centralized servers.
> State lives in variables, persists as an immutable event ledger, and synchronizes
> across a P2P mesh — all in pure Python, no external services required.

Aether-Core is a from-scratch experiment in collapsing the traditional web-app
stack (database + ORM + REST API + cache + pub/sub) into a single mathematical
primitive: a **Conflict-Free Replicated Data Type** stamped with a **Hybrid
Logical Clock** and gossiped over WebSockets. Every client is a peer. Every
write is durable. Every replica converges to the same state, regardless of the
order in which it sees mutations.

The repository contains a complete, working implementation in roughly 2,500
lines of Python and 1,300 lines of vanilla HTML/CSS/JS, with no third-party
dependencies beyond the `websockets` library.

---

## What's in the box

A five-layer architecture, each layer built on the one below it:

| Layer | Module | Responsibility |
|------:|:-------|:---------------|
| 1 | `crdt.py` | Hybrid Logical Clock, LWW Register, LWW Map — the deterministic-convergence math |
| 2 | `mesh.py` | P2P WebSocket gossip with epidemic relay and HLC-based deduplication |
| 3 | `storage.py` | Append-only JSONL event ledger with crash-resilient replay and snapshot-aware boot |
| 4 | `gateway.py` | Browser-facing WebSocket bridge plus a vanilla JS SDK (`web/aether.js`) |
| 5 | `web/index.html` | A real application built on the stack: a collaborative network-topology whiteboard |
| 6 | `compact.py` | Offline log compaction worker — folds the ledger into a snapshot for fast boots |

Each layer ships with a runnable self-test that proves its invariants in
isolation; the full stack ships with a launcher that wires everything together.

---

## Quick start

```bash
git clone <this repo> aether-core
cd aether-core
pip install -r requirements.txt

python run_demo.py
```

Then open <http://localhost:8080/> in two browser tabs.

* Click a device in the left palette to add it to the canvas.
* Drag it around — the other tab follows in real time.
* Select a device, click **Connect**, then click another device to wire them.
* Change a link's speed in the right panel — both tabs update instantly.
* Kill the process (Ctrl+C), restart it, refresh the page — every device and
  link comes back. State is in `ledger_demo.jsonl`.

---

## Architecture

```
┌─────────────────────┐                 ┌─────────────────────┐
│   browser tab 1     │  ws://*:8211    │   browser tab 2     │
│   (aether.js)       │ ──────┐  ┌───── │   (aether.js)       │
└─────────────────────┘       ▼  ▼      └─────────────────────┘
                          ┌────────────┐
                          │ Gateway    │  ◄── snapshot on connect
                          │ (Phase 4)  │  ◄── push deltas
                          └─────┬──────┘
                                │ on_op
                                ▼
                          ┌────────────┐
                          │ MeshNode   │  ◄── set/delete from any client
                          │ (Phase 2)  │ ──► ws://*:8201 to peer nodes
                          └─────┬──────┘
                                │ on_op (fanned via compose_hooks)
                ┌───────────────┼───────────────┐
                ▼               ▼               ▼
        ┌──────────────┐ ┌──────────────┐ ┌──────────────┐
        │ ChronoLedger │ │ (other       │ │ Gateway      │
        │ (Phase 3)    │ │  subscribers)│ │ broadcast    │
        └──────┬───────┘ └──────────────┘ └──────────────┘
               │ fsync
               ▼
        ledger_demo.jsonl  (replayed on boot)
```

Every state change in the system flows through exactly one type — `Operation`,
defined in `crdt.py` — stamped with a globally unique `HybridLogicalClock`.
This is what makes every layer compose without coordination: the network
doesn't need to preserve order, the disk doesn't need transactions, and the
client doesn't need to know what CRDTs are. The math handles all of it.

---

## Phase 1 :: CRDT engine (`aether_core/crdt.py`)

The mathematical core. Three primitives:

* **`HybridLogicalClock`** — a triple `(physical_ns, logical, node_id)` that
  forms a strict total order over distributed events. Wall-clock anchored,
  monotonic across clock skew, and unique per node-pair forever.
* **`LWWRegister[T]`** — a value plus its HLC stamp plus a tombstone flag.
  `merge(a, b) = other if other.stamp > self.stamp else self`. Commutative,
  associative, idempotent.
* **`LWWMap[K, V]`** — a dict of registers. Deletes are tombstoned, not
  removed, so late-arriving writes can't resurrect dead keys.

```python
from aether_core import Node

alpha = Node("alpha")
beta  = Node("beta")
gamma = Node("gamma")

# Concurrent writes on three "peers"
alpha.set("user:name", "Aleph")
beta.set("user:name",  "Beta")
gamma.set("user:name", "Gamma")

# Gossip every op to every peer, in random order. CRDT math handles convergence.
for src in [alpha, beta, gamma]:
    for op in src.oplog:
        for dst in [alpha, beta, gamma]:
            if dst.id != src.id and op.stamp.node_id == src.id:
                dst.receive(op)

# All three replicas now hold the exact same value.
assert alpha.store.snapshot() == beta.store.snapshot() == gamma.store.snapshot()
```

Run the self-test: `python -m aether_core.crdt`

## Phase 2 :: P2P mesh (`aether_core/mesh.py`)

A decentralized WebSocket gossip layer. Each `MeshNode` runs a server *and*
maintains outbound connections to known peers. There is no broker, no leader,
no consensus protocol.

* **Hello-handshake** on every connection identifies the remote `node_id`.
  Duplicate channels to the same peer are detected and closed.
* **Epidemic relay** with HLC-based deduplication: every operation reaches
  every reachable node in at most `diameter(mesh)` hops and never re-circulates.
* **Pluggable hook** (`on_op`) for subscribers — used by both the storage layer
  and the browser gateway.

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

Run the self-test (linear topology `alpha ↔ beta ↔ gamma`, no direct
alpha-gamma link, relay through beta proven):

`python -m aether_core.mesh`

## Phase 3 :: Chrono-Vector Storage (`aether_core/storage.py`)

An append-only event ledger. Each operation lands on disk as a single JSON
line via `os.write()` on an `O_APPEND` file descriptor — atomic per record
under POSIX guarantees, followed by `fsync()` for durability.

* **Crash recovery**: a torn final line (process killed mid-write) is detected
  on boot and truncated to the last clean newline.
* **Replay**: on startup, the ledger feeds every stored operation into the
  local CRDT before any networking begins. The CRDT's idempotence means the
  reconstructed state is byte-identical to the pre-crash state, **including
  tombstones**.
* **Async-friendly**: writes go through a single-writer background task fed
  by an `asyncio.Queue`, with `os.write`/`os.fsync` running on a worker
  thread via `asyncio.to_thread`. The event loop never blocks on disk.

Run the self-test (writes, cold-boot, simulated crash recovery):

`python -m aether_core.storage`

## Phase 4 :: Browser gateway (`aether_core/gateway.py` + `web/aether.js`)

A WebSocket endpoint for browser clients. The gateway translates flat
browser-friendly messages into mesh mutations and broadcasts every observed
operation down to every connected tab.

* **Snapshot on connect** — new clients immediately receive the current state
  so they start synchronized, not blank.
* **Auto-reconnect with exponential backoff** in the JS SDK; messages queued
  during disconnect are flushed on reopen; the server resends the snapshot
  and the client diffs against its local state.

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

aether.snapshot();                                 // {key: value, ...}
aether.keys();                                     // ['counter', ...]
```

Run the self-test (snapshot delivery, bidirectional sync between simulated
tabs, late-joiner snapshot, delete propagation, end-to-end persistence):

`python -m aether_core.gateway`

## Phase 5 :: Network topology whiteboard (`web/index.html`)

A real application built on the stack. Open in two tabs and you have a
collaborative editor for enterprise network diagrams — add firewalls,
switches, NAS units and app servers from a palette, drag them around the
canvas, wire them together, label them, change link speeds. Every change
propagates live to every other tab and persists in the ledger.

State schema:

```
node:<uuid>:type     "firewall" | "switch" | "nas" | "server"
node:<uuid>:label    user-editable display name
node:<uuid>:coords   {x: number, y: number}

link:<uuid>          {from: <uuid>, to: <uuid>, speed: <string>}
```

No frameworks. No build step. ~1,300 lines of pure HTML + CSS + vanilla JS.

---

## Phase 6 :: Log compaction (`aether_core/compact.py`)

The append-only ledger grows forever — every `set` and `delete` is a new
line. After tens of thousands of operations, boot-time replay slows
down. `compact.py` is an offline worker that folds the ledger into a
condensed `<ledger>.snapshot.json` containing the final value (or
tombstone) for each key, plus the HLC stamp that produced it.

```bash
# Stop the server first (compaction is offline-only)
python -m aether_core.compact ledger_demo.jsonl

# Optional: archive the ledger and start fresh after a known-good snapshot
python -m aether_core.compact ledger_demo.jsonl --rotate
```

Output:

```
Compacting ledger: ledger_demo.jsonl
  records read    : 1,247
  live keys       : 89
  tombstones      : 14
  max stamp       : 01782311939892269105.0000000000.alpha
  snapshot        : ledger_demo.jsonl.snapshot.json
  snapshot size   : 12,830 bytes
done.
```

**Boot integration.** `ChronoLedger.boot()` auto-detects
`<ledger>.snapshot.json`. If present, it loads each entry into the CRDT
first and then replays only ledger records with HLC stamps strictly
newer than the snapshot's `max_stamp`. Boot time becomes
O(records-since-last-compact) instead of O(records-ever-written).

**Atomicity.** The snapshot is written to a `.tmp` file, fsync'd, and
then atomically renamed over the final path. A process crash mid-write
leaves any prior snapshot intact.

**Rotation.** With `--rotate`, the original ledger is moved to
`<ledger>.archived.<unix-ts>` (never deleted — recoverable if the
snapshot turns out to be flawed), and a fresh empty ledger replaces it.

Run the self-test:

`python -m aether_core.compact`

## Project layout

```
aether-core/
├── aether_core/          Python package
│   ├── __init__.py       (re-exports the public API)
│   ├── crdt.py           Phase 1 — math
│   ├── mesh.py           Phase 2 — networking
│   ├── storage.py        Phase 3 — persistence (snapshot-aware)
│   ├── gateway.py        Phase 4 — browser bridge
│   └── compact.py        Phase 6 — log compaction worker
├── web/                  Browser-facing assets
│   ├── aether.js         Phase 4 — vanilla JS SDK
│   └── index.html        Phase 5 — topology whiteboard
├── run_demo.py           End-to-end launcher
├── requirements.txt
├── LICENSE
├── .gitignore
└── README.md
```

---

## Running the tests

Each phase ships with a runnable self-test that prints clear `PROVEN` /
`FAILED` verdicts:

```bash
python -m aether_core.crdt       # CRDT convergence
python -m aether_core.mesh       # mesh sync (linear topology)
python -m aether_core.storage    # ledger persistence + crash recovery
python -m aether_core.gateway    # gateway + simulated browser sync
python -m aether_core.compact    # log compaction + snapshot boot
```

All four exit 0 on success.

---

## Design constraints, honoured

* **No external databases.** Not SQLite, not Postgres, not Redis. State is
  variables, history is an `os.write()`-ed JSON-lines file.
* **No centralized server.** Every Python node is a peer; every browser tab
  is a thin client of a peer.
* **No JS frameworks.** No React, Vue, Tailwind, jQuery, or build step.
  The browser side is a single `<script src="aether.js">` away from working.
* **Pure stdlib + websockets.** Python depends only on `websockets>=11.0.3`.

---

## Project status

Phases 1–6 are complete and verified. Natural next milestones, in roughly
this order:

* **Anti-entropy resync** — after a network partition heals, peers should
  exchange Merkle digests of their oplogs and patch up the gaps without
  re-gossiping the entire history.
* **Range queries / indexes** — secondary structures over the LWW Map so
  apps can ask "all node:* keys" without scanning the whole snapshot. This
  is currently done client-side and is fine up to a few thousand keys.
* **Multi-region deployment** — the mesh layer is already P2P; a real
  deployment across continents just needs each region to run its own
  MeshNode peered with the others, with the existing CRDT layer handling
  reconciliation.

---

## License

MIT — see [`LICENSE`](LICENSE).

---

Built by **Nishant Bhatte** · [@IronFighter23](https://github.com/IronFighter23)
