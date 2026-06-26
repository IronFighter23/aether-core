# Concepts

A precise, code-level mental model for Aether-Core. Read this once
and the rest of the API will make sense without surprise.

## The big idea: zero-transit

Most real-time apps move data through three distinct representations
in transit: client state → API request → server-side model → database
row → server-side query result → API response → client state. Every
arrow is a transformation that has to be written, tested, kept in
sync, and migrated when something changes.

Aether-Core erases the middle. There is exactly **one** representation
— the CRDT-backed map of `(key, value)` pairs — and it exists
simultaneously in:

- Every connected browser tab (in-memory).
- The Python relay (in-memory).
- The on-disk ledger (line-delimited JSON).
- `localStorage` in every browser (cached copy).

Every layer holds the *same* JSON values under the *same* keys. The
Python relay's only job is to make sure all four layers stay
identical, not to interpret what's in them.

This is what "zero-transit" means: data is **not transited** between
representations. It is the same representation everywhere.

## CRDTs in 60 seconds

A CRDT is a data structure with two properties:

1. **Commutative merge** — given two states A and B,
   `merge(A, B) == merge(B, A)`. Order doesn't matter.
2. **Idempotent merge** — `merge(A, A) == A`. Replays are safe.

Together these mean: if every node eventually receives the same set
of operations (in any order, possibly with duplicates), all nodes
converge to the **same final state**, deterministically.

This is huge for distributed systems. It removes the need for:

- Two-phase commits
- Operational transformation (Google Docs–style)
- Single-writer constraints
- Conflict resolution UIs

The cost is that you must phrase every operation as a CRDT-compatible
merge. Aether-Core picks one specific CRDT — **Last-Writer-Wins
Map** — and exposes it as the only data model. This trades expressive
power for radical simplicity.

## Last-Writer-Wins, with HLC stamps

Aether-Core's map looks like a Python dict or JavaScript object:

```
{
  "node:abc:type":   "firewall",
  "node:abc:coords": {"x": 100, "y": 200},
  "counter":         42
}
```

But under the hood, each value carries a stamp:

```
{
  "node:abc:type":   (value="firewall",            stamp=HLC(t=1000, l=0, node="alpha")),
  "node:abc:coords": (value={"x":100,"y":200},     stamp=HLC(t=1003, l=0, node="alpha")),
  "counter":         (value=42,                    stamp=HLC(t=1005, l=2, node="beta"))
}
```

When the same key is written from two places, the value with the
**higher stamp wins**. Lower stamps are silently discarded.

This is "Last-Writer-Wins" — except "last" is defined by HLC, not
wall-clock time, which means it's well-defined even when clocks
disagree.

## What is an HLC?

A **Hybrid Logical Clock** is a triple:

```
(physical_time_ns, logical_counter, node_id)
```

- `physical_time_ns` — the wall clock in nanoseconds. Lets stamps
  roughly correspond to real time, so the system is intuitive to
  reason about.
- `logical_counter` — incremented when two events happen at the same
  physical nanosecond on the same node. Guarantees monotonicity
  even at clock-tick granularity.
- `node_id` — the originating node's string ID. Final tiebreaker:
  two different nodes producing a stamp at the same exact ns + lc
  still differ in node_id, so the comparison is **strictly total**.

Stamps compare lexicographically. Strictly total order means: given
any two HLCs, exactly one is greater. No ties. Ever.

**Why this matters for collaboration:** when Alice and Bob both edit
the same field at the same instant, the system picks one
deterministically. They might disagree on whose edit was "right" —
but they both see the same winner, immediately, without needing a
conflict-resolution dialog.

## Tombstones, not deletes

When you `delete(key)`, the engine doesn't remove the value from the
map. It writes a tombstone — a special value that means "deleted at
HLC=X". Tombstones live forever (modulo log compaction).

Why? Imagine:

1. Alice writes `counter = 5` at HLC=100.
2. Bob's network drops.
3. Carol deletes `counter` at HLC=200.
4. Bob's network comes back, and Bob's queued write `counter = 6`
   (with HLC=150) finally reaches the relay.

If we'd actually removed `counter` in step 3, Bob's stale write in
step 4 would silently resurrect a deleted key. With tombstones, the
tombstone at HLC=200 wins over Bob's HLC=150 write, and `counter`
stays deleted everywhere.

Tombstones make deletes safe under arbitrary network delays.

## The four layers and how they stay in sync

### Layer 1: browser memory (JavaScript)

`aether.js` keeps a `Map<key, value>` in memory. Every `get()` reads
from this map. It's the source of truth for the running UI.

### Layer 2: localStorage cache

After every state mutation, the JS client serialises the whole map
to localStorage (debounced ~12 Hz so a 60 Hz drag doesn't hammer the
synchronous storage API).

On page load, the client **first** reads localStorage, **then** opens
the WebSocket. This means a refresh — even with the server down —
renders instantly from cache.

### Layer 3: Python relay memory

The Python `MeshNode` holds the same `(key, value, stamp)` map as
the browser. Every operation received from any browser or peer
node passes through the CRDT merge, updating this map.

### Layer 4: the ledger

Every CRDT operation is appended to `ledger_*.jsonl` as a single
line. Each append is one `os.write()` call followed by `fsync()`.
POSIX guarantees the `os.write()` is atomic for records below
PIPE_BUF (≥4 KiB everywhere we run), and our records are far below
that, so a power loss can corrupt at most one trailing record.

On boot, the relay replays every line back into the CRDT. Since
merge is idempotent and commutative, replay always produces the
same final state, regardless of order or partial reads.

### Keeping the layers consistent

The arrows go in both directions:

```
            browser write
                  │
                  ▼
        localStorage  (immediate, debounced)
                  │
                  ▼
            WebSocket out
                  │
                  ▼
            Python merge  ─────►  ledger.jsonl  (fsync)
                  │
                  ▼
        broadcast to all other tabs
                  │
                  ▼
        every other browser's merge
                  │
                  ▼
        every other localStorage
```

Eventual consistency: any operation that reaches the Python relay
reaches every connected browser, every persistent ledger, and every
offline cache. Disconnected browsers catch up on reconnect via the
snapshot the relay sends as the first message.

## Snapshot vs ledger

The ledger is the durable, replayable, append-only log. It grows
forever.

A **snapshot** is the *current state* derived from the ledger — a
single JSON file with each key's final value. It's much smaller
than the ledger (typically 30–40% on realistic workloads, see
[BENCHMARKS.md](../BENCHMARKS.md)) and lets you skip replaying
old operations on boot.

You don't manage snapshots manually. The compaction worker
(`python -m aether_core.compact <ledger>`) reads the ledger, builds
a snapshot, and writes it next to the ledger. Next boot, the relay
loads the snapshot first, then replays only the ledger lines newer
than the snapshot's `max_stamp`.

This is the same trick that databases use: WAL + periodic checkpoint.
Aether-Core's WAL is the ledger; the checkpoint is the snapshot.

## What "zero-transit" buys you

When you accept this model, several things follow automatically:

- **No API design phase.** You don't write `/api/cards/:id` because
  there is no API. Just `aether.set('card:'+id, ...)`.
- **No serialisation boundaries.** What lives in JS lives in Python
  lives on disk, byte-for-byte. No DTOs.
- **No backend deploys for new features.** Adding a `priority` field
  to cards means adding `priority` to the value in the frontend.
  The relay neither knows nor cares.
- **Multi-tab UX comes for free.** Two tabs sync because they both
  talk to the same relay, not because you wrote any sync code.
- **Offline-first comes for free.** localStorage is the cache; the
  relay is the durability layer.
- **Time travel comes for free.** Replay the ledger up to timestamp
  T and you have the state at T.

## What zero-transit doesn't buy you

Be honest about limits:

- **You can't query.** There's no SQL. If you need to ask "how many
  cards are in column X?", you iterate `aether.keys()` and count
  client-side. Fine for hundreds of keys; not fine for millions.
- **Every byte goes to every client.** A new tab gets the full
  snapshot. Fine for kilobytes; not fine for gigabytes.
- **No fine-grained access control.** Every connected client sees
  every key. If you need permissions, run multiple relays with
  different ports/origins, and use a reverse proxy to gate access.
- **LWW is sometimes wrong.** Two people typing into the same field
  → one wins. For collaborative text editing of the same line, you
  want a character-level CRDT, not LWW. (Aether-Core's Markdown demo
  works around this by keying each paragraph separately.)
- **Forever-growing ledger.** Run compaction periodically or your
  boot time grows.

## Where this fits

Aether-Core is designed for the wide middle ground between "small
single-user app" and "thousands of QPS at planet scale":

- Real-time collaborative editors for niche / internal tools
- Whiteboards, diagram editors, kanban boards, mood boards
- Cursor-presence overlays on existing apps
- Multi-tab desktop-app-like web tools
- Hackathon and prototype demos that need "the magic" without a
  weekend of backend setup
- Internal admin tools where 5–50 users share state

It's not designed for:

- Public-facing apps with thousands of concurrent users per relay
  (you can shard, but it's manual)
- High-cardinality data (millions of keys per relay)
- Strong consistency requirements (CRDTs are eventually consistent
  by construction)
- Fine-grained authorization (no built-in auth at all)

## Next reading

- [Recipes](recipes.md) — concrete patterns for common UX needs.
- [JavaScript API](api-javascript.md) — what's actually available.
- [Python API](api-python.md) — what the relay exposes.
