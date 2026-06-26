# Recipes

Practical patterns for building real features on top of Aether-Core's
small primitive set. Each recipe is short, self-contained, and runnable.

## Recipe: simple counter

A shared counter that survives reloads, syncs across tabs.

```js
function increment() {
  const current = aether.get('counter') ?? 0;
  aether.set('counter', current + 1);
}

aether.on('counter', (value) => {
  document.getElementById('count').textContent = value ?? 0;
});
```

**Caveat**: under heavy concurrent load, this can lose increments.
Two tabs reading 5 simultaneously will both write 6, not 7. For
single-user-occasional-other-tabs scenarios this is fine. For
high-contention counters, use the "list-backed counter" pattern below.

## Recipe: list-backed counter (contention-safe)

Store each increment as its own key. The count is the live key
count. Lossless under any concurrency.

```js
function increment() {
  const id = crypto.randomUUID();
  aether.set('clicks:' + id, Date.now());
}

function getCount() {
  return aether.keys().filter(k => k.startsWith('clicks:')).length;
}

aether.onAny((key) => {
  if (key.startsWith('clicks:')) {
    document.getElementById('count').textContent = getCount();
  }
});
```

Cost: every click is a permanent key. Add a compaction pass if this
grows too large.

## Recipe: ordered list

Maintain ordering with a single `order` key holding an array of IDs.

```js
function addItem(text) {
  const id = 'item:' + crypto.randomUUID();
  aether.set(id, { id, text });
  const order = aether.get('list:order') ?? [];
  aether.set('list:order', [...order, id]);
}

function deleteItem(id) {
  aether.delete(id);
  const order = aether.get('list:order') ?? [];
  aether.set('list:order', order.filter(x => x !== id));
}

function moveItem(id, newIndex) {
  const order = (aether.get('list:order') ?? []).filter(x => x !== id);
  order.splice(newIndex, 0, id);
  aether.set('list:order', order);
}

function render() {
  const order = aether.get('list:order') ?? [];
  const ul = document.getElementById('list');
  ul.innerHTML = '';
  for (const id of order) {
    const item = aether.get(id);
    if (!item) continue;        // tombstoned, skip
    const li = document.createElement('li');
    li.textContent = item.text;
    ul.appendChild(li);
  }
}

aether.onAny(render);
```

**Trade-off**: the whole `order` array is one CRDT value. Concurrent
inserts at the same position resolve to one writer (LWW). For most
collaborative UX this is acceptable — visually surprising for half a
second, then converges. Real CRDT lists (RGA, YATA) would do better
but aren't built into this engine.

## Recipe: presence cursors

Show where other users' cursors are on a shared canvas.

```js
const cursors = new Map();

canvas.addEventListener('mousemove', (e) => {
  const r = canvas.getBoundingClientRect();
  aether.sendPresence(
    Math.round(e.clientX - r.left),
    Math.round(e.clientY - r.top),
  );
});

aether.onPresence((id, x, y, color) => {
  let el = cursors.get(id);
  if (!el) {
    el = document.createElement('div');
    el.className = 'remote-cursor';
    el.style.background = color;
    canvas.appendChild(el);
    cursors.set(id, el);
  }
  el.style.transform = `translate(${x}px, ${y}px)`;
});

aether.onPresenceLeave((id) => {
  cursors.get(id)?.remove();
  cursors.delete(id);
});
```

Presence is **ephemeral**. Coordinates never touch the CRDT or the
ledger. Disconnected clients automatically "leave."

## Recipe: optimistic UI

`set()` is already optimistic: the local state updates instantly,
the gateway's echo arrives microseconds later. If you need to show
an immediate state change *and* roll back on server rejection (e.g.
oversize value), use a confirmation key:

```js
function postMessage(text) {
  const id = 'msg:' + crypto.randomUUID();
  // Show immediately as "sending"
  aether.set(id, { id, text, status: 'sending' });

  // Confirm on server echo
  const off = aether.on(id, (echoed) => {
    if (echoed?.status === 'sending') {
      aether.set(id, { ...echoed, status: 'sent' });
      off();
    }
  });
}
```

Server rejection (oversize, rate-limit) means the gateway never
echoes the write, so `status` stays `sending`. Add a timeout for
UX clarity:

```js
setTimeout(() => {
  const msg = aether.get(id);
  if (msg?.status === 'sending') {
    aether.set(id, { ...msg, status: 'failed' });
  }
}, 5000);
```

## Recipe: schema versioning

When you change a value's shape, old data already in the ledger
won't auto-migrate. Two patterns:

### Pattern A: version field, read-time migration

```js
function readUser(id) {
  const raw = aether.get('user:' + id);
  if (!raw) return null;
  if (raw.v === 1) return raw;
  if (raw.v === undefined) {        // pre-versioned schema
    return { v: 1, ...raw, displayName: raw.name };
  }
  return null;
}

function writeUser(id, user) {
  aether.set('user:' + id, { v: 1, ...user });
}
```

### Pattern B: namespace bump

```js
// Old: aether.set('user:'+id, ...)
// New: aether.set('user-v2:'+id, ...)
```

Migrate by reading all `user:` keys, transforming, writing to
`user-v2:`, deleting the originals. Run once per deployment.

## Recipe: undo / redo

The append-only ledger gives you a natural undo log, but the live
CRDT doesn't expose it. Build undo by capturing snapshots:

```js
const undoStack = [];
let suppressCapture = false;

function captureSnapshot() {
  if (suppressCapture) return;
  undoStack.push(aether.snapshot());
  if (undoStack.length > 50) undoStack.shift();
}

function undo() {
  if (undoStack.length < 2) return;
  undoStack.pop();                       // current state
  const previous = undoStack[undoStack.length - 1];
  suppressCapture = true;
  // Apply: delete keys not in `previous`, set the rest.
  for (const k of aether.keys()) {
    if (!(k in previous)) aether.delete(k);
  }
  for (const [k, v] of Object.entries(previous)) {
    aether.set(k, v);
  }
  suppressCapture = false;
}

aether.onAny(captureSnapshot);
captureSnapshot();    // seed
```

This is per-tab undo, not cross-user. Cross-user undo with multiple
concurrent writers is a research problem; don't attempt it casually.

## Recipe: lazy initialisation

Many apps want to seed default content on first run.

```js
aether.ready().then(() => {
  if (aether.keys().length === 0) {
    aether.set('config:theme', 'dark');
    aether.set('config:locale', 'en');
  }
  render();
});
```

Note: this races between tabs on first open. Both might see an empty
state, both will seed, and the higher-HLC writes win. That's fine
for defaults but **not** for user identity. For identity, use a
server-issued client ID:

```js
aether.ready().then(() => {
  if (aether.clientId) {
    initWithIdentity(aether.clientId);
  }
});
```

## Recipe: derived state caches

Aether has no query layer. For O(1) "give me the cards in column X"
lookups, maintain a derived index in JS:

```js
const cardsByColumn = new Map();

function rebuildIndex() {
  cardsByColumn.clear();
  for (const key of aether.keys()) {
    if (!key.startsWith('card:')) continue;
    const card = aether.get(key);
    if (!card) continue;
    if (!cardsByColumn.has(card.columnId)) {
      cardsByColumn.set(card.columnId, []);
    }
    cardsByColumn.get(card.columnId).push(card);
  }
}

aether.onAny((key) => {
  if (key.startsWith('card:')) rebuildIndex();
});

aether.ready().then(rebuildIndex);
```

For small N this is fine. For N > 10k, rebuild incrementally inside
the `onAny` callback instead of full-scanning.

## Recipe: rate-limit your writes

The gateway will close connections that flood it (default 100
msg/sec). On hot interactions (drag, scrub), throttle:

```js
let scheduled = false;
let pending = null;

function debouncedSet(key, value) {
  pending = { key, value };
  if (scheduled) return;
  scheduled = true;
  requestAnimationFrame(() => {
    scheduled = false;
    if (pending) {
      aether.set(pending.key, pending.value);
      pending = null;
    }
  });
}

element.addEventListener('mousemove', (e) => {
  debouncedSet('cursor:position', { x: e.clientX, y: e.clientY });
});
```

`requestAnimationFrame` gives you ~60 Hz max, well under the
default 100/sec rate budget.

## Recipe: multi-room / multi-document

One Aether instance per "room" or "document." Use the `cacheKey`
option to keep them isolated:

```js
function openDocument(docId) {
  return new Aether('ws://relay.example.com/' + docId, {
    cacheKey: 'mydoc::' + docId,
  });
}

const doc1 = openDocument('design-2026');
const doc2 = openDocument('meeting-notes');
```

On the server side, run one relay per document (or shard them
behind a reverse proxy by path). See [Deployment](deployment.md).

## Recipe: subscription-based reactivity

If you want React/Vue/Svelte-style reactivity, write a tiny adapter.
Example for React:

```js
function useAetherKey(aether, key) {
  const [value, setValue] = React.useState(() => aether.get(key));
  React.useEffect(() => {
    const off = aether.on(key, (v) => setValue(v));
    return off;
  }, [key]);
  return value;
}

function Counter({ aether }) {
  const count = useAetherKey(aether, 'counter') ?? 0;
  return (
    <button onClick={() => aether.set('counter', count + 1)}>
      {count}
    </button>
  );
}
```

Same idea for Vue (`ref` + watch), Svelte (`writable` store).

## Recipe: server-side admin / inspection

Read or modify state from a Python script (e.g. an admin CLI). The
relay is just a Python process — write directly to its mesh, or
read straight from the ledger:

```python
from aether_core import ChronoLedger, MeshNode

async def list_all_cards():
    ledger = ChronoLedger("ledger_demo.jsonl")
    mesh = MeshNode("admin", port=0)
    await ledger.boot(mesh)

    for key in mesh.snapshot():
        if key.startswith("card:"):
            print(key, mesh.get(key))

    await ledger.close()

asyncio.run(list_all_cards())
```

This boots an isolated mesh from the same ledger, reads what it
needs, and exits. No network calls, no API, no permissions check —
you have filesystem access, you can do anything.

## See also

- [JavaScript API](api-javascript.md) — full reference for the
  primitives these recipes are built from
- [Python API](api-python.md) — for server-side recipes
- [Concepts](concepts.md) — why some recipes are "safe" and others
  have caveats (LWW semantics, tombstones, etc.)
