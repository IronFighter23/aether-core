# JavaScript API Reference

Complete reference for the browser-side client (`web/aether.js`). Every
method, option, and callback signature.

TypeScript users: a corresponding `aether.d.ts` ships in the same
folder. The types compile cleanly under `tsc --strict`.

## Constructor

```js
new Aether(url, options?)
```

Creates a client bound to the gateway at `url`. Synchronously
populates the in-memory state from `localStorage` (if any cached
state exists), **then** opens a WebSocket. `ready()` resolves
immediately if the cache hydrated, otherwise after the first gateway
snapshot.

### Parameters

| Name | Type | Description |
|---|---|---|
| `url` | `string` | The gateway URL, e.g. `'ws://localhost:8211'` or `'wss://app.example.com/ws'` |
| `options.autoReconnect` | `boolean` | Auto-reconnect with exponential backoff. Default `true`. |
| `options.maxReconnectDelayMs` | `number` | Cap on the backoff delay between reconnect attempts. Default `5000`. |
| `options.persist` | `boolean` | Persist state to `localStorage`. Default `true`. Set to `false` to disable offline-first behaviour. |
| `options.cacheKey` | `string` | Override the `localStorage` key. Defaults to `aether::state::<url>` so multiple gateways on the same origin don't collide. |

### Example

```js
const aether = new Aether('ws://localhost:8211', {
  autoReconnect: true,
  maxReconnectDelayMs: 10_000,
  cacheKey: 'myapp::v2::state',
});
```

## Properties (read-only)

| Property | Type | Description |
|---|---|---|
| `url` | `string` | The gateway URL. |
| `connected` | `boolean` | `true` once the WebSocket has completed its handshake. |
| `clientId` | `string \| null` | Server-issued unique ID for this session. `null` until connected. |
| `clientColor` | `string \| null` | Server-issued HSL colour for presence cursors. `null` until connected. |

## State methods

### `set(key, value)`

Write a value. Returns immediately. The value is:

1. Applied to local state.
2. Sent to the gateway over the WebSocket (queued if disconnected).
3. Persisted to localStorage (debounced).
4. Mirrored to sibling tabs via BroadcastChannel.

```js
aether.set('user:profile', { name: 'Aleph', city: 'Mumbai' });
aether.set('counter', 42);
aether.set('node:abc:coords', { x: 100, y: 200 });
```

**Important**: `set` is the only writer. There is no `update()` or
`patch()`. To modify a sub-field of an object, read-modify-write:

```js
const user = aether.get('user:profile') || {};
aether.set('user:profile', { ...user, name: 'Beth' });
```

This is intentional. Sub-field updates would require CRDT semantics
this engine doesn't support. See [Recipes](recipes.md) for patterns
that work well with whole-object replacement.

### `delete(key)`

Delete a key. The Python side records a tombstone, so the delete
cannot be silently reverted by a late-arriving stale write.

```js
aether.delete('counter');
```

### `get(key)`

Read the current local value, or `undefined`.

```js
const count = aether.get('counter') ?? 0;
```

### `has(key)`

`true` if the key currently has a non-tombstoned value.

```js
if (aether.has('user:profile')) { /* ... */ }
```

### `keys()`

Returns a fresh array of every currently-live key (tombstones
excluded).

```js
for (const key of aether.keys()) {
  if (key.startsWith('node:')) { /* ... */ }
}
```

### `snapshot()`

Returns a plain-object copy of the entire current state.

```js
const everything = aether.snapshot();
console.log(JSON.stringify(everything, null, 2));
```

## Subscriptions

### `on(key, callback)`

Watch a single key. Returns an unsubscribe function.

```js
const off = aether.on('counter', (newValue, oldValue) => {
  document.getElementById('count').textContent = newValue ?? 0;
});

// Later, when you no longer care:
off();
```

`newValue` is `undefined` when the key is deleted. `oldValue` is the
previous value before this change.

### `onAny(callback)`

Watch every change. Useful for catch-all renderers in small apps.

```js
const off = aether.onAny((key, newValue, oldValue) => {
  if (key.startsWith('node:')) rerenderNodes();
  if (key.startsWith('link:')) rerenderLinks();
});
```

### `onStatus(callback)`

Watch connection state. Fires immediately with the current state
when you subscribe, then on every flip.

```js
aether.onStatus((connected) => {
  document.getElementById('status').classList.toggle('connected', connected);
});
```

## Lifecycle

### `ready()`

Returns a `Promise<void>` that resolves once the client has usable
state to show:

- Immediately, if the `localStorage` cache hydrated successfully.
- Otherwise, on the first snapshot from the gateway.

```js
aether.ready().then(() => {
  initialRender();
});
```

Always wait for `ready()` before your initial render. Reads on a
fresh client may return `undefined` until the first snapshot arrives,
which would cause your UI to flash empty.

### `close()`

Tear down the connection and stop auto-reconnect. Use when
unmounting a single-page app from a larger host application.

```js
window.addEventListener('beforeunload', () => aether.close());
```

### `clearCache()`

Wipe the `localStorage` cache for this gateway. Does **not** touch
in-memory state — call `location.reload()` afterwards for a full
reset.

```js
document.getElementById('reset').onclick = () => {
  if (confirm('Wipe local cache?')) {
    aether.clearCache();
    location.reload();
  }
};
```

## Presence (ephemeral cursor sharing)

Presence updates do **not** touch the CRDT or the ledger. They ride
the WebSocket and are relayed to other connected clients only. Use
for cursors, typing indicators, anything where the previous state has
no value the moment the next state arrives.

### `sendPresence(x, y)`

Broadcast your local cursor position. Coordinates are integers.

```js
canvas.addEventListener('mousemove', (e) => {
  const rect = canvas.getBoundingClientRect();
  aether.sendPresence(
    Math.round(e.clientX - rect.left),
    Math.round(e.clientY - rect.top),
  );
});
```

### `onPresence(callback)`

Subscribe to remote cursors.

```js
aether.onPresence((id, x, y, color) => {
  let cursor = document.getElementById('cursor-' + id);
  if (!cursor) {
    cursor = document.createElement('div');
    cursor.id = 'cursor-' + id;
    cursor.className = 'remote-cursor';
    document.body.appendChild(cursor);
  }
  cursor.style.background = color;
  cursor.style.transform = `translate(${x}px, ${y}px)`;
});
```

### `onPresenceLeave(callback)`

Subscribe to "client left" events. Called when another client closes
its connection.

```js
aether.onPresenceLeave((id) => {
  document.getElementById('cursor-' + id)?.remove();
});
```

## Cross-tab sync via BroadcastChannel

The client automatically uses [BroadcastChannel] when available so
that **same-origin sibling tabs sync to each other directly**, without
going through the WebSocket. This makes cross-tab updates
instantaneous and works even when the gateway is offline.

You don't have to do anything to enable this. It's automatic and
transparent. If BroadcastChannel is unavailable (older browsers,
some privacy modes), the client falls back to WebSocket-only.

[BroadcastChannel]: https://developer.mozilla.org/en-US/docs/Web/API/BroadcastChannel

## Error handling

The client is designed to never throw from user-facing methods.
Invalid arguments are dropped silently:

- `set()` with a non-string key: dropped.
- `set()` with a value that can't be `JSON.stringify`'d: dropped.
- `delete()` of a non-existent key: no-op.
- `on()` / `onAny()` / `onStatus()` callback throws: error is
  `console.error`'d but does not interfere with other subscribers.

If you need to know whether a write succeeded server-side, listen for
the gateway's echo:

```js
aether.set('important:key', value);
aether.on('important:key', (echoed) => {
  if (echoed === value) console.log('server confirmed');
});
```

## Complete cheat sheet

```js
// Construction
const aether = new Aether(url, options);

// State
aether.set(key, value);
aether.delete(key);
aether.get(key);                 // -> value or undefined
aether.has(key);                 // -> boolean
aether.keys();                   // -> string[]
aether.snapshot();               // -> { [key]: value }

// Subscriptions
const off = aether.on(key, (newV, oldV) => {});
const off = aether.onAny((key, newV, oldV) => {});
const off = aether.onStatus((connected) => {});

// Lifecycle
await aether.ready();
aether.close();
aether.clearCache();

// Presence
aether.sendPresence(x, y);
aether.onPresence((id, x, y, color) => {});
aether.onPresenceLeave((id) => {});

// Read-only properties
aether.url;          // string
aether.connected;    // boolean
aether.clientId;     // string | null
aether.clientColor;  // string | null
```

## See also

- [Concepts](concepts.md) — why the API is shaped this way
- [Recipes](recipes.md) — common patterns built on these primitives
- [Python API](api-python.md) — the relay side
