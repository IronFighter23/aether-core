# Python API Reference

Complete reference for the relay side. Three classes do almost all
the work: `MeshNode`, `ClientGateway`, `ChronoLedger`. A fourth,
`SecurityLimits`, tunes the security envelope.

## `MeshNode`

Owns the CRDT replica and the peer-to-peer federation transport.

```python
from aether_core import MeshNode

mesh = MeshNode(
    node_id="alpha",          # string, unique per node in the federation
    host="127.0.0.1",         # bind interface
    port=8201,                # 0 = pick an ephemeral port
    on_op=None,               # optional async callback(op, source_peer)
    pubsub=None,              # optional custom MeshPubSub driver
    limits=None,              # optional SecurityLimits
)
```

### Constructor parameters

| Parameter | Type | Required | Description |
|---|---|---|---|
| `node_id` | `str` | yes | Globally unique ID. Used in HLC stamps. Two nodes with the same ID will produce duplicate stamps and corrupt the merge. |
| `host` | `str` | no | Interface to bind. `127.0.0.1` for local-only; `0.0.0.0` for federated production. |
| `port` | `int` | no | TCP port. `0` picks an ephemeral port (useful in tests). |
| `on_op` | `Callable` | no | Async callback `(op, source_peer) -> None`. Fired for every operation the mesh ingests (local or remote). Used by `ChronoLedger` and `ClientGateway` to subscribe to the op stream. |
| `pubsub` | `MeshPubSub` | no | Inject a custom transport (e.g. Redis, NATS). Default is `WebSocketMeshPubSub`. |
| `limits` | `SecurityLimits` | no | Override rate limits, payload caps, connection caps. |

### Methods

#### `await mesh.start()`

Bind the listening socket and become ready to accept federated peers.
Call **before** any peer connects to you. Idempotent — calling twice
is a no-op.

#### `await mesh.stop()`

Close every peer connection, cancel background tasks, shut the
server. Idempotent.

#### `await mesh.connect_to(host, port)`

Dial a federated peer. Returns the peer's `node_id` once the
hello-handshake completes. Raises `RuntimeError` if the handshake
fails.

```python
peer_id = await mesh.connect_to("10.0.0.5", 8201)
```

Duplicate connections to the same peer are detected: the second
channel is closed, the first survives.

#### `await mesh.set(key, value)`

Write a key locally and gossip to all peers. Returns the
`Operation` that was generated.

```python
op = await mesh.set("user:profile", {"name": "Aleph"})
```

The value must be JSON-serialisable. Non-serialisable values raise
`TypeError`.

#### `await mesh.delete(key)`

Tombstone a key and gossip the tombstone. Returns the `Operation`.

```python
op = await mesh.delete("user:profile")
```

#### `mesh.get(key) -> Any`

Synchronous read from the local replica.

#### `mesh.snapshot() -> dict[str, Any]`

Return a plain dict of every live key. Tombstoned keys are excluded.

### Properties

| Property | Type | Description |
|---|---|---|
| `mesh.id` | `str` | The node ID passed to the constructor. |
| `mesh.host` | `str` | The bound host. |
| `mesh.port` | `int` | The actual port (resolved if `port=0`). |
| `mesh.peer_ids` | `set[str]` | Currently-connected federated peers. |
| `mesh.pubsub` | `MeshPubSub` | The driver instance (`WebSocketMeshPubSub` by default). |
| `mesh.node` | `Node` | The underlying CRDT node. Advanced/test use only. |

### Complete example

```python
import asyncio
from aether_core import MeshNode

async def main():
    alpha = MeshNode("alpha", port=8001)
    beta  = MeshNode("beta",  port=8002)
    await alpha.start()
    await beta.start()
    await alpha.connect_to("127.0.0.1", 8002)

    await alpha.set("greeting", "hello from alpha")
    await asyncio.sleep(0.05)        # let gossip settle

    assert beta.get("greeting") == "hello from alpha"

    await alpha.stop()
    await beta.stop()

asyncio.run(main())
```

## `ClientGateway`

The browser-facing WebSocket endpoint. Binds to a mesh node and
relays CRDT operations to/from connected browser tabs.

```python
from aether_core import ClientGateway

gw = ClientGateway(
    mesh_node,                # required: a started MeshNode
    host="127.0.0.1",
    port=8211,
    limits=None,              # optional SecurityLimits
)
```

### Constructor parameters

| Parameter | Type | Required | Description |
|---|---|---|---|
| `mesh_node` | `MeshNode` | yes | The CRDT replica this gateway exposes. |
| `host` | `str` | no | `127.0.0.1` for local; `0.0.0.0` to accept external browsers. |
| `port` | `int` | no | TCP port. `0` = ephemeral. |
| `limits` | `SecurityLimits` | no | Rate limits, payload caps, conn caps. Defaults are tight; loosen for production traffic. |

### Methods

#### `await gw.start()`
Bind the listening socket. Idempotent.

#### `await gw.stop()`
Close every browser session, shut the server. Idempotent.

#### `await gw.on_op(op, source_peer)`
Mesh subscriber. Broadcasts the operation to every connected browser.
You don't call this directly — wire it into the mesh's `on_op`
parameter (see "Putting it all together" below).

### Properties

| Property | Type | Description |
|---|---|---|
| `gw.host` | `str` | Bound host. |
| `gw.port` | `int` | Actual port. |
| `gw.url` | `str` | `ws://host:port`. |
| `gw.client_count` | `int` | Number of currently-connected browsers. |
| `gw.limits` | `SecurityLimits` | The effective limits. |

## `ChronoLedger`

Append-only JSON-lines persistence with `O_APPEND + fsync` per
record. Crash-safe.

```python
from aether_core import ChronoLedger

ledger = ChronoLedger("ledger_demo.jsonl")
```

### Methods

#### `await ledger.boot(mesh_node) -> int`

Replay every record on disk into the supplied `MeshNode`. Detects
torn final writes and truncates them. Returns the number of
operations replayed.

```python
n = await ledger.boot(mesh)
print(f"replayed {n} ops from disk")
```

Must be called **before** `mesh.start()` so replay happens before
any new operations arrive.

#### `await ledger.on_op(op, source_peer)`

Mesh subscriber. Appends `op` to the ledger asynchronously. Returns
immediately; the actual `fsync` happens on a background task. Use
`flush()` if you need durability guarantees before a specific
moment.

#### `await ledger.flush()`

Block until every queued operation has been `fsync`'d. Useful in
tests and before clean shutdown.

#### `await ledger.close()`

Drain pending writes, fsync, close the file descriptor. Idempotent.

### Properties

| Property | Type | Description |
|---|---|---|
| `ledger.path` | `Path` | The ledger file path. |
| `ledger.replayed_count` | `int` | Operations replayed at boot. |
| `ledger.written_count` | `int` | Operations persisted since boot. |
| `ledger.truncated_bytes` | `int` | Bytes discarded as a torn final record at boot. |
| `ledger.is_open` | `bool` | True between `boot()` and `close()`. |

## `SecurityLimits`

A frozen dataclass tuning the security envelope for both the
gateway and the mesh.

```python
from aether_core._security import SecurityLimits

limits = SecurityLimits(
    # Payload caps
    max_frame_bytes        = 256 * 1024,    # WebSocket frame ceiling
    max_message_bytes      = 64 * 1024,     # JSON body ceiling
    max_key_bytes          = 256,           # individual CRDT key
    max_value_bytes        = 32 * 1024,     # individual CRDT value
    # Connection caps
    max_connections_total       = 256,
    max_connections_per_source  = 32,
    # Rate limiting (token bucket per connection)
    messages_per_second    = 100.0,
    messages_burst         = 200,
    # Slow-loris guard
    handshake_timeout_s    = 5.0,
)
```

Pass the same instance to both `MeshNode(..., limits=limits)` and
`ClientGateway(..., limits=limits)` to keep the threat model
uniform.

See [SECURITY.md](../SECURITY.md) for the full threat model and
which limit defends against what.

## `compose_hooks(*hooks)`

Helper for fanning a single `on_op` event out to multiple
subscribers. The mesh accepts only one callback; in practice you
want both the ledger and the gateway listening.

```python
from aether_core import compose_hooks, MeshNode, ClientGateway, ChronoLedger

ledger = ChronoLedger("ledger.jsonl")

# Forward-reference the gateway via a placeholder, because the
# gateway needs the mesh and the mesh needs the gateway's on_op.
placeholder = {}

async def on_op(op, src):
    await ledger.on_op(op, src)
    gw = placeholder.get("gw")
    if gw is not None:
        await gw.on_op(op, src)

mesh = MeshNode("alpha", port=8201, on_op=on_op)
gw   = ClientGateway(mesh, host="0.0.0.0", port=8211)
placeholder["gw"] = gw
```

Or use `compose_hooks` for static cases:

```python
async def on_op(op, src):
    pass    # placeholder, gateway not yet built

mesh = MeshNode("alpha", port=8201, on_op=on_op)
gw = ClientGateway(mesh)
mesh._on_op = compose_hooks(ledger.on_op, gw.on_op)  # advanced
```

`compose_hooks` swallows exceptions from individual subscribers and
logs them, so a buggy subscriber doesn't break the others.

## Putting it all together

The canonical "boot a relay" pattern, copied from `run_demo.py`:

```python
import asyncio
from aether_core import ChronoLedger, ClientGateway, MeshNode
from aether_core._security import SecurityLimits

async def main():
    # Tune the limits for production.
    limits = SecurityLimits(
        messages_per_second=500.0,
        max_connections_total=2048,
    )

    # 1. Build the durable layer.
    ledger = ChronoLedger("prod.jsonl")

    # 2. Wire the mesh, with a placeholder for the gateway's on_op.
    placeholder = {}
    async def on_op(op, src):
        await ledger.on_op(op, src)
        gw = placeholder.get("gw")
        if gw is not None:
            await gw.on_op(op, src)

    mesh = MeshNode("prod-1", host="0.0.0.0", port=8201,
                    on_op=on_op, limits=limits)

    # 3. Build the gateway.
    gw = ClientGateway(mesh, host="0.0.0.0", port=8211, limits=limits)
    placeholder["gw"] = gw

    # 4. Boot order matters: ledger first (replays into mesh), then
    #    mesh.start (accepts peers), then gw.start (accepts browsers).
    await ledger.boot(mesh)
    await mesh.start()
    await gw.start()

    # 5. (Optional) connect to federated peers.
    # await mesh.connect_to("10.0.0.5", 8201)

    # 6. Run forever.
    await asyncio.Event().wait()

    # 7. Clean shutdown (reverse order).
    await gw.stop()
    await mesh.stop()
    await ledger.close()

asyncio.run(main())
```

## Custom `MeshPubSub` drivers

The default federation transport is WebSocket gossip. To plug in a
different transport (Redis pub/sub, NATS, in-process), subclass
`MeshPubSub`:

```python
from aether_core import MeshPubSub
from aether_core.crdt import Operation

class RedisMeshPubSub(MeshPubSub):
    def __init__(self, node_id, redis_url, channel):
        super().__init__()
        self._node_id = node_id
        self._redis_url = redis_url
        self._channel = channel
        # ... connect to Redis ...

    async def start(self) -> None:
        # ... subscribe to the channel; spawn a task that calls
        # self._on_remote_op(op, peer_id) for every received op ...
        ...

    async def stop(self) -> None: ...

    async def connect_to(self, host: str, port: int) -> str:
        # Redis is hub-and-spoke; "connecting" is a no-op once subscribed.
        return host

    async def publish(self, op: Operation, *, exclude_peer=None) -> None:
        # ... publish op to the channel, tagged with self._node_id ...
        ...

    @property
    def peer_ids(self): return set()
    @property
    def host(self): return self._redis_url
    @property
    def port(self): return 0
```

Then inject:

```python
mesh = MeshNode("alpha", pubsub=RedisMeshPubSub("alpha", "redis://...", "aether-prod"))
```

The CRDT layer, gateway, ledger, and browser client are all
transport-agnostic. They keep working.

## Cheat sheet

```python
from aether_core import (
    MeshNode, ClientGateway, ChronoLedger,
    MeshPubSub, WebSocketMeshPubSub, compose_hooks,
)
from aether_core._security import SecurityLimits

# Mesh
mesh = MeshNode(node_id, host, port, on_op=cb, pubsub=driver, limits=limits)
await mesh.start(); await mesh.stop()
await mesh.connect_to(host, port)
await mesh.set(key, value); await mesh.delete(key)
mesh.get(key); mesh.snapshot(); mesh.peer_ids; mesh.id

# Gateway
gw = ClientGateway(mesh, host, port, limits=limits)
await gw.start(); await gw.stop()
gw.url; gw.client_count

# Ledger
ledger = ChronoLedger(path)
await ledger.boot(mesh) -> int
await ledger.on_op(op, src); await ledger.flush(); await ledger.close()
ledger.replayed_count; ledger.written_count; ledger.is_open

# Security
limits = SecurityLimits(...)
```

## See also

- [Getting Started](getting-started.md) — walkthrough that uses these APIs
- [JavaScript API](api-javascript.md) — the other half of the system
- [Deployment](deployment.md) — running this in production
- [SECURITY.md](../SECURITY.md) — full threat model
