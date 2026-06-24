"""
Aether-Core :: Mesh PubSub (federation transport)
=================================================

This module owns one thing and one thing only: **server-to-server
federation**. It moves serialized ``Operation`` objects between Python
nodes that participate in the federated mesh. It knows nothing about
the CRDT layer's semantics, nothing about browser clients, and nothing
about the on-disk ledger.

Adapter pattern
---------------
The transport is exposed through an abstract base class
``MeshPubSub`` so the federation driver is pluggable. The current
shipping implementation is ``WebSocketMeshPubSub`` (epidemic gossip
over WebSockets), but a Redis-, NATS-, or in-process implementation
could be substituted without touching the orchestrator, the ledger,
or the client gateway. The orchestrator (``MeshNode``) owns the CRDT
``Node`` and wires the pubsub driver to it.

Wire protocol (WebSocket driver)
--------------------------------
Two message types, both JSON:

    {"type": "hello", "node_id": "<id>"}
    {"type": "op",    "op": {"kind": "set"|"del", "key": ..., "value": ..., "stamp": {...}}}

A ``hello`` is exchanged in both directions immediately after a
TCP/WS upgrade so every connection becomes uniquely keyed by the
remote ``node_id``. Duplicate connections to the same peer (e.g.
both sides dial each other simultaneously) are detected and closed.

Gossip semantics (handled by the orchestrator, not the driver)
--------------------------------------------------------------
- Local mutations: published to every connected peer.
- Inbound operations: deduplicated by HLC stamp (which is globally
  unique by construction -- ``node_id`` is part of the stamp),
  applied to the local Node, then re-published to every peer *except*
  the sender. This produces epidemic gossip with bounded propagation:
  every op reaches every reachable node in at most ``diameter(mesh)``
  hops, and the dedup set ensures it does not circulate forever.
"""
from __future__ import annotations

import abc
import asyncio
import json
import logging
from typing import Any, Awaitable, Callable, Optional

from websockets import connect, serve
from websockets.exceptions import ConnectionClosed

from aether_core.crdt import (
    HybridLogicalClock,
    Node,
    Operation,
    OpKind,
)

__all__ = [
    "MeshNode",
    "MeshPubSub",
    "WebSocketMeshPubSub",
    "serialize_hlc",
    "deserialize_hlc",
    "serialize_operation",
    "deserialize_operation",
]

logger = logging.getLogger(__name__)

# Type alias for the inbound-from-peer callback the orchestrator installs
# on the pubsub driver. The string is the peer node_id the op came from.
RemoteOpHandler = Callable[[Operation[Any, Any], str], Awaitable[None]]


# ---------------------------------------------------------------------------
# Wire-format serialization
# ---------------------------------------------------------------------------

def serialize_hlc(hlc: HybridLogicalClock) -> dict[str, Any]:
    """Compact JSON form of an HLC stamp."""
    return {"p": hlc.physical_ns, "l": hlc.logical, "n": hlc.node_id}


def deserialize_hlc(payload: dict[str, Any]) -> HybridLogicalClock:
    return HybridLogicalClock(
        physical_ns=int(payload["p"]),
        logical=int(payload["l"]),
        node_id=str(payload["n"]),
    )


def serialize_operation(op: Operation[Any, Any]) -> dict[str, Any]:
    """Serialize an Operation. K and V are assumed JSON-native here."""
    return {
        "kind": op.kind.value,
        "key": op.key,
        "value": op.value,
        "stamp": serialize_hlc(op.stamp),
    }


def deserialize_operation(payload: dict[str, Any]) -> Operation[Any, Any]:
    return Operation(
        kind=OpKind(payload["kind"]),
        key=payload["key"],
        value=payload.get("value"),
        stamp=deserialize_hlc(payload["stamp"]),
    )


# ---------------------------------------------------------------------------
# MeshPubSub :: abstract federation driver
# ---------------------------------------------------------------------------

class MeshPubSub(abc.ABC):
    """
    Abstract backend driver for **server-to-server federation only**.

    A ``MeshPubSub`` implementation is a dumb transport. It moves
    ``Operation`` objects between Python nodes. It is not aware of:

    * CRDT semantics (deduplication, conflict resolution, tombstones)
    * Browser clients (those live on the ``ClientGateway`` side)
    * Persistence (that lives on the ``ChronoLedger`` side)

    The orchestrator (``MeshNode``) installs a single callback via
    ``set_on_remote_op``; the driver invokes it for every inbound op
    received from a peer. The orchestrator is responsible for HLC
    dedup, CRDT merge, ledger persistence, and epidemic re-broadcast.
    """

    def __init__(self) -> None:
        self._on_remote_op: Optional[RemoteOpHandler] = None

    def set_on_remote_op(self, handler: Optional[RemoteOpHandler]) -> None:
        """
        Register the single callback invoked for every operation
        received from any federated peer. Passing ``None`` clears it.
        """
        self._on_remote_op = handler

    # -- lifecycle ----------------------------------------------------------

    @abc.abstractmethod
    async def start(self) -> None:
        """Bind any sockets and become ready to accept inbound peers."""

    @abc.abstractmethod
    async def stop(self) -> None:
        """Close every peer connection and tear down the transport."""

    # -- federation ---------------------------------------------------------

    @abc.abstractmethod
    async def connect_to(self, host: str, port: int) -> str:
        """
        Dial a peer node. Implementations should perform a handshake
        sufficient to learn the remote ``node_id`` and return it. If
        a duplicate connection is detected the existing channel must
        be kept and the new one closed.
        """

    @abc.abstractmethod
    async def publish(
        self,
        op: Operation[Any, Any],
        *,
        exclude_peer: Optional[str] = None,
    ) -> None:
        """
        Broadcast a serialized operation to every currently-federated
        peer. ``exclude_peer`` lets the orchestrator skip the sender
        when re-broadcasting an inbound op (epidemic gossip).
        """

    # -- introspection ------------------------------------------------------

    @property
    @abc.abstractmethod
    def peer_ids(self) -> set[str]:
        """Set of currently-connected federated peer node ids."""

    @property
    @abc.abstractmethod
    def host(self) -> str: ...

    @property
    @abc.abstractmethod
    def port(self) -> int: ...


# ---------------------------------------------------------------------------
# WebSocketMeshPubSub :: concrete WebSocket gossip driver
# ---------------------------------------------------------------------------

class WebSocketMeshPubSub(MeshPubSub):
    """
    WebSocket implementation of ``MeshPubSub``. Runs a server on
    ``(host, port)`` and maintains outbound client connections to dialled
    peers. Every channel is duplex once the hello-handshake has agreed
    on the remote ``node_id``.

    Edge cases handled:
        * Malformed JSON on the wire -> logged + skipped, channel kept.
        * Duplicate inbound/outbound channels to the same peer -> the
          second is closed; the first survives.
        * Self-connection attempts -> refused.
        * Peer drop mid-broadcast -> caught per-target, others succeed.
    """

    __slots__ = (
        "_node_id", "_host", "_port",
        "_connections", "_server", "_tasks", "_lock", "_closed",
    )

    def __init__(self, node_id: str, host: str = "127.0.0.1", port: int = 0) -> None:
        super().__init__()
        self._node_id = node_id
        self._host = host
        self._port = port
        # peer_id -> live WebSocket (server- or client-side, both duplex).
        self._connections: dict[str, Any] = {}
        self._server: Optional[Any] = None
        self._tasks: set[asyncio.Task[Any]] = set()
        self._lock = asyncio.Lock()
        self._closed = False

    # -- introspection ------------------------------------------------------

    @property
    def host(self) -> str:
        return self._host

    @property
    def port(self) -> int:
        return self._port

    @property
    def peer_ids(self) -> set[str]:
        return set(self._connections.keys())

    # -- lifecycle ----------------------------------------------------------

    async def start(self) -> None:
        if self._closed:
            raise RuntimeError("driver already stopped; construct a new one")
        if self._server is not None:
            return
        self._server = await serve(self._handle_inbound, self._host, self._port)
        # Resolve ephemeral ports if the caller asked for one.
        if self._port == 0:
            for sock in self._server.sockets:
                self._port = sock.getsockname()[1]
                break
        logger.info(
            "[%s] mesh-pubsub listening on ws://%s:%d",
            self._node_id, self._host, self._port,
        )

    async def stop(self) -> None:
        if self._closed:
            return
        self._closed = True

        # 1. Close every peer channel so read loops see ConnectionClosed.
        async with self._lock:
            conns = list(self._connections.values())
            self._connections.clear()
        for ws in conns:
            try:
                await ws.close()
            except Exception:  # noqa: BLE001  -- best-effort shutdown
                pass

        # 2. Cancel and drain background read tasks.
        for task in list(self._tasks):
            task.cancel()
        if self._tasks:
            await asyncio.gather(*self._tasks, return_exceptions=True)
        self._tasks.clear()

        # 3. Stop the server.
        if self._server is not None:
            self._server.close()
            await self._server.wait_closed()
            self._server = None

        logger.info("[%s] mesh-pubsub stopped", self._node_id)

    # -- federation: dial a peer -------------------------------------------

    async def connect_to(self, host: str, port: int) -> str:
        if self._closed:
            raise RuntimeError("driver stopped")
        uri = f"ws://{host}:{port}"
        ws = await connect(uri)
        try:
            await ws.send(json.dumps({"type": "hello", "node_id": self._node_id}))
            raw = await ws.recv()
            msg = json.loads(raw)
            if msg.get("type") != "hello" or "node_id" not in msg:
                await ws.close()
                raise RuntimeError(f"bad handshake from {uri}: {msg!r}")
            peer_id = str(msg["node_id"])
        except Exception:
            try:
                await ws.close()
            except Exception:  # noqa: BLE001
                pass
            raise

        if not await self._register_connection(peer_id, ws, direction="out"):
            try:
                await ws.close()
            except Exception:  # noqa: BLE001
                pass
            return peer_id

        self._spawn_read_loop(peer_id, ws)
        logger.info("[%s] outbound -> %s", self._node_id, peer_id)
        return peer_id

    # -- federation: server handler ----------------------------------------

    async def _handle_inbound(self, ws: Any) -> None:
        """Websockets server handler. One coroutine per inbound connection."""
        peer_id: Optional[str] = None
        try:
            raw = await ws.recv()
            try:
                msg = json.loads(raw)
            except json.JSONDecodeError:
                await ws.close()
                return
            if not isinstance(msg, dict) or msg.get("type") != "hello" \
               or "node_id" not in msg:
                await ws.close()
                return
            peer_id = str(msg["node_id"])
            await ws.send(json.dumps({"type": "hello", "node_id": self._node_id}))

            if not await self._register_connection(peer_id, ws, direction="in"):
                await ws.close()
                return

            logger.info("[%s] inbound  <- %s", self._node_id, peer_id)
            # Run the read loop *inline* on the handler task; when it
            # returns, the websockets library closes the connection.
            await self._read_loop(peer_id, ws)
        except ConnectionClosed:
            pass
        except Exception:  # noqa: BLE001
            logger.exception("[%s] inbound handler error", self._node_id)
        finally:
            if peer_id is not None:
                async with self._lock:
                    if self._connections.get(peer_id) is ws:
                        self._connections.pop(peer_id, None)

    async def _register_connection(
        self, peer_id: str, ws: Any, *, direction: str,
    ) -> bool:
        """Atomically claim ownership of a peer slot. False = duplicate."""
        if peer_id == self._node_id:
            logger.warning(
                "[%s] refusing self-connection (%s)", self._node_id, direction,
            )
            return False
        async with self._lock:
            if peer_id in self._connections:
                logger.info(
                    "[%s] duplicate %s connection to %s, dropping",
                    self._node_id, direction, peer_id,
                )
                return False
            self._connections[peer_id] = ws
            return True

    def _spawn_read_loop(self, peer_id: str, ws: Any) -> None:
        task = asyncio.create_task(self._owned_read_loop(peer_id, ws))
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)

    async def _owned_read_loop(self, peer_id: str, ws: Any) -> None:
        """Wrap _read_loop with the cleanup the server handler does inline."""
        try:
            await self._read_loop(peer_id, ws)
        finally:
            async with self._lock:
                if self._connections.get(peer_id) is ws:
                    self._connections.pop(peer_id, None)

    # -- federation: message pump ------------------------------------------

    async def _read_loop(self, peer_id: str, ws: Any) -> None:
        try:
            async for raw in ws:
                try:
                    msg = json.loads(raw)
                except json.JSONDecodeError:
                    logger.warning("[%s] bad JSON from %s", self._node_id, peer_id)
                    continue
                if not isinstance(msg, dict):
                    continue

                mtype = msg.get("type")
                if mtype == "op":
                    try:
                        op = deserialize_operation(msg["op"])
                    except (KeyError, ValueError, TypeError):
                        logger.warning(
                            "[%s] malformed op from %s",
                            self._node_id, peer_id,
                        )
                        continue
                    if self._on_remote_op is not None:
                        try:
                            await self._on_remote_op(op, peer_id)
                        except Exception:  # noqa: BLE001
                            logger.exception(
                                "[%s] on_remote_op subscriber raised",
                                self._node_id,
                            )
                # Other message types reserved for future milestones
                # (anti-entropy sync, ledger requests, etc.).
        except ConnectionClosed:
            pass

    # -- federation: publish ----------------------------------------------

    async def publish(
        self,
        op: Operation[Any, Any],
        *,
        exclude_peer: Optional[str] = None,
    ) -> None:
        if self._closed:
            return
        payload = json.dumps({"type": "op", "op": serialize_operation(op)})
        async with self._lock:
            targets = [
                (pid, ws) for pid, ws in self._connections.items()
                if pid != exclude_peer
            ]
        if not targets:
            return

        async def _send(pid: str, ws: Any) -> None:
            try:
                await ws.send(payload)
            except ConnectionClosed:
                logger.info(
                    "[%s] peer %s closed during broadcast",
                    self._node_id, pid,
                )
            except Exception:  # noqa: BLE001
                logger.exception("[%s] send to %s failed", self._node_id, pid)

        await asyncio.gather(*[_send(pid, ws) for pid, ws in targets])


# ---------------------------------------------------------------------------
# MeshNode :: orchestrator (CRDT + pubsub driver + on_op fanout)
# ---------------------------------------------------------------------------

class MeshNode:
    """
    Orchestrator that binds a CRDT ``Node`` to a ``MeshPubSub`` driver.

    Responsibilities (and *only* these):

    1. Own the local CRDT replica.
    2. Apply local mutations and publish them through the pubsub driver.
    3. Receive remote operations from the driver, deduplicate by HLC,
       merge into the CRDT, fan out to subscribers, and re-publish to
       all peers except the sender (epidemic gossip).

    The driver knows nothing about CRDTs. The CRDT knows nothing about
    networking. This class is the seam between them.

    Lifecycle::

        node = MeshNode("alpha", port=8001)
        await node.start()           # bind server
        await node.connect_to(host, port)   # dial peers
        await node.set("k", "v")     # local mutation, auto-gossiped
        await node.stop()            # clean shutdown
    """

    __slots__ = (
        "node",
        "_pubsub",
        "_seen",
        "_on_op",
    )

    def __init__(
        self,
        node_id: str,
        host: str = "127.0.0.1",
        port: int = 0,
        *,
        on_op: Optional[Callable[[Operation[Any, Any], Optional[str]], Awaitable[None]]] = None,
        pubsub: Optional[MeshPubSub] = None,
    ) -> None:
        self.node: Node[str, Any] = Node(node_id)
        # Default driver: WebSocket gossip on (host, port).
        self._pubsub: MeshPubSub = (
            pubsub if pubsub is not None
            else WebSocketMeshPubSub(node_id, host, port)
        )
        self._pubsub.set_on_remote_op(self._ingest_remote)
        # HLC stamps already processed (loop prevention for epidemic
        # gossip). Public-ish: ``ChronoLedger`` populates this during
        # replay so already-persisted ops are not re-broadcast on boot.
        self._seen: set[HybridLogicalClock] = set()
        # Optional hook: called for every op the mesh ingests (local
        # or remote). ``ChronoLedger`` and ``ClientGateway`` subscribe
        # here, fanned out via ``compose_hooks``.
        self._on_op = on_op

    # -- introspection ------------------------------------------------------

    @property
    def id(self) -> str:
        return self.node.id

    @property
    def host(self) -> str:
        return self._pubsub.host

    @property
    def port(self) -> int:
        return self._pubsub.port

    @property
    def peer_ids(self) -> set[str]:
        return self._pubsub.peer_ids

    @property
    def pubsub(self) -> MeshPubSub:
        return self._pubsub

    def get(self, key: str) -> Any:
        return self.node.get(key)

    def snapshot(self) -> dict[str, Any]:
        return self.node.store.snapshot()

    # -- lifecycle ----------------------------------------------------------

    async def start(self) -> None:
        await self._pubsub.start()

    async def stop(self) -> None:
        await self._pubsub.stop()

    async def connect_to(self, host: str, port: int) -> str:
        return await self._pubsub.connect_to(host, port)

    # -- mutation API (auto-gossip) ----------------------------------------

    async def set(self, key: str, value: Any) -> Operation[str, Any]:
        op = self.node.set(key, value)
        self._seen.add(op.stamp)
        if self._on_op is not None:
            await self._on_op(op, None)
        await self._pubsub.publish(op)
        return op

    async def delete(self, key: str) -> Operation[str, Any]:
        op = self.node.delete(key)
        self._seen.add(op.stamp)
        if self._on_op is not None:
            await self._on_op(op, None)
        await self._pubsub.publish(op)
        return op

    # -- remote ingest (called by the driver) ------------------------------

    async def _ingest_remote(
        self, op: Operation[Any, Any], source_peer: str,
    ) -> None:
        """
        Driver hook for an op received from a federated peer.

        Steps:
          1. HLC dedup -- already-seen ops are dropped on the floor.
          2. CRDT merge -- idempotent + commutative, safe on replay.
          3. Fan out to subscribers (ledger, gateway) via ``_on_op``.
          4. Epidemic re-publish to every peer except the sender.
        """
        if op.stamp in self._seen:
            return
        self._seen.add(op.stamp)
        self.node.receive(op)
        if self._on_op is not None:
            await self._on_op(op, source_peer)
        await self._pubsub.publish(op, exclude_peer=source_peer)


# ---------------------------------------------------------------------------
# Self-test :: three-node mesh in a non-trivial topology
# ---------------------------------------------------------------------------

async def _wait_until(predicate: Callable[[], bool], *, timeout: float = 2.0) -> bool:
    """Poll until ``predicate`` becomes true or ``timeout`` elapses."""
    loop = asyncio.get_event_loop()
    deadline = loop.time() + timeout
    while loop.time() < deadline:
        if predicate():
            return True
        await asyncio.sleep(0.005)
    return predicate()


async def _demo() -> None:
    logging.basicConfig(level=logging.INFO, format="%(message)s")

    print("=" * 72)
    print("Aether-Core :: Mesh PubSub :: self-test")
    print("=" * 72)

    n1 = MeshNode("alpha", port=8001)
    n2 = MeshNode("beta",  port=8002)
    n3 = MeshNode("gamma", port=8003)

    await n1.start()
    await n2.start()
    await n3.start()

    # LINEAR topology, deliberately not a full mesh:
    #
    #     alpha  <--->  beta  <--->  gamma
    #
    # alpha and gamma share NO direct WebSocket. If gamma still observes
    # alpha's writes (and vice versa), the epidemic relay has worked.
    print("\n[topology] alpha <-> beta <-> gamma  (no direct alpha<->gamma link)")
    await n1.connect_to("127.0.0.1", 8002)   # alpha dials beta
    await n2.connect_to("127.0.0.1", 8003)   # beta  dials gamma

    # Give the second hellos time to land before reading peer_ids.
    await asyncio.sleep(0.05)
    print(f"  alpha peers: {sorted(n1.peer_ids)}")
    print(f"  beta  peers: {sorted(n2.peer_ids)}")
    print(f"  gamma peers: {sorted(n3.peer_ids)}")
    assert n1.peer_ids == {"beta"},          f"alpha peers wrong: {n1.peer_ids}"
    assert n2.peer_ids == {"alpha", "gamma"}, f"beta peers wrong: {n2.peer_ids}"
    assert n3.peer_ids == {"beta"},          f"gamma peers wrong: {n3.peer_ids}"

    # ----- Test 1: forward propagation -----
    print("\n[phase 1] mutation on alpha must traverse beta to reach gamma")
    op = await n1.set("user:profile:name", "Aleph the First")
    print(f"  alpha emitted: {op.kind.value} {op.key!r} = {op.value!r}")
    print(f"  stamp        : {op.stamp.encode()}")

    ok = await _wait_until(
        lambda: n3.get("user:profile:name") == "Aleph the First", timeout=2.0
    )
    print(f"  gamma.get('user:profile:name') = {n3.get('user:profile:name')!r}")
    assert ok, "gamma did not receive alpha's mutation via beta"

    # ----- Test 2: reverse propagation -----
    print("\n[phase 2] mutation on gamma must traverse beta to reach alpha")
    await n3.set("session:token", "z99-from-gamma")
    ok = await _wait_until(
        lambda: n1.get("session:token") == "z99-from-gamma", timeout=2.0
    )
    print(f"  alpha.get('session:token') = {n1.get('session:token')!r}")
    assert ok, "alpha did not receive gamma's mutation via beta"

    # ----- Test 3: simultaneous concurrent writes from all three -----
    print("\n[phase 3] concurrent writes to the SAME key from all three nodes")
    ops = await asyncio.gather(
        n1.set("contested", "from-alpha"),
        n2.set("contested", "from-beta"),
        n3.set("contested", "from-gamma"),
    )
    for o in ops:
        print(f"  {o.stamp.node_id:>5} stamp = {o.stamp.encode()}")

    # Identify the deterministic winner by max HLC.
    winner = max(ops, key=lambda o: o.stamp)
    print(f"  expected winner (max HLC) = {winner.stamp.node_id} -> {winner.value!r}")

    ok = await _wait_until(
        lambda: (
            n1.get("contested") == n2.get("contested") == n3.get("contested") == winner.value
        ),
        timeout=3.0,
    )
    print(f"  alpha sees: {n1.get('contested')!r}")
    print(f"  beta  sees: {n2.get('contested')!r}")
    print(f"  gamma sees: {n3.get('contested')!r}")
    assert ok, "replicas did not converge to the HLC winner"

    # ----- Test 4: full state convergence -----
    print("\n[phase 4] full snapshot convergence across all replicas")
    s1, s2, s3 = n1.snapshot(), n2.snapshot(), n3.snapshot()
    print(f"  alpha: {s1}")
    print(f"  beta : {s2}")
    print(f"  gamma: {s3}")
    assert s1 == s2 == s3, "replicas diverged"

    # ----- Test 5: deletion propagates as a tombstone -----
    print("\n[phase 5] delete propagation (tombstone via beta)")
    await n1.delete("session:token")
    ok = await _wait_until(lambda: n3.get("session:token") is None, timeout=2.0)
    print(f"  gamma.get('session:token') = {n3.get('session:token')!r}")
    assert ok, "tombstone did not propagate"

    # ----- Test 6: driver is the only thing that knows the wire -----
    print("\n[phase 6] driver isolation: pubsub is a WebSocketMeshPubSub")
    assert isinstance(n1.pubsub, WebSocketMeshPubSub)
    assert isinstance(n1.pubsub, MeshPubSub)
    print(f"  alpha.pubsub = {type(n1.pubsub).__name__}  "
          f"(implements MeshPubSub: True)")

    print("\n" + "=" * 72)
    print("MESH SYNCHRONIZATION: PROVEN")
    print("=" * 72)

    await n1.stop()
    await n2.stop()
    await n3.stop()


if __name__ == "__main__":
    asyncio.run(_demo())
