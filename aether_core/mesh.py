"""
Aether-Core :: Holographic Execution Plane
==========================================

A decentralized WebSocket mesh that gossips CRDT operations between
``Node`` replicas. Each ``MeshNode`` runs a local WebSocket server *and*
maintains outbound client connections to known peers. There is no broker,
no leader, no consensus protocol; convergence is delegated entirely to
the Phase 1 CRDT layer. This module is purely a transport.

Wire protocol
-------------
Two message types, both JSON:

    {"type": "hello", "node_id": "<id>"}
    {"type": "op",    "op": {"kind": "set"|"del", "key": ..., "value": ..., "stamp": {...}}}

A "hello" is exchanged in both directions immediately after a TCP/WS
upgrade, so every connection becomes uniquely keyed by the remote
node_id. Duplicate connections to the same peer (e.g. both sides dial
each other simultaneously) are detected and closed.

Gossip semantics
----------------
- Local mutations: broadcast to every connected peer.
- Inbound operations: deduplicated by HLC stamp (which is globally
  unique by construction — node_id is part of the stamp), applied to
  the local Node, then re-broadcast to every peer *except* the sender.
  This produces epidemic gossip with bounded propagation: every op
  reaches every reachable node in at most ``diameter(mesh)`` hops, and
  the dedup set ensures it does not circulate forever.
"""
from __future__ import annotations

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
    "serialize_hlc",
    "deserialize_hlc",
    "serialize_operation",
    "deserialize_operation",
]

logger = logging.getLogger(__name__)


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
# MeshNode :: a CRDT replica wrapped in a P2P WebSocket transport
# ---------------------------------------------------------------------------

class MeshNode:
    """
    Asynchronous wrapper around a Phase 1 ``Node``.

    Lifecycle:
        node = MeshNode("alpha", port=8001)
        await node.start()           # bind server
        await node.connect_to(host, port)   # dial peers
        await node.set("k", "v")     # local mutation, auto-gossiped
        await node.stop()            # clean shutdown
    """

    __slots__ = (
        "node",
        "host",
        "port",
        "_connections",
        "_seen",
        "_server",
        "_tasks",
        "_lock",
        "_on_op",
    )

    def __init__(
        self,
        node_id: str,
        host: str = "127.0.0.1",
        port: int = 0,
        *,
        on_op: Optional[Callable[[Operation[Any, Any], Optional[str]], Awaitable[None]]] = None,
    ) -> None:
        self.node: Node[str, Any] = Node(node_id)
        self.host = host
        self.port = port
        # peer_id -> live WebSocket (server- or client-side, both are duplex)
        self._connections: dict[str, Any] = {}
        # HLC stamps already processed (loop prevention for epidemic gossip)
        self._seen: set[HybridLogicalClock] = set()
        self._server: Optional[Any] = None
        self._tasks: set[asyncio.Task[Any]] = set()
        self._lock = asyncio.Lock()
        # Optional hook: called for every op the mesh ingests (local or remote).
        # The next milestone (Chrono-Vector Storage) will subscribe via this.
        self._on_op = on_op

    # -- introspection ------------------------------------------------------

    @property
    def id(self) -> str:
        return self.node.id

    @property
    def peer_ids(self) -> set[str]:
        return set(self._connections.keys())

    def get(self, key: str) -> Any:
        return self.node.get(key)

    def snapshot(self) -> dict[str, Any]:
        return self.node.store.snapshot()

    # -- lifecycle ----------------------------------------------------------

    async def start(self) -> None:
        self._server = await serve(self._handle_inbound, self.host, self.port)
        # If the caller asked for an ephemeral port, resolve the real one.
        if self.port == 0:
            for sock in self._server.sockets:
                self.port = sock.getsockname()[1]
                break
        logger.info("[%s] listening on ws://%s:%d", self.id, self.host, self.port)

    async def stop(self) -> None:
        # 1. Close all peer connections so read loops see ConnectionClosed.
        conns = list(self._connections.values())
        self._connections.clear()
        for ws in conns:
            try:
                await ws.close()
            except Exception:  # noqa: BLE001  -- best-effort shutdown
                pass
        # 2. Cancel background read tasks.
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
        logger.info("[%s] stopped", self.id)

    # -- connection management ---------------------------------------------

    async def connect_to(self, host: str, port: int) -> str:
        """Dial a peer. Returns the discovered peer node_id."""
        uri = f"ws://{host}:{port}"
        ws = await connect(uri)
        try:
            await ws.send(json.dumps({"type": "hello", "node_id": self.id}))
            raw = await ws.recv()
            msg = json.loads(raw)
            if msg.get("type") != "hello" or "node_id" not in msg:
                await ws.close()
                raise RuntimeError(f"bad handshake from {uri}: {msg!r}")
            peer_id = str(msg["node_id"])
        except Exception:
            await ws.close()
            raise

        if not await self._register_connection(peer_id, ws, direction="out"):
            await ws.close()
            return peer_id

        self._spawn_read_loop(peer_id, ws)
        logger.info("[%s] outbound -> %s", self.id, peer_id)
        return peer_id

    async def _handle_inbound(self, ws: Any) -> None:
        """websockets server handler. One coroutine per inbound connection."""
        peer_id: Optional[str] = None
        try:
            raw = await ws.recv()
            msg = json.loads(raw)
            if msg.get("type") != "hello" or "node_id" not in msg:
                await ws.close()
                return
            peer_id = str(msg["node_id"])
            await ws.send(json.dumps({"type": "hello", "node_id": self.id}))

            if not await self._register_connection(peer_id, ws, direction="in"):
                await ws.close()
                return

            logger.info("[%s] inbound  <- %s", self.id, peer_id)
            # Run the read loop *inline* on the handler task; when it returns,
            # the websockets library closes the connection.
            await self._read_loop(peer_id, ws)
        except ConnectionClosed:
            pass
        except Exception:  # noqa: BLE001
            logger.exception("[%s] inbound handler error", self.id)
        finally:
            if peer_id is not None:
                async with self._lock:
                    if self._connections.get(peer_id) is ws:
                        self._connections.pop(peer_id, None)

    async def _register_connection(
        self, peer_id: str, ws: Any, *, direction: str
    ) -> bool:
        """Atomically claim ownership of a peer slot. False = duplicate."""
        if peer_id == self.id:
            logger.warning("[%s] refusing self-connection (%s)", self.id, direction)
            return False
        async with self._lock:
            if peer_id in self._connections:
                logger.info(
                    "[%s] duplicate %s connection to %s, dropping",
                    self.id, direction, peer_id,
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

    # -- message pump -------------------------------------------------------

    async def _read_loop(self, peer_id: str, ws: Any) -> None:
        try:
            async for raw in ws:
                try:
                    msg = json.loads(raw)
                except json.JSONDecodeError:
                    logger.warning("[%s] bad JSON from %s", self.id, peer_id)
                    continue

                mtype = msg.get("type")
                if mtype == "op":
                    try:
                        op = deserialize_operation(msg["op"])
                    except (KeyError, ValueError, TypeError):
                        logger.warning("[%s] malformed op from %s", self.id, peer_id)
                        continue
                    await self._ingest(op, source_peer=peer_id)
                # Other message types reserved for future milestones
                # (anti-entropy sync, ledger requests, etc.).
        except ConnectionClosed:
            pass

    async def _ingest(self, op: Operation[Any, Any], source_peer: Optional[str]) -> None:
        if op.stamp in self._seen:
            return
        self._seen.add(op.stamp)
        self.node.receive(op)
        if self._on_op is not None:
            await self._on_op(op, source_peer)
        # Epidemic relay: forward to every peer except the one that sent it.
        await self._broadcast(op, exclude={source_peer} if source_peer else set())

    async def _broadcast(self, op: Operation[Any, Any], exclude: set[Optional[str]]) -> None:
        payload = json.dumps({"type": "op", "op": serialize_operation(op)})
        async with self._lock:
            targets = [
                (pid, ws) for pid, ws in self._connections.items() if pid not in exclude
            ]
        if not targets:
            return

        async def _send(pid: str, ws: Any) -> None:
            try:
                await ws.send(payload)
            except ConnectionClosed:
                logger.info("[%s] peer %s closed during broadcast", self.id, pid)
            except Exception:  # noqa: BLE001
                logger.exception("[%s] send to %s failed", self.id, pid)

        await asyncio.gather(*[_send(pid, ws) for pid, ws in targets])

    # -- mutation API (auto-gossip) ----------------------------------------

    async def set(self, key: str, value: Any) -> Operation[str, Any]:
        op = self.node.set(key, value)
        self._seen.add(op.stamp)
        if self._on_op is not None:
            await self._on_op(op, None)
        await self._broadcast(op, exclude=set())
        return op

    async def delete(self, key: str) -> Operation[str, Any]:
        op = self.node.delete(key)
        self._seen.add(op.stamp)
        if self._on_op is not None:
            await self._on_op(op, None)
        await self._broadcast(op, exclude=set())
        return op


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
    print("Aether-Core :: Holographic Execution Plane :: self-test")
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

    print("\n" + "=" * 72)
    print("MESH SYNCHRONIZATION: PROVEN")
    print("=" * 72)

    await n1.stop()
    await n2.stop()
    await n3.stop()


if __name__ == "__main__":
    asyncio.run(_demo())
