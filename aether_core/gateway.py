"""
Aether-Core :: Client Gateway (browser <-> server only)
=======================================================

This module is the **client-facing** half of the Aether-Core server.
It runs a dedicated WebSocket endpoint that **browser tabs** connect
to. It does NOT participate in federation; node-to-node traffic is
handled exclusively by ``aether_core.mesh.MeshPubSub``.

Adapter-pattern separation
--------------------------
* ``ClientGateway`` -- browser <-> server (this file)
* ``MeshPubSub``    -- server <-> server (``aether_core/mesh.py``)
* ``ChronoLedger``  -- server <-> disk   (``aether_core/storage.py``)

All three subscribe to the same ``MeshNode.on_op`` stream via
``compose_hooks`` and never reach across each other's boundaries.

Wire protocol (browser <-> gateway)
-----------------------------------
Browser -> gateway::

    {"type": "set",      "key": "<str>", "value": <json>}
    {"type": "delete",   "key": "<str>"}
    {"type": "presence", "x": <int>, "y": <int>}     # ephemeral cursor

Gateway -> browser::

    {"type": "hello",          "id": "<uuid>", "color": "<hsl>"}    # on connect
    {"type": "snapshot",       "data": {"<key>": <json>, ...}}      # on connect
    {"type": "set",            "key": "<str>", "value": <json>}     # mutation
    {"type": "delete",         "key": "<str>"}                      # tombstone
    {"type": "presence",       "id": "<uuid>", "color": "<hsl>",
                                "x": <int>, "y": <int>}              # cursor relay
    {"type": "presence-leave", "id": "<uuid>"}                       # disconnect

The gateway is intentionally "dumb" about CRDT semantics: it converts
inbound JSON to ``mesh.set/delete`` calls (which generate HLC stamps
on the Python side) and converts outbound ``Operation``s to JSON
messages. All conflict resolution, persistence, and gossip happen in
the existing Python layers; the browser only ever sees the resolved
state.
"""
from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import uuid
from typing import Any, Awaitable, Callable, Optional

from websockets import serve
from websockets.exceptions import ConnectionClosed

from aether_core.crdt import OpKind, Operation
from aether_core.mesh import MeshNode

__all__ = ["ClientGateway", "compose_hooks"]

logger = logging.getLogger(__name__)


def compose_hooks(
    *hooks: Optional[Callable[[Operation[Any, Any], Optional[str]], Awaitable[None]]],
) -> Callable[[Operation[Any, Any], Optional[str]], Awaitable[None]]:
    """
    Fan a single ``on_op`` event out to multiple async subscribers.

    The mesh layer's ``on_op`` is a single callable, but in practice
    we need both the ``ChronoLedger`` (for persistence) and the
    ``ClientGateway`` (for browser push) listening to the same
    stream. ``compose_hooks`` bundles them into one callback that
    invokes each in declaration order; ``None`` entries are skipped
    so the helper is also safe to use when some subscribers are
    optional. Exceptions raised by any one hook are logged but do
    not abort the others.
    """
    real_hooks = [h for h in hooks if h is not None]

    async def composed(op: Operation[Any, Any], source_peer: Optional[str]) -> None:
        for hook in real_hooks:
            try:
                await hook(op, source_peer)
            except Exception:  # noqa: BLE001
                logger.exception("on_op subscriber raised")

    return composed


def _color_for(client_id: str) -> str:
    """
    Deterministically derive a vibrant HSL colour string from a client
    id. Same client always gets the same colour across reconnects.
    Wide hue spread + fixed saturation/lightness keeps cursors
    readable on the dark canvas regardless of which colours adjacent
    peers happen to get.
    """
    h = int(hashlib.sha1(client_id.encode("utf-8")).hexdigest()[:6], 16)
    hue = h % 360
    return f"hsl({hue}, 78%, 62%)"


class ClientGateway:
    """
    WebSocket endpoint for **thin browser clients only**.

    Bind to a running ``MeshNode``, expose a port, and wire the
    gateway into the mesh's ``on_op`` stream via ``compose_hooks``.
    The gateway never speaks the federation protocol -- if you need
    to peer with another Python node, configure ``MeshNode``'s
    ``MeshPubSub`` driver instead.
    """

    __slots__ = (
        "_mesh", "_host", "_port",
        "_clients",          # set of live WebSocket connections
        "_client_index",     # ws -> {"id": str, "color": str}
        "_server", "_lock", "_closed",
    )

    def __init__(
        self,
        mesh_node: MeshNode,
        host: str = "127.0.0.1",
        port: int = 0,
    ) -> None:
        self._mesh = mesh_node
        self._host = host
        self._port = port
        # Live browser sessions, tracked as a set of WebSockets.
        self._clients: set[Any] = set()
        # Per-client metadata for presence (cursor) broadcasts.
        # Cursor positions live ONLY in memory and in transit -- never
        # in the CRDT, never in the ledger.
        self._client_index: dict[Any, dict[str, str]] = {}
        self._server: Optional[Any] = None
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
    def url(self) -> str:
        return f"ws://{self._host}:{self._port}"

    @property
    def client_count(self) -> int:
        return len(self._clients)

    # -- lifecycle ----------------------------------------------------------

    async def start(self) -> None:
        if self._closed:
            raise RuntimeError("gateway already stopped; construct a new one")
        if self._server is not None:
            return
        self._server = await serve(self._handle_client, self._host, self._port)
        if self._port == 0:
            for sock in self._server.sockets:
                self._port = sock.getsockname()[1]
                break
        logger.info("[gateway] browser endpoint live on %s", self.url)

    async def stop(self) -> None:
        if self._closed:
            return
        self._closed = True
        # Close all open browser sessions.
        async with self._lock:
            clients = list(self._clients)
            self._clients.clear()
            self._client_index.clear()
        for ws in clients:
            try:
                await ws.close()
            except Exception:  # noqa: BLE001
                pass
        # Shut the server.
        if self._server is not None:
            self._server.close()
            await self._server.wait_closed()
            self._server = None
        logger.info("[gateway] stopped")

    # -- mesh subscriber ----------------------------------------------------

    async def on_op(
        self, op: Operation[Any, Any], source_peer: Optional[str],
    ) -> None:
        """
        Hook for ``MeshNode``'s on_op stream. Fans every operation
        observed by the mesh (local or remote) out to every connected
        browser client. ``source_peer`` is ignored for the browser
        protocol -- browsers see fully resolved state changes, not
        raw gossip identities.
        """
        if self._closed:
            return
        async with self._lock:
            clients = list(self._clients)
        if not clients:
            return

        if op.kind is OpKind.SET:
            payload: dict[str, Any] = {
                "type": "set", "key": op.key, "value": op.value,
            }
        else:
            payload = {"type": "delete", "key": op.key}
        message = json.dumps(payload, separators=(",", ":"))

        await asyncio.gather(
            *[self._safe_send(ws, message) for ws in clients],
            return_exceptions=True,
        )

    # -- browser session ----------------------------------------------------

    async def _handle_client(self, ws: Any) -> None:
        # Mint a stable, server-side identity for this browser session.
        client_id = str(uuid.uuid4())
        color     = _color_for(client_id)
        async with self._lock:
            self._clients.add(ws)
            self._client_index[ws] = {"id": client_id, "color": color}
        logger.info(
            "[gateway] client %s connected (total=%d)",
            client_id[:8], len(self._clients),
        )

        try:
            # 1. Tell the client its own identity. Used so the browser
            #    can ignore its own cursor echoes and label itself.
            await ws.send(json.dumps({
                "type":  "hello",
                "id":    client_id,
                "color": color,
            }, separators=(",", ":")))

            # 2. Push the current durable state snapshot.
            snapshot = self._mesh.snapshot()
            await ws.send(json.dumps(
                {"type": "snapshot", "data": snapshot},
                separators=(",", ":"),
            ))

            # 3. Accept inbound messages (set/delete/presence). Anything
            #    malformed is silently dropped -- the gateway is a
            #    public endpoint and must not crash on bad input.
            async for raw in ws:
                await self._handle_client_message(raw, ws)

        except ConnectionClosed:
            pass
        except Exception:  # noqa: BLE001
            logger.exception(
                "[gateway] client %s handler crashed", client_id[:8],
            )
        finally:
            async with self._lock:
                self._clients.discard(ws)
                self._client_index.pop(ws, None)
            logger.info(
                "[gateway] client %s disconnected (total=%d)",
                client_id[:8], len(self._clients),
            )
            # Tell remaining peers this cursor is gone so they can
            # fade it out instead of leaving a stale dot on the canvas.
            await self._announce_leave(client_id)

    async def _handle_client_message(
        self, raw: str | bytes, sender_ws: Any,
    ) -> None:
        # 1. Parse JSON. Bad payload -> drop the message.
        try:
            msg = json.loads(raw)
        except (json.JSONDecodeError, TypeError, UnicodeDecodeError):
            return
        if not isinstance(msg, dict):
            return

        mtype = msg.get("type")

        # 2. Ephemeral presence (cursor) -- relay only, never persisted.
        if mtype == "presence":
            await self._relay_presence(sender_ws, msg)
            return

        # 3. Durable mutations -- go through the CRDT + ledger.
        key = msg.get("key")
        if not isinstance(key, str) or not key:
            return
        try:
            if mtype == "set":
                # Value can be any JSON-encodable thing the browser
                # sent. The CRDT layer is type-agnostic.
                await self._mesh.set(key, msg.get("value"))
            elif mtype == "delete":
                await self._mesh.delete(key)
            # Unknown types: ignore. Forward-compat with future
            # protocol extensions (subscriptions, range queries, ...).
        except Exception:  # noqa: BLE001
            # A failing mesh write must not bring down the client
            # connection. Log and continue accepting messages.
            logger.exception("[gateway] mesh write failed for %r", key)

    async def _relay_presence(
        self, sender_ws: Any, msg: dict[str, Any],
    ) -> None:
        """
        Relay an ephemeral cursor update to every OTHER connected
        client. This path deliberately bypasses the mesh and the
        ledger -- cursor coordinates have no business in the durable
        event log.
        """
        meta = self._client_index.get(sender_ws)
        if not meta:
            return
        # Coerce + clamp to ints so we don't waste bytes on float jitter.
        try:
            x = int(msg.get("x", 0))
            y = int(msg.get("y", 0))
        except (TypeError, ValueError):
            return
        outbound = json.dumps({
            "type":  "presence",
            "id":    meta["id"],
            "color": meta["color"],
            "x":     x,
            "y":     y,
        }, separators=(",", ":"))

        async with self._lock:
            peers = [ws for ws in self._clients if ws is not sender_ws]
        if not peers:
            return
        await asyncio.gather(
            *[self._safe_send(ws, outbound) for ws in peers],
            return_exceptions=True,
        )

    async def _announce_leave(self, client_id: str) -> None:
        """Broadcast a presence-leave so peers can remove this cursor."""
        if self._closed:
            return
        outbound = json.dumps(
            {"type": "presence-leave", "id": client_id},
            separators=(",", ":"),
        )
        async with self._lock:
            peers = list(self._clients)
        if not peers:
            return
        await asyncio.gather(
            *[self._safe_send(ws, outbound) for ws in peers],
            return_exceptions=True,
        )

    async def _safe_send(self, ws: Any, message: str) -> None:
        try:
            await ws.send(message)
        except ConnectionClosed:
            pass
        except Exception:  # noqa: BLE001
            logger.exception("[gateway] send to client failed")


# ---------------------------------------------------------------------------
# Self-test :: simulated browser clients prove end-to-end gateway sync
# ---------------------------------------------------------------------------

async def _demo() -> None:
    import tempfile
    import shutil
    from pathlib import Path

    from websockets import connect

    from aether_core.storage import ChronoLedger

    logging.basicConfig(level=logging.INFO, format="%(message)s")

    print("=" * 72)
    print("Aether-Core :: Client Gateway :: self-test")
    print("=" * 72)

    workdir = Path(tempfile.mkdtemp(prefix="aether-gateway-"))
    ledger_path = workdir / "ledger_gateway_demo.jsonl"

    # ----- assemble the stack ----------------------------------------------
    # ChronoLedger and ClientGateway both subscribe to the mesh's on_op
    # stream. compose_hooks fans the single mesh callback out to both.
    ledger = ChronoLedger(ledger_path)
    placeholder_gateway: dict[str, ClientGateway] = {}

    async def composed_on_op(
        op: Operation[Any, Any], src: Optional[str],
    ) -> None:
        await ledger.on_op(op, src)
        gw = placeholder_gateway.get("g")
        if gw is not None:
            await gw.on_op(op, src)

    mesh = MeshNode("alpha", port=8201, on_op=composed_on_op)
    gateway = ClientGateway(mesh, host="127.0.0.1", port=8211)
    placeholder_gateway["g"] = gateway

    print("\n[stack]")
    print(f"  ledger    : {ledger_path}")
    print(f"  mesh peer : ws://127.0.0.1:8201  (federation, MeshPubSub)")
    print(f"  gateway   : {gateway.url}  (browser clients only)")

    await ledger.boot(mesh)
    await mesh.start()
    await gateway.start()

    # Pre-seed the mesh with a value, so we can prove the snapshot
    # mechanism actually delivers existing state to new clients.
    await mesh.set("preexisting", "I was here first")
    await ledger.flush()

    # ----- simulated browser clients --------------------------------------
    async def open_browser_client(label: str) -> tuple[Any, asyncio.Queue]:
        ws = await connect(gateway.url)
        inbox: asyncio.Queue = asyncio.Queue()

        async def reader() -> None:
            try:
                async for raw in ws:
                    await inbox.put(json.loads(raw))
            except ConnectionClosed:
                pass

        asyncio.create_task(reader(), name=f"reader:{label}")
        return ws, inbox

    async def wait_for(
        inbox: asyncio.Queue,
        predicate: Callable[[dict[str, Any]], bool],
        timeout: float = 2.0,
    ) -> dict[str, Any]:
        loop = asyncio.get_event_loop()
        deadline = loop.time() + timeout
        while loop.time() < deadline:
            remaining = deadline - loop.time()
            try:
                msg = await asyncio.wait_for(inbox.get(), timeout=remaining)
            except asyncio.TimeoutError:
                break
            if predicate(msg):
                return msg
        raise AssertionError("predicate did not match within timeout")

    print("\n[phase 1] two simulated browsers connect, both receive initial snapshot")
    print("-" * 72)
    ws_a, inbox_a = await open_browser_client("A")
    ws_b, inbox_b = await open_browser_client("B")

    snap_a = await wait_for(inbox_a, lambda m: m.get("type") == "snapshot")
    snap_b = await wait_for(inbox_b, lambda m: m.get("type") == "snapshot")
    print(f"  tab A snapshot : {snap_a['data']}")
    print(f"  tab B snapshot : {snap_b['data']}")
    assert snap_a["data"] == snap_b["data"] == {"preexisting": "I was here first"}

    print("\n[phase 2] tab A writes -> tab B observes (and vice versa)")
    print("-" * 72)
    await ws_a.send(json.dumps({"type": "set", "key": "shared:msg", "value": "hello from A"}))
    msg = await wait_for(
        inbox_b,
        lambda m: m.get("type") == "set" and m.get("key") == "shared:msg",
    )
    print(f"  tab B received from A: {msg['key']!r} = {msg['value']!r}")
    assert msg["value"] == "hello from A"

    await ws_b.send(json.dumps({"type": "set", "key": "counter", "value": 42}))
    msg = await wait_for(
        inbox_a,
        lambda m: m.get("type") == "set" and m.get("key") == "counter",
    )
    print(f"  tab A received from B: {msg['key']!r} = {msg['value']!r}")
    assert msg["value"] == 42

    print("\n[phase 3] a THIRD tab joins late -> gets the full current snapshot")
    print("-" * 72)
    ws_c, inbox_c = await open_browser_client("C")
    snap_c = await wait_for(inbox_c, lambda m: m.get("type") == "snapshot")
    print(f"  tab C snapshot : {snap_c['data']}")
    assert snap_c["data"]["preexisting"]  == "I was here first"
    assert snap_c["data"]["shared:msg"]   == "hello from A"
    assert snap_c["data"]["counter"]      == 42

    print("\n[phase 4] delete propagates as 'delete' message (not absence in snapshot)")
    print("-" * 72)
    await ws_a.send(json.dumps({"type": "delete", "key": "shared:msg"}))
    msg = await wait_for(
        inbox_c,
        lambda m: m.get("type") == "delete" and m.get("key") == "shared:msg",
    )
    print(f"  tab C received: delete {msg['key']!r}")
    assert mesh.get("shared:msg") is None

    print("\n[phase 5] malformed JSON from a client must not kill the gateway")
    print("-" * 72)
    await ws_a.send("this is not json at all")
    await ws_a.send(json.dumps({"type": "set"}))  # missing key
    await ws_a.send(json.dumps({"type": "set", "key": "", "value": 1}))  # empty key
    # Gateway must still be alive and responsive afterwards.
    await ws_a.send(json.dumps({"type": "set", "key": "post:malformed", "value": "ok"}))
    msg = await wait_for(
        inbox_b,
        lambda m: m.get("type") == "set" and m.get("key") == "post:malformed",
    )
    print(f"  tab B received post-malformed set: {msg['value']!r}")
    assert msg["value"] == "ok"

    print("\n[phase 6] ledger persisted everything (boot a fresh instance)")
    print("-" * 72)
    canonical_fp = mesh.node.store.state_fingerprint()

    await ws_a.close()
    await ws_b.close()
    await ws_c.close()
    await gateway.stop()
    await mesh.stop()
    await ledger.close()

    # Cold boot from the same ledger file -- no mesh peers, no gateway.
    ledger2 = ChronoLedger(ledger_path)
    mesh2 = MeshNode("alpha", port=8202, on_op=ledger2.on_op)
    replayed = await ledger2.boot(mesh2)
    fresh_fp = mesh2.node.store.state_fingerprint()
    print(f"  replayed {replayed} ops from ledger written via the gateway")
    print(
        f"  fingerprint matches the pre-shutdown state: "
        f"{fresh_fp == canonical_fp}"
    )
    assert fresh_fp == canonical_fp
    await ledger2.close()

    shutil.rmtree(workdir, ignore_errors=True)

    print("\n" + "=" * 72)
    print("CLIENT GATEWAY: PROVEN  (snapshot + sync + malformed-input safety + persistence)")
    print("=" * 72)


if __name__ == "__main__":
    asyncio.run(_demo())
