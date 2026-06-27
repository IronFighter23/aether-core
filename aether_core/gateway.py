"""
Aether-Core :: Client Gateway (browser <-> server, hardened)
============================================================

V3 changes vs V2
----------------
The gateway is now defensible against hostile, buggy, and slow clients.
The threat model and every applied mitigation are documented in
``SECURITY.md``; this module owns the enforcement.

* **Payload caps** -- both the WebSocket frame size and the application-
  level JSON message size are bounded. Oversize frames are rejected by
  the websockets library before they enter Python's address space.
* **Per-connection token bucket** -- limits messages/second per client.
  Connections that overrun are closed, not back-pressured -- this
  protects honest clients from a single noisy peer.
* **Connection caps** -- global concurrent connection limit and a
  per-source-IP limit. New connections beyond the cap are refused.
* **Slow-loris timeout** -- a connection that does not produce its
  first message within ``handshake_timeout_s`` is closed.

Every limit is configurable per ``ClientGateway`` instance via the
``limits=`` constructor parameter. Defaults live in
``SecurityLimits`` (``aether_core/_security.py``).

Wire protocol (browser <-> gateway) — also asserted in
``tests/test_protocol_conformance.py`` so this doc cannot drift::

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
"""
from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import ssl
import uuid
from typing import Any, Awaitable, Callable, Optional
from urllib.parse import parse_qs, urlparse

from websockets import serve
from websockets.exceptions import ConnectionClosed

from aether_core._security import (
    AuthConfig,
    ConnectionCounter,
    ConnectionLimitError,
    PayloadTooLargeError,
    SecurityLimits,
    TokenBucket,
    validate_key,
    validate_payload,
    validate_value,
    with_handshake_timeout,
)
from aether_core.crdt import OpKind, Operation
from aether_core.mesh import MeshNode

__all__ = ["ClientGateway", "compose_hooks"]

logger = logging.getLogger(__name__)


def compose_hooks(
    *hooks: Optional[Callable[[Operation[Any, Any], Optional[str]], Awaitable[None]]],
) -> Callable[[Operation[Any, Any], Optional[str]], Awaitable[None]]:
    """
    Fan a single ``on_op`` event out to multiple async subscribers.

    Exceptions from any one subscriber are logged but do not abort the
    others, so a buggy ledger writer cannot prevent the gateway from
    pushing updates to browsers.
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
    """Deterministic HSL colour for a stable cursor identity."""
    h = int(hashlib.sha1(client_id.encode("utf-8")).hexdigest()[:6], 16)
    return f"hsl({h % 360}, 78%, 62%)"


def _remote_addr(ws: Any) -> str:
    """Best-effort remote IP extraction. Falls back to '?' if unavailable."""
    try:
        addr = ws.remote_address  # (host, port) for IPv4, more for IPv6
        if addr and len(addr) >= 1:
            return str(addr[0])
    except Exception:  # noqa: BLE001
        pass
    return "?"


def _extract_query_token(ws: Any) -> Optional[str]:
    """
    Best-effort: pull ``?auth_token=...`` out of the upgrade request URL.

    Browsers cannot set custom WebSocket headers, so the auth token is
    most conveniently delivered as a query parameter on the WS URL.
    Server-side, the request path is available on the websockets
    connection object under different attribute names depending on the
    library version (``request.path`` on >=12, ``path`` on older).
    Probe both and return ``None`` if neither is present so the caller
    can fall back to the first-message auth path.
    """
    path: Optional[str] = None
    req = getattr(ws, "request", None)
    if req is not None:
        path = getattr(req, "path", None)
    if path is None:
        path = getattr(ws, "path", None)
    if not isinstance(path, str) or "?" not in path:
        return None
    try:
        parsed = urlparse(path if path.startswith(("/", "ws")) else "/" + path)
        qs = parse_qs(parsed.query)
    except Exception:  # noqa: BLE001
        return None
    vals = qs.get("auth_token")
    if not vals:
        return None
    return vals[0]


class ClientGateway:
    """
    Hardened WebSocket endpoint for browser clients.

    Construction::

        gw = ClientGateway(
            mesh_node,
            host="127.0.0.1",
            port=8211,
            limits=SecurityLimits(messages_per_second=200, max_connections_total=512),
        )
        await gw.start()
        # ...
        await gw.stop()
    """

    __slots__ = (
        "_mesh", "_host", "_port",
        "_limits", "_auth", "_ssl",
        "_clients",
        "_client_index",         # ws -> {"id": str, "color": str, "source": str}
        "_buckets",              # ws -> TokenBucket
        "_conn_counter",
        "_server", "_lock", "_closed",
    )

    def __init__(
        self,
        mesh_node: MeshNode,
        host: str = "127.0.0.1",
        port: int = 0,
        *,
        limits: Optional[SecurityLimits] = None,
        auth: Optional[AuthConfig] = None,
        ssl_context: Optional[ssl.SSLContext] = None,
    ) -> None:
        self._mesh = mesh_node
        self._host = host
        self._port = port
        self._limits = limits or SecurityLimits()
        self._auth = auth or AuthConfig()
        self._ssl = ssl_context
        self._clients: set[Any] = set()
        self._client_index: dict[Any, dict[str, str]] = {}
        self._buckets: dict[Any, TokenBucket] = {}
        self._conn_counter = ConnectionCounter(limits=self._limits)
        self._server: Optional[Any] = None
        self._lock = asyncio.Lock()
        self._closed = False

    # -- introspection ------------------------------------------------------

    @property
    def host(self) -> str:        return self._host
    @property
    def port(self) -> int:        return self._port
    @property
    def url(self) -> str:
        scheme = "wss" if self._ssl else "ws"
        return f"{scheme}://{self._host}:{self._port}"
    @property
    def client_count(self) -> int: return len(self._clients)
    @property
    def limits(self) -> SecurityLimits: return self._limits
    @property
    def auth(self) -> AuthConfig:  return self._auth

    # -- lifecycle ----------------------------------------------------------

    async def start(self) -> None:
        if self._closed:
            raise RuntimeError("gateway already stopped; construct a new one")
        if self._server is not None:
            return
        self._server = await serve(
            self._handle_client,
            self._host,
            self._port,
            # Tell websockets to reject oversize frames at the protocol
            # level, before any bytes hit Python heap allocations.
            max_size=self._limits.max_frame_bytes,
            ssl=self._ssl,
        )
        if self._port == 0:
            for sock in self._server.sockets:
                self._port = sock.getsockname()[1]
                break
        logger.info(
            "[gateway] browser endpoint live on %s "
            "(rate=%.0f msg/s, max_conn=%d, frame_cap=%d B, auth=%s, tls=%s)",
            self.url,
            self._limits.messages_per_second,
            self._limits.max_connections_total,
            self._limits.max_frame_bytes,
            "required" if self._auth.required else "off",
            "on" if self._ssl else "off",
        )

    async def stop(self) -> None:
        if self._closed:
            return
        self._closed = True
        async with self._lock:
            clients = list(self._clients)
            self._clients.clear()
            self._client_index.clear()
            self._buckets.clear()
        for ws in clients:
            try:
                await ws.close()
            except Exception:  # noqa: BLE001
                pass
        if self._server is not None:
            self._server.close()
            await self._server.wait_closed()
            self._server = None
        logger.info("[gateway] stopped")

    # -- mesh subscriber ----------------------------------------------------

    async def on_op(
        self, op: Operation[Any, Any], source_peer: Optional[str],
    ) -> None:
        """Mesh on_op hook -- broadcast resolved mutations to every client."""
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
        # 1. Enforce connection caps BEFORE the session starts.
        source = _remote_addr(ws)
        try:
            self._conn_counter.acquire(source)
        except ConnectionLimitError as e:
            logger.warning("[gateway] refused %s: %s", source, e)
            try:
                await ws.close(code=1013, reason="server busy")  # 1013 = Try Again Later
            except Exception:  # noqa: BLE001
                pass
            return

        client_id = str(uuid.uuid4())
        color     = _color_for(client_id)
        bucket    = TokenBucket(
            capacity=self._limits.messages_burst,
            refill_per_second=self._limits.messages_per_second,
        )

        # 2. AUTHENTICATE (if required) BEFORE the connection is registered
        #    against _clients and BEFORE the snapshot is sent. A client
        #    that fails auth must never see the state. Two delivery modes
        #    are accepted (browsers cannot set custom WS headers, so we
        #    take the token via URL or first-message):
        #      a) ?auth_token=... query param on the WebSocket URL
        #      b) {"type": "auth", "token": "..."} as the first frame
        #    Mode (a) is faster -- we never enter the client into the
        #    registry, never spend a snapshot serialization on a rejected
        #    peer. Mode (b) is the safety net for clients whose URL
        #    happened to drop the query string.
        if self._auth.required:
            url_token = _extract_query_token(ws)
            if url_token is not None:
                if not self._auth.verify(url_token):
                    logger.warning(
                        "[gateway] refused %s: bad auth_token in URL",
                        source,
                    )
                    try:
                        await ws.close(code=1008, reason="auth failed")
                    except Exception:  # noqa: BLE001
                        pass
                    self._conn_counter.release(source)
                    return
            else:
                # Wait for the first-message auth frame. Slow-loris guard
                # ensures we cannot be parked here indefinitely.
                try:
                    auth_raw = await with_handshake_timeout(
                        ws.recv(), limits=self._limits, what="client auth",
                    )
                except asyncio.TimeoutError:
                    self._conn_counter.release(source)
                    return
                try:
                    text = validate_payload(auth_raw, self._limits)
                    auth_msg = json.loads(text)
                except (PayloadTooLargeError, ValueError, json.JSONDecodeError):
                    try:
                        await ws.close(code=1008, reason="auth failed")
                    except Exception:  # noqa: BLE001
                        pass
                    self._conn_counter.release(source)
                    return
                if (
                    not isinstance(auth_msg, dict)
                    or auth_msg.get("type") != "auth"
                    or not self._auth.verify(auth_msg.get("token"))
                ):
                    logger.warning(
                        "[gateway] refused %s: auth handshake failed",
                        source,
                    )
                    try:
                        await ws.close(code=1008, reason="auth failed")
                    except Exception:  # noqa: BLE001
                        pass
                    self._conn_counter.release(source)
                    return

        # 3. Register the (now authenticated) client.
        async with self._lock:
            self._clients.add(ws)
            self._client_index[ws] = {"id": client_id, "color": color, "source": source}
            self._buckets[ws] = bucket
        logger.info(
            "[gateway] client %s connected from %s (total=%d, source=%d)",
            client_id[:8], source, len(self._clients),
            self._conn_counter.for_source(source),
        )

        try:
            # 4. Send hello + snapshot.
            await ws.send(json.dumps({
                "type": "hello", "id": client_id, "color": color,
            }, separators=(",", ":")))
            await ws.send(json.dumps(
                {"type": "snapshot", "data": self._mesh.snapshot()},
                separators=(",", ":"),
            ))

            # 5. Slow-loris guard: the FIRST inbound message must arrive
            #    within handshake_timeout_s, or we close the socket.
            try:
                first_raw = await with_handshake_timeout(
                    ws.recv(), limits=self._limits, what="client first-message",
                )
            except asyncio.TimeoutError:
                return
            await self._handle_client_message(first_raw, ws)

            # 6. Steady-state message loop with rate limiting.
            async for raw in ws:
                if not bucket.try_consume():
                    logger.warning(
                        "[gateway] client %s exceeded rate budget, closing",
                        client_id[:8],
                    )
                    try:
                        await ws.close(code=1008, reason="rate limit")
                    except Exception:  # noqa: BLE001
                        pass
                    return
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
                self._buckets.pop(ws, None)
            self._conn_counter.release(source)
            logger.info(
                "[gateway] client %s disconnected (total=%d)",
                client_id[:8], len(self._clients),
            )
            await self._announce_leave(client_id)

    async def _handle_client_message(
        self, raw: Any, sender_ws: Any,
    ) -> None:
        # 1. Application-level payload validation (size, encoding).
        try:
            text = validate_payload(raw, self._limits)
        except (PayloadTooLargeError, ValueError) as e:
            logger.info("[gateway] dropping bad payload: %s", e)
            return

        # 2. JSON parse with strict error handling. Anything that is not
        #    a JSON object is dropped on the floor.
        try:
            msg = json.loads(text)
        except (json.JSONDecodeError, ValueError):
            return
        if not isinstance(msg, dict):
            return

        mtype = msg.get("type")

        # 3. Presence (cursor) -- relay only, never persisted.
        if mtype == "presence":
            await self._relay_presence(sender_ws, msg)
            return

        # 3b. Duplicate auth frames after a successful handshake are
        #     ignored. (Clients that talk to both an auth-required and
        #     an auth-disabled gateway may send the same first frame
        #     in either order; we accept the redundancy silently.)
        if mtype == "auth":
            return

        # 4. Durable mutations -- validate key and value, then push
        #    through the mesh.
        try:
            key = validate_key(msg.get("key"), self._limits)
        except (PayloadTooLargeError, ValueError) as e:
            logger.info("[gateway] dropping mutation with bad key: %s", e)
            return

        try:
            if mtype == "set":
                value = validate_value(msg.get("value"), self._limits)
                await self._mesh.set(key, value)
            elif mtype == "delete":
                await self._mesh.delete(key)
            # Unknown types: ignore (forward-compat with future protocol extensions).
        except (PayloadTooLargeError, ValueError) as e:
            logger.info("[gateway] dropping mutation with bad value: %s", e)
        except Exception:  # noqa: BLE001
            logger.exception("[gateway] mesh write failed for %r", key)

    async def _relay_presence(
        self, sender_ws: Any, msg: dict[str, Any],
    ) -> None:
        meta = self._client_index.get(sender_ws)
        if not meta:
            return
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
    print("Aether-Core :: Client Gateway (hardened) :: self-test")
    print("=" * 72)

    workdir = Path(tempfile.mkdtemp(prefix="aether-gateway-"))
    ledger_path = workdir / "ledger_gateway_demo.jsonl"

    ledger = ChronoLedger(ledger_path)
    placeholder_gateway: dict[str, ClientGateway] = {}

    async def composed_on_op(op: Operation[Any, Any], src: Optional[str]) -> None:
        await ledger.on_op(op, src)
        gw = placeholder_gateway.get("g")
        if gw is not None:
            await gw.on_op(op, src)

    mesh = MeshNode("alpha", port=8201, on_op=composed_on_op)
    gateway = ClientGateway(mesh, host="127.0.0.1", port=8211)
    placeholder_gateway["g"] = gateway

    print(f"\n[stack] ledger={ledger_path.name}")
    print(f"        mesh   ws://127.0.0.1:8201")
    print(f"        gateway {gateway.url}")
    print(f"        limits  rate={gateway.limits.messages_per_second:.0f}/s "
          f"burst={gateway.limits.messages_burst} "
          f"max_conn={gateway.limits.max_connections_total}")

    await ledger.boot(mesh)
    await mesh.start()
    await gateway.start()

    await mesh.set("preexisting", "I was here first")
    await ledger.flush()

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

    async def wait_for(inbox, predicate, timeout=2.0):
        loop = asyncio.get_event_loop()
        deadline = loop.time() + timeout
        while loop.time() < deadline:
            try:
                msg = await asyncio.wait_for(inbox.get(), timeout=deadline - loop.time())
            except asyncio.TimeoutError:
                break
            if predicate(msg):
                return msg
        raise AssertionError("predicate did not match within timeout")

    print("\n[phase 1] two clients connect, both receive snapshot")
    print("-" * 72)
    ws_a, inbox_a = await open_browser_client("A")
    ws_b, inbox_b = await open_browser_client("B")
    snap_a = await wait_for(inbox_a, lambda m: m.get("type") == "snapshot")
    snap_b = await wait_for(inbox_b, lambda m: m.get("type") == "snapshot")
    assert snap_a["data"] == snap_b["data"] == {"preexisting": "I was here first"}
    print(f"  both snapshots = {snap_a['data']}")

    print("\n[phase 2] bidirectional sync (A writes -> B observes; B writes -> A observes)")
    print("-" * 72)
    await ws_a.send(json.dumps({"type": "set", "key": "shared:msg", "value": "hello from A"}))
    msg = await wait_for(inbox_b, lambda m: m.get("type") == "set" and m.get("key") == "shared:msg")
    assert msg["value"] == "hello from A"
    await ws_b.send(json.dumps({"type": "set", "key": "counter", "value": 42}))
    msg = await wait_for(inbox_a, lambda m: m.get("type") == "set" and m.get("key") == "counter")
    assert msg["value"] == 42
    print("  bidirectional sync OK")

    print("\n[phase 3] late-joiner gets the full current snapshot")
    print("-" * 72)
    ws_c, inbox_c = await open_browser_client("C")
    snap_c = await wait_for(inbox_c, lambda m: m.get("type") == "snapshot")
    assert snap_c["data"]["counter"] == 42
    print(f"  late joiner C snapshot includes 'counter'={snap_c['data']['counter']}")

    print("\n[phase 4] delete propagates as 'delete' message")
    print("-" * 72)
    await ws_a.send(json.dumps({"type": "delete", "key": "shared:msg"}))
    msg = await wait_for(inbox_c, lambda m: m.get("type") == "delete" and m.get("key") == "shared:msg")
    print(f"  delete propagated to late joiner C")

    print("\n[phase 5] malformed input survival")
    print("-" * 72)
    await ws_a.send("this is not json at all")
    await ws_a.send(json.dumps({"type": "set"}))
    await ws_a.send(json.dumps({"type": "set", "key": "", "value": 1}))
    await ws_a.send(json.dumps({"type": "set", "key": "post:malformed", "value": "ok"}))
    msg = await wait_for(inbox_b, lambda m: m.get("type") == "set" and m.get("key") == "post:malformed")
    assert msg["value"] == "ok"
    print("  gateway survived malformed inputs and still delivered subsequent set")

    print("\n[phase 6] oversize payload rejected, connection stays alive")
    print("-" * 72)
    # Send a payload that exceeds max_value_bytes (32 KiB default).
    huge = "x" * (gateway.limits.max_value_bytes + 100)
    await ws_a.send(json.dumps({"type": "set", "key": "should:be:rejected", "value": huge}))
    # Follow up with a normal write that should land.
    await ws_a.send(json.dumps({"type": "set", "key": "after:oversize", "value": "ok"}))
    msg = await wait_for(inbox_b, lambda m: m.get("type") == "set" and m.get("key") == "after:oversize")
    assert msg["value"] == "ok"
    # The huge one must NOT be present in any subsequent snapshot.
    ws_d, inbox_d = await open_browser_client("D")
    snap_d = await wait_for(inbox_d, lambda m: m.get("type") == "snapshot")
    assert "should:be:rejected" not in snap_d["data"]
    print("  oversize value rejected, gateway still serving")

    print("\n[phase 7] persistence: boot a fresh instance and verify the ledger")
    print("-" * 72)
    canonical_fp = mesh.node.store.state_fingerprint()
    await ws_a.close(); await ws_b.close(); await ws_c.close(); await ws_d.close()
    await gateway.stop(); await mesh.stop(); await ledger.close()

    ledger2 = ChronoLedger(ledger_path)
    mesh2 = MeshNode("alpha", port=8202, on_op=ledger2.on_op)
    replayed = await ledger2.boot(mesh2)
    fresh_fp = mesh2.node.store.state_fingerprint()
    assert fresh_fp == canonical_fp
    print(f"  replayed {replayed} ops, fingerprint matches pre-shutdown: True")
    await ledger2.close()

    shutil.rmtree(workdir, ignore_errors=True)

    print("\n" + "=" * 72)
    print("CLIENT GATEWAY (hardened): PROVEN  "
          "(snapshot + sync + malformed + oversize + persistence)")
    print("=" * 72)


if __name__ == "__main__":
    asyncio.run(_demo())
