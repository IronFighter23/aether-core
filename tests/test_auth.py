"""
Tests for shared-secret authentication on both the client gateway
and the mesh pubsub. The threat model is:

* A peer or browser that does NOT present the configured token must be
  rejected before it can read or write any state.
* A peer or browser that presents a WRONG token must be rejected with
  the same "auth failed" close code.
* The pre-auth code path must not leak state (no snapshot, no hello)
  to the rejected client.
* With ``AuthConfig()`` (no token) the legacy open-relay behaviour is
  preserved end-to-end.
* Token comparison is constant-time.
"""
from __future__ import annotations

import asyncio
import json
import time

import pytest
from websockets import connect
from websockets.exceptions import ConnectionClosed, InvalidStatus

from aether_core import (
    AuthConfig,
    ClientGateway,
    MeshNode,
    secure_compare,
)


# ---------------------------------------------------------------------------
# Unit: secure_compare semantics
# ---------------------------------------------------------------------------

def test_secure_compare_equal():
    assert secure_compare("hunter2", "hunter2") is True


def test_secure_compare_different():
    assert secure_compare("hunter2", "wrong-secret") is False


def test_secure_compare_different_length():
    # Different lengths must still return False (not raise) and not
    # leak length info via timing -- compare_digest handles that.
    assert secure_compare("short", "a-much-longer-token") is False


def test_secure_compare_none_inputs():
    # None is never equal to anything, including another None. This
    # is intentional: "the server has no token configured" must not
    # authenticate a peer that also presents nothing.
    assert secure_compare(None, None) is False
    assert secure_compare(None, "x") is False
    assert secure_compare("x", None) is False


def test_authconfig_required_flag():
    assert AuthConfig().required is False
    assert AuthConfig(token=None).required is False
    assert AuthConfig(token="").required is False        # empty string == no auth
    assert AuthConfig(token="x").required is True


def test_authconfig_verify_closed_by_default():
    # An AuthConfig with no token must REJECT every credential.
    # Callers should check ``.required`` first; .verify() on an unset
    # config refusing everything is a fail-closed safety property.
    cfg = AuthConfig()
    assert cfg.verify(None) is False
    assert cfg.verify("anything") is False


def test_authconfig_verify_constant_time_correctness():
    cfg = AuthConfig(token="sekret")
    assert cfg.verify("sekret") is True
    assert cfg.verify("Sekret") is False     # case-sensitive
    assert cfg.verify("sekre") is False      # prefix
    assert cfg.verify("sekrett") is False    # suffix
    assert cfg.verify("") is False
    assert cfg.verify(None) is False


# ---------------------------------------------------------------------------
# Integration: ClientGateway auth — URL-query mode
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_gateway_no_auth_required_lets_anyone_in():
    """Backwards compat: no AuthConfig => no auth required => clients connect freely."""
    mesh = MeshNode("alpha", port=0)
    gateway = ClientGateway(mesh, host="127.0.0.1", port=0)   # no auth=
    await mesh.start()
    await gateway.start()
    try:
        async with connect(gateway.url) as ws:
            hello = json.loads(await ws.recv())
            assert hello["type"] == "hello"
            snap = json.loads(await ws.recv())
            assert snap["type"] == "snapshot"
            assert snap["data"] == {}
    finally:
        await gateway.stop()
        await mesh.stop()


@pytest.mark.asyncio
async def test_gateway_auth_via_url_query_succeeds():
    mesh = MeshNode("alpha", port=0)
    gateway = ClientGateway(
        mesh, host="127.0.0.1", port=0,
        auth=AuthConfig(token="hunter2"),
    )
    await mesh.start()
    await gateway.start()
    try:
        async with connect(gateway.url + "/?auth_token=hunter2") as ws:
            hello = json.loads(await ws.recv())
            snap  = json.loads(await ws.recv())
            assert hello["type"] == "hello"
            assert snap["type"]  == "snapshot"
    finally:
        await gateway.stop()
        await mesh.stop()


@pytest.mark.asyncio
async def test_gateway_auth_via_url_query_wrong_token_closes():
    mesh = MeshNode("alpha", port=0)
    gateway = ClientGateway(
        mesh, host="127.0.0.1", port=0,
        auth=AuthConfig(token="hunter2"),
    )
    await mesh.start()
    await gateway.start()
    try:
        ws = await connect(gateway.url + "/?auth_token=WRONG")
        # We must get closed without ever seeing 'hello' or 'snapshot'.
        # (The gateway rejects BEFORE it sends anything, so .recv()
        # blocks on a closed socket.)
        with pytest.raises(ConnectionClosed):
            await asyncio.wait_for(ws.recv(), timeout=2.0)
    finally:
        await gateway.stop()
        await mesh.stop()


@pytest.mark.asyncio
async def test_gateway_auth_via_first_message_succeeds():
    mesh = MeshNode("alpha", port=0)
    gateway = ClientGateway(
        mesh, host="127.0.0.1", port=0,
        auth=AuthConfig(token="hunter2"),
    )
    await mesh.start()
    await gateway.start()
    try:
        async with connect(gateway.url) as ws:
            await ws.send(json.dumps({"type": "auth", "token": "hunter2"}))
            hello = json.loads(await ws.recv())
            snap  = json.loads(await ws.recv())
            assert hello["type"] == "hello"
            assert snap["type"]  == "snapshot"
    finally:
        await gateway.stop()
        await mesh.stop()


@pytest.mark.asyncio
async def test_gateway_auth_via_first_message_wrong_token_closes():
    mesh = MeshNode("alpha", port=0)
    gateway = ClientGateway(
        mesh, host="127.0.0.1", port=0,
        auth=AuthConfig(token="hunter2"),
    )
    await mesh.start()
    await gateway.start()
    try:
        ws = await connect(gateway.url)
        await ws.send(json.dumps({"type": "auth", "token": "WRONG"}))
        with pytest.raises(ConnectionClosed):
            await asyncio.wait_for(ws.recv(), timeout=2.0)
    finally:
        await gateway.stop()
        await mesh.stop()


@pytest.mark.asyncio
async def test_gateway_auth_does_not_leak_snapshot_to_rejected_client():
    """A rejected client must not see the snapshot, ever."""
    mesh = MeshNode("alpha", port=0)
    await mesh.start()
    # Seed some state that an attacker would want to read.
    await mesh.set("secret:api:key", "DO-NOT-LEAK-ABCDEF")
    gateway = ClientGateway(
        mesh, host="127.0.0.1", port=0,
        auth=AuthConfig(token="hunter2"),
    )
    await gateway.start()
    try:
        # Wrong token via URL.
        ws = await connect(gateway.url + "/?auth_token=ATTACKER")
        with pytest.raises(ConnectionClosed):
            msg = await asyncio.wait_for(ws.recv(), timeout=2.0)
            # If we ever GET here, the snapshot leaked. Fail loud.
            assert "DO-NOT-LEAK" not in str(msg)
    finally:
        await gateway.stop()
        await mesh.stop()


# ---------------------------------------------------------------------------
# Integration: mesh-to-mesh auth (peer federation)
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_mesh_auth_matching_tokens_federate():
    auth = AuthConfig(token="cluster-shared-secret")
    a = MeshNode("alpha", port=0, auth=auth)
    b = MeshNode("beta",  port=0, auth=auth)
    await a.start()
    await b.start()
    try:
        await a.connect_to("127.0.0.1", b.port)
        # Wait a moment for the bidirectional hello to settle.
        await asyncio.sleep(0.1)
        assert "beta"  in a.peer_ids
        assert "alpha" in b.peer_ids

        # A write on alpha must propagate to beta.
        await a.set("hello", "from alpha")
        deadline = time.monotonic() + 1.0
        while time.monotonic() < deadline and b.get("hello") != "from alpha":
            await asyncio.sleep(0.02)
        assert b.get("hello") == "from alpha"
    finally:
        await a.stop()
        await b.stop()


@pytest.mark.asyncio
async def test_mesh_auth_mismatched_tokens_refuse_to_federate():
    a = MeshNode("alpha", port=0, auth=AuthConfig(token="alpha-secret"))
    b = MeshNode("beta",  port=0, auth=AuthConfig(token="beta-secret"))
    await a.start()
    await b.start()
    try:
        # connect_to should raise on auth mismatch (we re-raise as
        # AuthError from the outbound side).
        with pytest.raises(Exception):
            await a.connect_to("127.0.0.1", b.port)
        # And neither side should have peered.
        await asyncio.sleep(0.1)
        assert a.peer_ids == set()
        assert b.peer_ids == set()

        # And a write on a should NOT reach b.
        await a.set("hello", "should-not-arrive")
        await asyncio.sleep(0.2)
        assert b.get("hello") is None
    finally:
        await a.stop()
        await b.stop()


@pytest.mark.asyncio
async def test_mesh_auth_one_side_disabled_one_required_refuses():
    # Asymmetric config -- this should fail closed. A peer that
    # doesn't think auth is required will try to hello WITHOUT a
    # token; the peer that requires one will reject.
    a = MeshNode("alpha", port=0, auth=AuthConfig())   # no auth
    b = MeshNode("beta",  port=0, auth=AuthConfig(token="cluster-secret"))
    await a.start()
    await b.start()
    try:
        with pytest.raises(Exception):
            await a.connect_to("127.0.0.1", b.port)
        await asyncio.sleep(0.1)
        assert "alpha" not in b.peer_ids
    finally:
        await a.stop()
        await b.stop()
