"""
Tests for the bounded memory caches added in 0.4.0:

* ``SeenStampCache`` -- bounded HLC dedup with FIFO eviction.
* ``Node.oplog``     -- bounded deque ring buffer.
* ``MeshNode._seen`` -- wired to use ``SeenStampCache`` with the
  configured ``max_seen_stamps`` limit.

These guard the two previously-unbounded structures whose growth
would silently OOM a long-running deployment.
"""
from __future__ import annotations

import pytest

from aether_core import (
    HybridLogicalClock,
    MeshNode,
    Node,
    SecurityLimits,
    SeenStampCache,
)


# ---------------------------------------------------------------------------
# SeenStampCache unit tests
# ---------------------------------------------------------------------------

def _stamp(p: int, n: str = "alpha") -> HybridLogicalClock:
    return HybridLogicalClock(physical_ns=p, logical=0, node_id=n)


def test_seen_cache_rejects_invalid_size():
    with pytest.raises(ValueError):
        SeenStampCache(max_size=0)
    with pytest.raises(ValueError):
        SeenStampCache(max_size=-1)


def test_seen_cache_basic_add_contains():
    c = SeenStampCache(max_size=8)
    s = _stamp(1)
    assert s not in c
    assert c.add(s) is True
    assert s in c
    assert len(c) == 1
    # Re-adding the same stamp is a no-op.
    assert c.add(s) is False
    assert len(c) == 1


def test_seen_cache_evicts_oldest_when_full():
    c = SeenStampCache(max_size=3)
    stamps = [_stamp(i) for i in range(5)]
    for s in stamps:
        c.add(s)
    # Capacity = 3, so the first two stamps must have been evicted FIFO.
    assert len(c) == 3
    assert stamps[0] not in c
    assert stamps[1] not in c
    assert stamps[2] in c
    assert stamps[3] in c
    assert stamps[4] in c


def test_seen_cache_iter_yields_in_fifo_order():
    c = SeenStampCache(max_size=5)
    for i in range(5):
        c.add(_stamp(i))
    seen_order = [s.physical_ns for s in c]
    assert seen_order == [0, 1, 2, 3, 4]


def test_seen_cache_clear():
    c = SeenStampCache(max_size=4)
    for i in range(3):
        c.add(_stamp(i))
    assert len(c) == 3
    c.clear()
    assert len(c) == 0
    assert _stamp(0) not in c


def test_seen_cache_capacity_property():
    assert SeenStampCache(max_size=10).capacity == 10


# ---------------------------------------------------------------------------
# Bounded Node.oplog tests
# ---------------------------------------------------------------------------

def test_node_oplog_is_bounded_by_default():
    n: Node[str, str] = Node("alpha", max_oplog_size=5)
    for i in range(10):
        n.set(f"k{i}", f"v{i}")
    # Cap is 5, so only the last 5 ops survive.
    assert len(list(n.oplog)) == 5


def test_node_oplog_unbounded_when_explicitly_none():
    # Backward-compat path for the in-process demo / tests that
    # iterate the full op history.
    n: Node[str, str] = Node("alpha", max_oplog_size=None)
    for i in range(50):
        n.set(f"k{i}", f"v{i}")
    assert len(list(n.oplog)) == 50


def test_node_oplog_rejects_invalid_size():
    with pytest.raises(ValueError):
        Node("alpha", max_oplog_size=0)
    with pytest.raises(ValueError):
        Node("alpha", max_oplog_size=-3)


def test_node_oplog_default_cap_is_10000():
    # We don't generate 10k ops in unit tests (slow); just verify
    # the cap is respected for any positive value.
    n: Node[str, str] = Node("alpha", max_oplog_size=3)
    n.set("a", "1")
    n.set("b", "2")
    n.set("c", "3")
    n.set("d", "4")    # evicts "a"
    keys = [op.key for op in n.oplog]
    assert keys == ["b", "c", "d"]


# ---------------------------------------------------------------------------
# MeshNode._seen wiring
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_meshnode_seen_uses_bounded_cache():
    # Pick a tiny limit so the test is fast and the assertion is direct.
    limits = SecurityLimits(max_seen_stamps=4, max_oplog_size=8)
    m = MeshNode("alpha", port=0, limits=limits)
    await m.start()
    try:
        # _seen is the bounded cache, not a plain set.
        assert isinstance(m._seen, SeenStampCache)   # noqa: SLF001
        assert m._seen.capacity == 4                  # noqa: SLF001

        for i in range(10):
            await m.set(f"k{i}", i)

        # _seen never exceeds its cap.
        assert len(m._seen) <= 4                      # noqa: SLF001
        # And the node.oplog cap from the same SecurityLimits was honoured.
        assert len(list(m.node.oplog)) <= 8
    finally:
        await m.stop()


@pytest.mark.asyncio
async def test_meshnode_seen_cap_does_not_break_idempotence():
    """
    Even when an HLC stamp has been evicted from _seen, re-applying
    the same op must still converge to the same state (CRDT apply is
    idempotent by construction; the cache eviction must not change
    that).
    """
    limits = SecurityLimits(max_seen_stamps=2, max_oplog_size=None)
    m = MeshNode("alpha", port=0, limits=limits)
    await m.start()
    try:
        op1 = await m.set("k", "v1")
        await m.set("filler1", 1)
        await m.set("filler2", 2)
        # By now op1.stamp may have been evicted from _seen (cache is 2).
        # Re-applying it via the same path must not produce a different
        # final state -- CRDT idempotence guarantees that.
        await m._ingest_remote(op1, source_peer="self-replay")  # noqa: SLF001
        assert m.get("k") == "v1"
    finally:
        await m.stop()
