"""
Aether-Core :: Variable Entanglement Layer
==========================================

A state-based CRDT engine. State is mutated locally and reconciled across
nodes via a commutative, associative, idempotent merge operation. Conflict
resolution is deterministic by construction; there is no quorum, no leader,
no centralized arbiter.

Mathematical guarantees
-----------------------
For any LWW value X with stamp S(X) drawn from a totally-ordered HLC space:

    merge(a, b) = merge(b, a)               (commutativity)
    merge(merge(a, b), c) = merge(a, merge(b, c))   (associativity)
    merge(a, a) = a                         (idempotence)

Because the Hybrid Logical Clock yields a *total order* over events
(physical_ns, logical_counter, node_id), no two distinct writes can ever
be "concurrent" from the merge function's point of view. Convergence is
therefore guaranteed for any delivery order of operations.
"""
from __future__ import annotations

import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Generic, Iterator, Optional, TypeVar

__all__ = [
    "HybridLogicalClock",
    "HLCGenerator",
    "LWWRegister",
    "LWWMap",
    "Operation",
    "OpKind",
    "Node",
]

T = TypeVar("T")
K = TypeVar("K")
V = TypeVar("V")

NodeId = str


# ---------------------------------------------------------------------------
# Temporal Vector :: Hybrid Logical Clock
# ---------------------------------------------------------------------------

@dataclass(frozen=True, slots=True)
class HybridLogicalClock:
    """
    A Hybrid Logical Clock stamp.

    The triple ``(physical_ns, logical, node_id)`` forms a strict total
    order. ``physical_ns`` tracks wall-clock time so stamps stay close to
    real time even across nodes; ``logical`` disambiguates events within
    the same nanosecond on the same node; ``node_id`` is the final
    tiebreaker for genuinely concurrent events on different nodes.

    The stamp is the temporal vector of a write: it records *when* in the
    causal lattice the intent occurred, independent of network delivery.
    """
    physical_ns: int
    logical: int
    node_id: NodeId

    def __lt__(self, other: "HybridLogicalClock") -> bool:
        return (self.physical_ns, self.logical, self.node_id) < (
            other.physical_ns,
            other.logical,
            other.node_id,
        )

    def __le__(self, other: "HybridLogicalClock") -> bool:
        return (self.physical_ns, self.logical, self.node_id) <= (
            other.physical_ns,
            other.logical,
            other.node_id,
        )

    def __gt__(self, other: "HybridLogicalClock") -> bool:
        return other.__lt__(self)

    def __ge__(self, other: "HybridLogicalClock") -> bool:
        return other.__le__(self)

    @staticmethod
    def zero(node_id: NodeId) -> "HybridLogicalClock":
        return HybridLogicalClock(0, 0, node_id)

    def encode(self) -> str:
        """Wire-format encoding (used later by the Holographic Plane)."""
        return f"{self.physical_ns:020d}.{self.logical:010d}.{self.node_id}"


class HLCGenerator:
    """
    Stateful issuer of HLC stamps for a single node.

    - ``tick()`` is the *send* path: produce a fresh stamp for a local event.
    - ``observe()`` is the *receive* path: advance the local clock to
      strictly dominate a remote stamp before issuing the next local one.

    Both operations preserve the monotonicity invariant: every stamp
    issued by this generator is strictly greater than every prior stamp
    it has issued or observed.
    """

    __slots__ = ("_node_id", "_last")

    def __init__(self, node_id: NodeId) -> None:
        self._node_id = node_id
        self._last: HybridLogicalClock = HybridLogicalClock.zero(node_id)

    @property
    def node_id(self) -> NodeId:
        return self._node_id

    @property
    def last(self) -> HybridLogicalClock:
        return self._last

    def tick(self) -> HybridLogicalClock:
        now_ns = time.time_ns()
        if now_ns > self._last.physical_ns:
            self._last = HybridLogicalClock(now_ns, 0, self._node_id)
        else:
            # Wall clock didn't advance (same nanosecond or backwards skew):
            # bump the logical counter to preserve monotonicity.
            self._last = HybridLogicalClock(
                self._last.physical_ns,
                self._last.logical + 1,
                self._node_id,
            )
        return self._last

    def observe(self, remote: HybridLogicalClock) -> HybridLogicalClock:
        """Advance local clock past ``remote`` and return the new stamp."""
        now_ns = time.time_ns()
        local = self._last
        max_phys = max(now_ns, local.physical_ns, remote.physical_ns)

        if max_phys == local.physical_ns and max_phys == remote.physical_ns:
            logical = max(local.logical, remote.logical) + 1
        elif max_phys == local.physical_ns:
            logical = local.logical + 1
        elif max_phys == remote.physical_ns:
            logical = remote.logical + 1
        else:
            logical = 0

        self._last = HybridLogicalClock(max_phys, logical, self._node_id)
        return self._last


# ---------------------------------------------------------------------------
# CRDT primitive :: Last-Writer-Wins Register
# ---------------------------------------------------------------------------

@dataclass(frozen=True, slots=True)
class LWWRegister(Generic[T]):
    """
    Last-Writer-Wins Register.

    Holds a single value plus the HLC stamp at which it was last written.
    A tombstone flag distinguishes "absent" from "explicitly deleted",
    which matters because a late-arriving write with a *lower* HLC must
    not be allowed to resurrect a key that a later writer has removed.

    The register is immutable; ``set`` and ``delete`` return new instances.
    """

    value: Optional[T]
    stamp: HybridLogicalClock
    tombstone: bool = False

    def merge(self, other: "LWWRegister[T]") -> "LWWRegister[T]":
        # Total order on stamps => deterministic winner, always.
        return other if other.stamp > self.stamp else self

    def set(self, value: T, stamp: HybridLogicalClock) -> "LWWRegister[T]":
        if stamp > self.stamp:
            return LWWRegister(value=value, stamp=stamp, tombstone=False)
        return self

    def delete(self, stamp: HybridLogicalClock) -> "LWWRegister[T]":
        if stamp > self.stamp:
            return LWWRegister(value=None, stamp=stamp, tombstone=True)
        return self

    @property
    def is_present(self) -> bool:
        return not self.tombstone


# ---------------------------------------------------------------------------
# Operation record :: the intent of a state change
# ---------------------------------------------------------------------------

class OpKind(Enum):
    SET = "set"
    DEL = "del"


@dataclass(frozen=True, slots=True)
class Operation(Generic[K, V]):
    """
    An immutable record of intent. Each mutation produces exactly one
    Operation. Operations are the atoms that the Chrono-Vector Storage
    layer (next milestone) will append to its temporal ledger, and that
    the Holographic Execution Plane will replicate across the mesh.

    For state-based CRDT merge we do not strictly need to retain
    operations, but exposing them here gives the upper layers a clean
    interface to subscribe to.
    """
    kind: OpKind
    key: K
    value: Optional[V]
    stamp: HybridLogicalClock


# ---------------------------------------------------------------------------
# CRDT primitive :: Last-Writer-Wins Map
# ---------------------------------------------------------------------------

@dataclass(slots=True)
class LWWMap(Generic[K, V]):
    """
    A keyed collection of LWWRegisters.

    Deletions are tombstoned, not removed. This is essential: if node A
    deletes key ``k`` at stamp S_a and node B writes ``k`` at stamp S_b
    with S_b < S_a but the delete arrives at B *after* its own write,
    the tombstone with stamp S_a still dominates and ``k`` stays gone.
    Without tombstones, the late-arriving delete would silently no-op.
    """

    _entries: dict[K, LWWRegister[V]] = field(default_factory=dict)

    # -- mutators -----------------------------------------------------------

    def set(self, key: K, value: V, stamp: HybridLogicalClock) -> Operation[K, V]:
        current = self._entries.get(key)
        if current is None:
            self._entries[key] = LWWRegister(value=value, stamp=stamp)
        else:
            self._entries[key] = current.set(value, stamp)
        return Operation(OpKind.SET, key, value, stamp)

    def delete(self, key: K, stamp: HybridLogicalClock) -> Operation[K, V]:
        current = self._entries.get(key)
        if current is None:
            self._entries[key] = LWWRegister(value=None, stamp=stamp, tombstone=True)
        else:
            self._entries[key] = current.delete(stamp)
        return Operation(OpKind.DEL, key, None, stamp)

    def apply(self, op: Operation[K, V]) -> None:
        """Apply a remote operation. Idempotent and order-independent."""
        if op.kind is OpKind.SET:
            # op.value is guaranteed non-None for SET ops by construction.
            assert op.value is not None
            self.set(op.key, op.value, op.stamp)
        else:
            self.delete(op.key, op.stamp)

    def merge(self, other: "LWWMap[K, V]") -> None:
        """State-based merge: pairwise register merge across the key union."""
        for k, remote in other._entries.items():
            local = self._entries.get(k)
            self._entries[k] = remote if local is None else local.merge(remote)

    # -- readers ------------------------------------------------------------

    def get(self, key: K) -> Optional[V]:
        reg = self._entries.get(key)
        if reg is None or reg.tombstone:
            return None
        return reg.value

    def __contains__(self, key: object) -> bool:
        reg = self._entries.get(key)  # type: ignore[arg-type]
        return reg is not None and not reg.tombstone

    def __iter__(self) -> Iterator[K]:
        return (k for k, r in self._entries.items() if not r.tombstone)

    def __len__(self) -> int:
        return sum(1 for r in self._entries.values() if not r.tombstone)

    def items(self) -> Iterator[tuple[K, V]]:
        for k, r in self._entries.items():
            if not r.tombstone and r.value is not None:
                yield k, r.value

    def snapshot(self) -> dict[K, V]:
        """Materialized live view, tombstones excluded."""
        return dict(self.items())

    def state_fingerprint(self) -> tuple[tuple[K, HybridLogicalClock, bool, Optional[V]], ...]:
        """
        Canonical representation of the *entire* register state, including
        tombstones and stamps. Two replicas are convergent iff their
        fingerprints are equal. Used by the test harness below.
        """
        return tuple(
            sorted(
                (
                    (k, r.stamp, r.tombstone, r.value)
                    for k, r in self._entries.items()
                ),
                key=lambda t: str(t[0]),
            )
        )


# ---------------------------------------------------------------------------
# Node :: a local replica with its own HLC
# ---------------------------------------------------------------------------

class Node(Generic[K, V]):
    """
    A local replica. Owns its HLC generator and an LWWMap.

    In the full architecture this object is what the Holographic Plane
    will wrap with a WebSocket transport. For now it exposes a synchronous
    in-process API so we can prove the math without networking noise.
    """

    __slots__ = ("_id", "_clock", "_store", "_oplog")

    def __init__(self, node_id: NodeId) -> None:
        self._id = node_id
        self._clock = HLCGenerator(node_id)
        self._store: LWWMap[K, V] = LWWMap()
        self._oplog: list[Operation[K, V]] = []

    @property
    def id(self) -> NodeId:
        return self._id

    @property
    def store(self) -> LWWMap[K, V]:
        return self._store

    @property
    def oplog(self) -> list[Operation[K, V]]:
        # Exposed for the future Chrono-Vector Storage layer to consume.
        return self._oplog

    def set(self, key: K, value: V) -> Operation[K, V]:
        op = self._store.set(key, value, self._clock.tick())
        self._oplog.append(op)
        return op

    def delete(self, key: K) -> Operation[K, V]:
        op = self._store.delete(key, self._clock.tick())
        self._oplog.append(op)
        return op

    def get(self, key: K) -> Optional[V]:
        return self._store.get(key)

    def receive(self, op: Operation[K, V]) -> None:
        """Receive a remote operation. Advances local HLC past sender's stamp."""
        self._clock.observe(op.stamp)
        self._store.apply(op)
        self._oplog.append(op)

    def merge_from(self, other: "Node[K, V]") -> None:
        """State-based merge: pull all of ``other``'s state into self."""
        self._clock.observe(other._clock.last)
        self._store.merge(other._store)


# ---------------------------------------------------------------------------
# Self-test :: three nodes, concurrent writes, provable convergence
# ---------------------------------------------------------------------------

def _demo() -> None:
    import itertools
    import random

    print("=" * 72)
    print("Aether-Core :: Variable Entanglement Engine :: self-test")
    print("=" * 72)

    # Three independent replicas, no network, no coordinator.
    alpha: Node[str, str] = Node("alpha")
    beta: Node[str, str] = Node("beta")
    gamma: Node[str, str] = Node("gamma")
    nodes = [alpha, beta, gamma]

    print("\n[phase 1] each node writes the SAME key concurrently")
    print("-" * 72)
    # All three race to set the same key. Because HLC is a total order, exactly
    # one of them will win, deterministically, regardless of merge order.
    op_a = alpha.set("user:profile:name", "Aleph from Alpha")
    op_b = beta.set("user:profile:name", "Beta's Verdict")
    op_g = gamma.set("user:profile:name", "Gamma Says So")

    for op in (op_a, op_b, op_g):
        print(f"  {op.stamp.node_id:>5} @ {op.stamp.encode()}  "
              f"=>  {op.kind.value} {op.key!r} = {op.value!r}")

    print("\n[phase 2] each node ALSO writes some non-conflicting keys")
    print("-" * 72)
    alpha.set("user:profile:city", "Mumbai")
    alpha.set("session:token", "a-tok-001")
    beta.set("user:profile:role", "engineer")
    beta.set("session:token", "b-tok-002")        # conflicts with alpha's session:token
    gamma.set("user:profile:org", "BFSI Edge")
    gamma.delete("session:token")                  # tombstones over both above

    print("  local snapshots BEFORE merge:")
    for n in nodes:
        print(f"    {n.id:>5}: {n.store.snapshot()}")

    print("\n[phase 3] gossip every operation to every other node, in RANDOM order")
    print("-" * 72)
    # Collect every operation produced so far, then shuffle and deliver to all
    # peers. Random delivery order is the toughest test of CRDT convergence:
    # if the math is right, the order cannot matter.
    all_ops: list[tuple[str, Operation[str, str]]] = []
    for n in nodes:
        for op in n.oplog:
            all_ops.append((n.id, op))

    rng = random.Random(0xAE7AE7)  # deterministic shuffle so the demo is reproducible
    deliveries: dict[str, list[Operation[str, str]]] = {n.id: [] for n in nodes}
    for origin, op in all_ops:
        for n in nodes:
            if n.id != origin:
                deliveries[n.id].append(op)

    for n in nodes:
        rng.shuffle(deliveries[n.id])
        for op in deliveries[n.id]:
            n.receive(op)
        print(f"    {n.id:>5} consumed {len(deliveries[n.id])} remote ops "
              f"in shuffled order")

    print("\n[phase 4] post-merge snapshots")
    print("-" * 72)
    snapshots = [n.store.snapshot() for n in nodes]
    for n, snap in zip(nodes, snapshots):
        print(f"    {n.id:>5}: {snap}")

    print("\n[phase 5] convergence proof")
    print("-" * 72)
    # 1. Snapshots equal across all replicas.
    all_equal = all(s == snapshots[0] for s in snapshots[1:])
    print(f"    snapshot equality across replicas        : {all_equal}")

    # 2. Full state fingerprints (including tombstones + stamps) equal.
    fps = [n.store.state_fingerprint() for n in nodes]
    fp_equal = all(f == fps[0] for f in fps[1:])
    print(f"    full fingerprint equality (with tombstones): {fp_equal}")

    # 3. Commutativity: try every permutation of merging fresh replicas from
    #    the three oplogs and confirm the result is invariant.
    def replay(order: tuple[Node[str, str], ...]) -> tuple:
        fresh: Node[str, str] = Node("test")
        for src in order:
            for op in src.oplog:
                # Skip ops the source merely received; re-applying its own
                # full oplog (sets + deletes + receives) is equivalent but
                # we only want originating ops to avoid double-counting.
                if op.stamp.node_id == src.id:
                    fresh.receive(op)
        return fresh.store.state_fingerprint()

    perm_results = {replay(p) for p in itertools.permutations(nodes)}
    print(f"    distinct results across all 6 merge orders: {len(perm_results)} "
          f"(must be 1)")

    # 4. Winning value of the contested key is the writer with the max HLC.
    contested_stamps = {op.stamp: op for op in (op_a, op_b, op_g)}
    expected_winner = max(contested_stamps.keys())
    winning_op = contested_stamps[expected_winner]
    converged_value = snapshots[0]["user:profile:name"]
    print(f"    contested key winner (by max HLC)        : "
          f"{winning_op.stamp.node_id} -> {winning_op.value!r}")
    print(f"    converged value across replicas          : {converged_value!r}")
    winner_correct = converged_value == winning_op.value

    print("\n" + "=" * 72)
    verdict = all_equal and fp_equal and len(perm_results) == 1 and winner_correct
    print(f"CONVERGENCE: {'PROVEN' if verdict else 'FAILED'}")
    print("=" * 72)

    if not verdict:
        raise SystemExit(1)


if __name__ == "__main__":
    _demo()
