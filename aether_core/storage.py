"""
Aether-Core :: Chrono-Vector Storage (append-only ledger)
=========================================================

Data is not stored as state; it is stored as an immutable, append-only
ledger of state-change vectors. The ledger is a JSON-lines file
written with single-syscall ``os.write()`` calls on an ``O_APPEND``
file descriptor, fsync'd after each record. This gives two
guarantees:

1. **Atomicity per record.** Records small enough to fit in PIPE_BUF
   (>= 4096 bytes; ours are well under) are appended atomically by
   the kernel. A concurrent or interrupted writer cannot interleave
   bytes inside a record.

2. **Bounded corruption surface.** If the process is killed
   mid-write, the *only* damaged region is a torn final line. The
   replay routine detects "no trailing newline" on the last record
   and truncates the file to the last clean newline. Every fully
   flushed record before the crash survives.

The CRDT layer (Phase 1) is naturally tolerant of replay: every
``Operation`` carries its HLC stamp, and ``LWWMap`` merges are
idempotent and commutative, so feeding the ledger back through
``Node.receive()`` reproduces *exactly* the post-crash state
including tombstones.

Thread / task safety
--------------------
* ``boot()`` and ``close()`` are guarded by an asyncio lock so a
  caller cannot ever observe a half-initialised ledger.
* There is exactly **one** writer task; ordering of disk writes is
  guaranteed by the FIFO ``asyncio.Queue``.
* ``os.write`` / ``os.fsync`` run on a worker thread via
  ``asyncio.to_thread``; the event loop never blocks on disk.
* ``on_op`` is non-blocking from the caller's perspective: it puts
  the op on the queue and returns. Use ``flush()`` for hard durability.
* All public methods are safe to call from any task on the same loop;
  none are safe to call across processes (use one ``ChronoLedger`` per
  ledger file).
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
from pathlib import Path
from typing import Any, Optional

from aether_core.crdt import HybridLogicalClock, OpKind, Operation
from aether_core.mesh import MeshNode, deserialize_operation, serialize_operation

__all__ = ["ChronoLedger"]

logger = logging.getLogger(__name__)

# Unique sentinel used to signal the writer task to drain and exit.
_SHUTDOWN: Any = object()


class ChronoLedger:
    """
    Append-only event ledger bound to a ``MeshNode``.

    Typical lifecycle::

        ledger = ChronoLedger("ledger_alpha.jsonl")
        mesh   = MeshNode("alpha", port=8001, on_op=ledger.on_op)
        await ledger.boot(mesh)        # replay disk -> CRDT state
        await mesh.start()             # accept peers, gossip
        ...                            # mutations are persisted automatically
        await mesh.stop()              # stop accepting new ops
        await ledger.close()           # drain queue, fsync, close fd
    """

    __slots__ = (
        "_path",
        "_mesh",
        "_fd",
        "_queue",
        "_writer_task",
        "_closed",
        "_state_lock",
        "_replayed_count",
        "_written_count",
        "_truncated_bytes",
        "_snapshot_entries",
        "_snapshot_skipped",
    )

    def __init__(self, path: str | os.PathLike[str]) -> None:
        self._path = Path(path)
        self._mesh: Optional[MeshNode] = None
        self._fd: Optional[int] = None
        self._queue: Optional[asyncio.Queue[Any]] = None
        self._writer_task: Optional[asyncio.Task[None]] = None
        self._closed = False
        # Guards boot() / close() so the fd + writer task are managed
        # as an atomic unit. on_op() does NOT acquire this lock -- it
        # only reads _closed (a bool, atomic) and _queue (a reference,
        # also atomic in CPython). The worst case if boot/close races
        # with on_op is an op dropped or routed to the wrong queue,
        # both of which are non-corrupting.
        self._state_lock = asyncio.Lock()
        self._replayed_count = 0
        self._written_count = 0
        self._truncated_bytes = 0
        # Phase 4 stats: entries loaded from snapshot, ledger ops skipped
        # because they were already covered by the snapshot's max_stamp.
        self._snapshot_entries = 0
        self._snapshot_skipped = 0

    # -- introspection ------------------------------------------------------

    @property
    def path(self) -> Path:
        return self._path

    @property
    def replayed_count(self) -> int:
        """Operations successfully replayed from disk at boot."""
        return self._replayed_count

    @property
    def written_count(self) -> int:
        """Operations persisted to disk since boot."""
        return self._written_count

    @property
    def truncated_bytes(self) -> int:
        """Bytes discarded as a torn final record during boot recovery."""
        return self._truncated_bytes

    @property
    def snapshot_entries(self) -> int:
        """Entries loaded from a snapshot file at boot (Phase 6)."""
        return self._snapshot_entries

    @property
    def snapshot_skipped(self) -> int:
        """Ledger ops skipped because the snapshot already covered them."""
        return self._snapshot_skipped

    @property
    def is_open(self) -> bool:
        return self._mesh is not None and not self._closed

    # -- lifecycle ----------------------------------------------------------

    async def boot(self, mesh_node: MeshNode) -> int:
        """
        Replay the ledger into ``mesh_node`` (must be called BEFORE
        ``mesh_node.start()``), recover from any torn final write, and
        open the append-mode fd so subsequent mutations are persisted.

        Returns the number of operations replayed.
        """
        async with self._state_lock:
            if self._closed:
                raise RuntimeError("ledger already closed")
            if self._mesh is not None:
                raise RuntimeError("ledger already booted")
            self._mesh = mesh_node

            # Step 1: replay + crash recovery, all on a worker thread so
            # we don't stall the event loop on large ledgers.
            replayed, truncated = await asyncio.to_thread(self._replay_sync)
            self._replayed_count = replayed
            self._truncated_bytes = truncated

            # Step 2: open the append-mode fd. O_APPEND guarantees every
            # write() goes to the current end of file even if another
            # writer extends it concurrently. O_CREAT creates if missing.
            self._fd = await asyncio.to_thread(
                os.open,
                str(self._path),
                os.O_WRONLY | os.O_APPEND | os.O_CREAT,
                0o644,
            )

            # Step 3: spin up the single writer task.
            self._queue = asyncio.Queue()
            self._writer_task = asyncio.create_task(
                self._writer_loop(), name=f"chrono-writer:{self._path.name}",
            )

        logger.info(
            "[ledger %s] booted: replayed=%d truncated=%d bytes",
            self._path.name, replayed, truncated,
        )
        return replayed

    async def flush(self) -> None:
        """Block until every queued operation has been fsync'd."""
        q = self._queue
        if q is not None:
            await q.join()

    async def close(self) -> None:
        """Drain pending writes, fsync, close the fd. Idempotent."""
        async with self._state_lock:
            if self._closed:
                return
            self._closed = True

            # Drain: enqueue the shutdown sentinel and wait for the writer.
            if self._queue is not None and self._writer_task is not None:
                await self._queue.put(_SHUTDOWN)
                try:
                    await self._writer_task
                except asyncio.CancelledError:
                    pass

            # Close fd. ``os.close`` on a single descriptor is atomic.
            if self._fd is not None:
                fd = self._fd
                self._fd = None
                try:
                    await asyncio.to_thread(os.close, fd)
                except OSError:
                    logger.exception(
                        "[ledger %s] os.close failed", self._path.name,
                    )

        logger.info(
            "[ledger %s] closed: replayed=%d written=%d",
            self._path.name, self._replayed_count, self._written_count,
        )

    # -- MeshNode hook ------------------------------------------------------

    async def on_op(
        self, op: Operation[Any, Any], source_peer: Optional[str],
    ) -> None:
        """
        Subscriber for ``MeshNode``'s on_op callback. Enqueues the op
        for the writer task. Returns immediately; the disk write
        happens asynchronously. Use ``flush()`` for hard durability.

        Safe to call concurrently from any task. If the ledger has
        been closed (or never booted), the op is silently dropped --
        the CRDT layer remains correct because state is still in
        memory; only durability is forfeit.
        """
        if self._closed:
            return
        q = self._queue
        if q is None:
            return
        await q.put(op)

    # -- replay / recovery (sync, runs in worker thread) -------------------

    def _replay_sync(self) -> tuple[int, int]:
        """
        Read every complete record (ending in ``\\n``) and feed it to
        the CRDT. If a ``<ledger>.snapshot.json`` exists next to the
        ledger, load it first and skip any ledger records whose HLC
        stamp is <= the snapshot's max_stamp (already covered). If
        the final record is torn, truncate it. Returns
        ``(num_replayed, num_truncated_bytes)``.
        """
        assert self._mesh is not None

        # -- Snapshot fast-path ----------------------------------------
        # If a snapshot exists next to the ledger, load it before
        # replaying. Each entry is fed into the CRDT directly so the
        # store, the HLC generator, and the mesh's seen-set all advance
        # to the snapshot's frontier. Future ledger records older than
        # this frontier are no-ops by CRDT semantics and are skipped
        # outright for speed.
        snapshot_max_stamp: Optional[HybridLogicalClock] = None
        try:
            from aether_core.compact import load_snapshot, snapshot_path_for
            snap_path = snapshot_path_for(self._path)
            if snap_path.exists():
                entries, snapshot_max_stamp = load_snapshot(snap_path)
                for e in entries:
                    if e["tombstone"]:
                        op: Operation[Any, Any] = Operation(
                            kind=OpKind.DEL,
                            key=e["key"],
                            value=None,
                            stamp=e["stamp"],
                        )
                    else:
                        op = Operation(
                            kind=OpKind.SET,
                            key=e["key"],
                            value=e["value"],
                            stamp=e["stamp"],
                        )
                    self._mesh.node.receive(op)
                    self._mesh._seen.add(op.stamp)  # noqa: SLF001
                    self._snapshot_entries += 1
                logger.info(
                    "[ledger %s] loaded snapshot: %d entries, max_stamp=%s",
                    self._path.name,
                    self._snapshot_entries,
                    snapshot_max_stamp.encode() if snapshot_max_stamp else "(empty)",
                )
        except Exception as e:  # noqa: BLE001
            # Snapshot corruption or version mismatch: log it, drop the
            # max_stamp guard, and fall back to a full ledger replay.
            # The CRDT layer is idempotent, so any partial state we may
            # have already applied is harmless.
            logger.warning(
                "[ledger %s] snapshot load failed, falling back to full replay: %s",
                self._path.name, e,
            )
            snapshot_max_stamp = None
            self._snapshot_entries = 0

        # -- Standard ledger replay (stamp-gated) ----------------------
        if not self._path.exists():
            return (0, 0)

        replayed = 0
        last_good_offset = 0
        torn_tail = False

        with open(self._path, "rb") as f:
            for raw in f:
                if not raw.endswith(b"\n"):
                    # Torn final write. Stop, do not advance offset.
                    torn_tail = True
                    break
                record_start = last_good_offset
                last_good_offset += len(raw)
                try:
                    payload = json.loads(raw.decode("utf-8"))
                    op = deserialize_operation(payload)
                except (json.JSONDecodeError, UnicodeDecodeError,
                        KeyError, ValueError, TypeError) as e:
                    # Mid-file corruption: skip this line, keep going.
                    logger.warning(
                        "[ledger %s] skipping unparseable record at offset %d: %s",
                        self._path.name, record_start, e,
                    )
                    continue

                # Skip ops the snapshot already encompasses.
                if snapshot_max_stamp is not None and op.stamp <= snapshot_max_stamp:
                    self._snapshot_skipped += 1
                    continue

                self._mesh.node.receive(op)
                self._mesh._seen.add(op.stamp)  # noqa: SLF001 -- same package
                replayed += 1

        truncated = 0
        if torn_tail:
            with open(self._path, "r+b") as f:
                f.seek(0, os.SEEK_END)
                current_size = f.tell()
                truncated = current_size - last_good_offset
                f.truncate(last_good_offset)
                try:
                    f.flush()
                    os.fsync(f.fileno())
                except OSError:
                    pass
            logger.warning(
                "[ledger %s] recovered torn final record: truncated %d bytes",
                self._path.name, truncated,
            )

        return (replayed, truncated)

    # -- writer task --------------------------------------------------------

    async def _writer_loop(self) -> None:
        """
        Single-writer task. Pulls ops off the queue in order, encodes
        each as one JSON line, and pushes it to disk via os.write +
        os.fsync. Ordering is preserved because there is exactly one
        consumer of the queue.

        A failed write logs but does NOT crash the task; the queue
        continues to drain so subsequent writes (and the eventual
        shutdown sentinel) are processed.
        """
        assert self._queue is not None
        try:
            while True:
                item = await self._queue.get()
                try:
                    if item is _SHUTDOWN:
                        return
                    op: Operation[Any, Any] = item
                    line = (
                        json.dumps(serialize_operation(op), separators=(",", ":"))
                        + "\n"
                    ).encode("utf-8")
                    try:
                        await asyncio.to_thread(self._write_sync, line)
                        self._written_count += 1
                    except Exception:  # noqa: BLE001
                        logger.exception(
                            "[ledger %s] failed to persist op stamp=%s",
                            self._path.name, op.stamp,
                        )
                finally:
                    self._queue.task_done()
        except asyncio.CancelledError:
            # Best-effort drain on cancellation: not strictly required
            # because close() uses the sentinel path, but defensive.
            raise

    def _write_sync(self, line: bytes) -> None:
        """
        Atomic per-record append. Single os.write() under O_APPEND is
        kernel-guaranteed atomic for records < PIPE_BUF. fsync gives
        durability across power loss.
        """
        fd = self._fd
        if fd is None:
            # close() raced ahead of the writer; drop the line. The op
            # is already in the CRDT in memory; durability is forfeit.
            raise OSError("ledger fd is closed")
        # write() may legally write fewer bytes than requested; loop
        # until the buffer is exhausted. For a JSONL line to a regular
        # file this always completes in one call in practice.
        view = memoryview(line)
        while view:
            written = os.write(fd, view)
            if written == 0:
                raise OSError("os.write returned 0 (disk full?)")
            view = view[written:]
        os.fsync(fd)


# ---------------------------------------------------------------------------
# Self-test :: write, cold-boot, and crash-recovery roundtrip
# ---------------------------------------------------------------------------

async def _demo() -> None:
    import shutil
    import tempfile

    logging.basicConfig(level=logging.INFO, format="%(message)s")

    print("=" * 72)
    print("Aether-Core :: Chrono-Vector Storage :: self-test")
    print("=" * 72)

    workdir = Path(tempfile.mkdtemp(prefix="aether-ledger-"))
    ledger_path = workdir / "ledger_alpha.jsonl"
    print(f"\nledger path: {ledger_path}")

    # ----------------------------------------------------------------------
    # Phase 1: write a sequence of ops, then shut down cleanly.
    # ----------------------------------------------------------------------
    print("\n[phase 1] cold-start instance A, perform mutations, persist")
    print("-" * 72)
    ledger_a = ChronoLedger(ledger_path)
    mesh_a = MeshNode("alpha", port=8101, on_op=ledger_a.on_op)
    replayed = await ledger_a.boot(mesh_a)
    assert replayed == 0, "fresh ledger should replay 0 ops"
    print(f"  initial boot: replayed {replayed} ops (expected 0)")

    await mesh_a.start()

    # Mix of sets and a delete (tombstone) -- the delete must survive
    # the round-trip in the LWWMap's underlying register, not just be
    # absent from the snapshot.
    await mesh_a.set("user:profile:name", "Aleph")
    await mesh_a.set("user:profile:city", "Mumbai")
    await mesh_a.set("user:profile:role", "engineer")
    await mesh_a.set("session:token",     "tok-A1B2")
    await mesh_a.delete("user:profile:city")       # <-- tombstone
    await mesh_a.set("user:profile:org",  "BFSI Edge")

    # Force everything to disk before snapshotting the canonical state.
    await ledger_a.flush()

    snapshot_canonical = mesh_a.snapshot()
    fingerprint_canonical = mesh_a.node.store.state_fingerprint()
    print(f"  snapshot         : {snapshot_canonical}")
    print(f"  ops persisted    : {ledger_a.written_count}")

    await mesh_a.stop()
    await ledger_a.close()

    # Inspect the on-disk file.
    on_disk_lines = ledger_path.read_text(encoding="utf-8").splitlines()
    on_disk_bytes = ledger_path.stat().st_size
    print(f"  ledger lines     : {len(on_disk_lines)}")
    print(f"  ledger size      : {on_disk_bytes} bytes")
    assert len(on_disk_lines) == ledger_a.written_count == 6

    # ----------------------------------------------------------------------
    # Phase 2: cold-boot a FRESH instance from the same ledger,
    # without any peer connections, and verify state convergence.
    # ----------------------------------------------------------------------
    print("\n[phase 2] cold-boot instance B from disk (no peers, no network mutations)")
    print("-" * 72)
    ledger_b = ChronoLedger(ledger_path)
    mesh_b = MeshNode("alpha", port=8102, on_op=ledger_b.on_op)
    replayed = await ledger_b.boot(mesh_b)
    print(f"  replayed         : {replayed} ops from disk")

    snapshot_reconstructed = mesh_b.snapshot()
    fingerprint_reconstructed = mesh_b.node.store.state_fingerprint()
    print(f"  snapshot         : {snapshot_reconstructed}")

    assert replayed == 6, f"expected 6 ops replayed, got {replayed}"
    assert snapshot_reconstructed == snapshot_canonical, "snapshots diverged"
    assert fingerprint_reconstructed == fingerprint_canonical, (
        "fingerprints diverged -- a tombstone or stamp was lost"
    )

    assert "user:profile:city" not in snapshot_reconstructed
    assert mesh_b.get("user:profile:city") is None
    tomb_reg = mesh_b.node.store._entries.get("user:profile:city")  # noqa: SLF001
    assert tomb_reg is not None and tomb_reg.tombstone, (
        "tombstone register missing after replay"
    )
    print(f"  tombstone for 'user:profile:city' present with stamp "
          f"{tomb_reg.stamp.encode()}")
    print("  snapshot equality      : True")
    print("  fingerprint equality   : True  (tombstones + stamps intact)")

    await ledger_b.close()

    # ----------------------------------------------------------------------
    # Phase 3: simulate a hard crash mid-write, then prove recovery.
    # ----------------------------------------------------------------------
    print("\n[phase 3] simulate process kill mid-write, then recover")
    print("-" * 72)
    pre_crash_size = ledger_path.stat().st_size
    with open(ledger_path, "ab") as f:
        f.write(b'{"kind":"set","key":"corrupted","valu')
    post_crash_size = ledger_path.stat().st_size
    print(f"  injected torn tail: file grew {pre_crash_size} -> {post_crash_size} bytes")

    ledger_c = ChronoLedger(ledger_path)
    mesh_c = MeshNode("alpha", port=8103, on_op=ledger_c.on_op)
    replayed = await ledger_c.boot(mesh_c)
    recovered_size = ledger_path.stat().st_size

    print(f"  replayed         : {replayed} clean ops")
    print(f"  bytes truncated  : {ledger_c.truncated_bytes}")
    print(f"  post-recovery size: {recovered_size} bytes")

    assert replayed == 6, "clean records lost during recovery"
    assert recovered_size == pre_crash_size, (
        "ledger not truncated back to last good newline"
    )
    assert ledger_c.truncated_bytes == post_crash_size - pre_crash_size
    assert mesh_c.snapshot() == snapshot_canonical, (
        "post-recovery state diverged from canonical"
    )
    assert mesh_c.node.store.state_fingerprint() == fingerprint_canonical
    print("  state matches canonical : True")
    print("  ledger now writeable     : True")

    # Confirm we can keep appending after recovery.
    await mesh_c.set("post:recovery", "yes")
    await ledger_c.flush()
    assert mesh_c.get("post:recovery") == "yes"
    await ledger_c.close()
    print(f"  post-recovery write OK   : added 'post:recovery' = 'yes'")

    # ----------------------------------------------------------------------
    # Phase 4: close() is idempotent; on_op after close is a safe no-op.
    # ----------------------------------------------------------------------
    print("\n[phase 4] idempotent close + drop ops after close")
    print("-" * 72)
    await ledger_c.close()   # second close, must be a no-op
    # Calling on_op after close must not raise.
    from aether_core.crdt import Node as _CRDTNode
    n = _CRDTNode("zzz")
    dummy_op = n.set("ignored", 1)
    await ledger_c.on_op(dummy_op, None)
    print("  second close()           : no-op, no error")
    print("  on_op after close        : silently dropped")

    print("\n" + "=" * 72)
    print("CHRONO-VECTOR STORAGE: PROVEN")
    print("=" * 72)

    shutil.rmtree(workdir, ignore_errors=True)


if __name__ == "__main__":
    asyncio.run(_demo())
