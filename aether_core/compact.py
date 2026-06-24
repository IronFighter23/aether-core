"""
Aether-Core :: Log Compaction Worker
====================================

Reads an append-only event ledger, folds every operation into a per-key
LWW register, and writes a condensed ``<ledger>.snapshot.json`` file.
The snapshot is the serialized end-state of the CRDT at compaction time
-- including tombstones and HLC stamps -- everything ``ChronoLedger.boot()``
needs to reconstruct the post-snapshot state instantly without replaying
every historical record.

Usage
-----
    # Produce <ledger>.snapshot.json next to the ledger
    python -m aether_core.compact ledger_demo.jsonl

    # Custom output path
    python -m aether_core.compact ledger_demo.jsonl --output /tmp/snap.json

    # Compact and atomically rotate the ledger (archive original,
    # start a fresh empty one). Safest pattern after a known-good
    # snapshot has been verified.
    python -m aether_core.compact ledger_demo.jsonl --rotate

Boot integration
----------------
``ChronoLedger.boot()`` auto-detects ``<ledger>.snapshot.json``. If
present, the snapshot loads first; only ledger records with HLC stamps
strictly newer than the snapshot's ``max_stamp`` are replayed.

Operational notes
-----------------
Compaction is **offline-only** -- stop the server before running this
script. Online compaction would require holding the writer lock for
the entire fold pass, which defeats the purpose.

Atomicity is enforced by writing to ``<snapshot>.tmp``, fsync'ing,
then ``os.replace()``-ing over the final path. A crash mid-write leaves
the prior snapshot untouched.
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import time
from pathlib import Path
from typing import Any, Optional

from aether_core.crdt import HybridLogicalClock, LWWMap, OpKind
from aether_core.mesh import deserialize_operation

__all__ = ["compact", "load_snapshot", "snapshot_path_for", "SNAPSHOT_VERSION"]

logger = logging.getLogger(__name__)

SNAPSHOT_VERSION = 1
SNAPSHOT_SUFFIX  = ".snapshot.json"


def snapshot_path_for(ledger_path: str | os.PathLike[str]) -> Path:
    """Default snapshot path: ``<ledger>.snapshot.json``."""
    return Path(str(ledger_path) + SNAPSHOT_SUFFIX)


def _encode_hlc(hlc: HybridLogicalClock) -> dict[str, Any]:
    return {"p": hlc.physical_ns, "l": hlc.logical, "n": hlc.node_id}


def _decode_hlc(payload: dict[str, Any]) -> HybridLogicalClock:
    return HybridLogicalClock(
        physical_ns=int(payload["p"]),
        logical=int(payload["l"]),
        node_id=str(payload["n"]),
    )


def compact(
    ledger_path: str | os.PathLike[str],
    snapshot_path: Optional[str | os.PathLike[str]] = None,
    *,
    rotate: bool = False,
) -> dict[str, Any]:
    """
    Fold the ledger at ``ledger_path`` into a snapshot at ``snapshot_path``.

    Returns a stats dict::

        {
            "records_read":       int,
            "skipped_corrupt":    int,
            "live_keys":          int,
            "tombstones":         int,
            "max_stamp":          HybridLogicalClock | None,
            "snapshot_path":      str,
            "snapshot_bytes":     int,
            "ledger_archived_to": str | None,
        }

    Atomicity: the snapshot is written to ``<path>.tmp``, fsync'd, then
    atomically renamed over the final path. A crash mid-write cannot
    corrupt a previous snapshot.
    """
    ledger_path = Path(ledger_path)
    snapshot_path = Path(snapshot_path) if snapshot_path else snapshot_path_for(ledger_path)

    if not ledger_path.exists():
        raise FileNotFoundError(f"ledger not found: {ledger_path}")

    # ------------------------------------------------------------------
    # 1. Fold every record into an in-memory LWW map.
    # ------------------------------------------------------------------
    store: LWWMap[str, Any] = LWWMap()
    records_read    = 0
    skipped_corrupt = 0
    max_stamp: Optional[HybridLogicalClock] = None

    with open(ledger_path, "rb") as f:
        for raw in f:
            if not raw.endswith(b"\n"):
                # Torn final record -- stop without consuming it.
                logger.warning("[compact] torn final record at EOF, ignored")
                break
            try:
                payload = json.loads(raw.decode("utf-8"))
                op = deserialize_operation(payload)
            except (json.JSONDecodeError, UnicodeDecodeError, KeyError, ValueError, TypeError) as e:
                skipped_corrupt += 1
                logger.warning("[compact] skipping unparseable record: %s", e)
                continue

            if op.kind is OpKind.SET:
                store.set(op.key, op.value, op.stamp)
            else:
                store.delete(op.key, op.stamp)
            records_read += 1
            if max_stamp is None or op.stamp > max_stamp:
                max_stamp = op.stamp

    # ------------------------------------------------------------------
    # 2. Serialize the post-fold register state.
    # ------------------------------------------------------------------
    entries: list[dict[str, Any]] = []
    live_keys  = 0
    tombstones = 0
    # NOTE: we access LWWMap._entries directly to iterate registers
    # *including* tombstones. The public snapshot() hides them, and
    # tombstones are part of the canonical CRDT state -- we must persist
    # them to keep the convergence math correct after a snapshot load.
    for key, reg in store._entries.items():  # noqa: SLF001
        entries.append({
            "key":       key,
            "value":     reg.value,
            "tombstone": reg.tombstone,
            "stamp":     _encode_hlc(reg.stamp),
        })
        if reg.tombstone:
            tombstones += 1
        else:
            live_keys += 1
    # Stable ordering -- nicer diffs, easier debugging.
    entries.sort(key=lambda e: e["key"])

    payload = {
        "version":      SNAPSHOT_VERSION,
        "compacted_at": time.time_ns(),
        "ledger_path":  str(ledger_path),
        "max_stamp":    _encode_hlc(max_stamp) if max_stamp is not None else None,
        "stats": {
            "records_read":    records_read,
            "skipped_corrupt": skipped_corrupt,
            "live_keys":       live_keys,
            "tombstones":      tombstones,
        },
        "entries": entries,
    }

    # ------------------------------------------------------------------
    # 3. Atomic write: tmp -> fsync -> rename.
    # ------------------------------------------------------------------
    tmp_path = Path(str(snapshot_path) + ".tmp")
    snapshot_path.parent.mkdir(parents=True, exist_ok=True)
    with open(tmp_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp_path, snapshot_path)
    snapshot_bytes = snapshot_path.stat().st_size

    # ------------------------------------------------------------------
    # 4. Optional ledger rotation.
    # ------------------------------------------------------------------
    archived_to: Optional[str] = None
    if rotate:
        # Archive (don't delete) the original ledger -- recoverable if
        # we ever need to re-derive the snapshot from raw history.
        archive = Path(str(ledger_path) + f".archived.{int(time.time())}")
        os.replace(ledger_path, archive)
        # Create a new empty ledger so future writes have a target.
        ledger_path.touch()
        archived_to = str(archive)

    return {
        "records_read":       records_read,
        "skipped_corrupt":    skipped_corrupt,
        "live_keys":          live_keys,
        "tombstones":         tombstones,
        "max_stamp":          max_stamp,
        "snapshot_path":      str(snapshot_path),
        "snapshot_bytes":     snapshot_bytes,
        "ledger_archived_to": archived_to,
    }


def load_snapshot(
    snapshot_path: str | os.PathLike[str],
) -> tuple[list[dict[str, Any]], Optional[HybridLogicalClock]]:
    """
    Load a snapshot from disk.

    Returns ``(entries, max_stamp)`` where ``entries`` is the list of
    ``{key, value, tombstone, stamp}`` dicts. Each ``stamp`` is a fully
    decoded ``HybridLogicalClock``. Returns ``([], None)`` if the file
    doesn't exist.

    Raises ``ValueError`` if the snapshot's version isn't supported.
    """
    snapshot_path = Path(snapshot_path)
    if not snapshot_path.exists():
        return [], None
    with open(snapshot_path, "r", encoding="utf-8") as f:
        payload = json.load(f)

    version = payload.get("version")
    if version != SNAPSHOT_VERSION:
        raise ValueError(f"unsupported snapshot version: {version!r}")

    entries: list[dict[str, Any]] = []
    for raw in payload.get("entries", []):
        entries.append({
            "key":       str(raw["key"]),
            "value":     raw.get("value"),
            "tombstone": bool(raw.get("tombstone", False)),
            "stamp":     _decode_hlc(raw["stamp"]),
        })

    ms = payload.get("max_stamp")
    max_stamp = _decode_hlc(ms) if ms is not None else None
    return entries, max_stamp


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def _format_int(n: int) -> str:
    return f"{n:,}"


def _format_stamp(s: Optional[HybridLogicalClock]) -> str:
    return s.encode() if s is not None else "(empty)"


def main(argv: Optional[list[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        prog="python -m aether_core.compact",
        description="Compact an Aether-Core append-only ledger into a snapshot.",
    )
    parser.add_argument("ledger", help="path to the append-only ledger (.jsonl)")
    parser.add_argument(
        "-o", "--output",
        help=(f"output snapshot path "
              f"(default: <ledger>{SNAPSHOT_SUFFIX})"),
    )
    parser.add_argument(
        "--rotate", action="store_true",
        help="archive the original ledger after compaction (safe; never deletes data)",
    )
    parser.add_argument(
        "--quiet", action="store_true",
        help="suppress info logging",
    )
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=logging.WARNING if args.quiet else logging.INFO,
        format="%(message)s",
    )

    try:
        result = compact(args.ledger, args.output, rotate=args.rotate)
    except FileNotFoundError as e:
        print(f"error: {e}", file=sys.stderr)
        return 1
    except Exception as e:  # noqa: BLE001
        print(f"error: {type(e).__name__}: {e}", file=sys.stderr)
        return 2

    print(f"Compacting ledger: {args.ledger}")
    print(f"  records read    : {_format_int(result['records_read'])}")
    if result["skipped_corrupt"]:
        print(f"  skipped corrupt : {_format_int(result['skipped_corrupt'])}")
    print(f"  live keys       : {_format_int(result['live_keys'])}")
    print(f"  tombstones      : {_format_int(result['tombstones'])}")
    print(f"  max stamp       : {_format_stamp(result['max_stamp'])}")
    print(f"  snapshot        : {result['snapshot_path']}")
    print(f"  snapshot size   : {_format_int(result['snapshot_bytes'])} bytes")
    if result["ledger_archived_to"]:
        print(f"  ledger archived : {result['ledger_archived_to']}")
        print(f"  ledger reset    : {args.ledger} is now empty")
    print("done.")
    return 0


# ---------------------------------------------------------------------------
# Self-test
# ---------------------------------------------------------------------------

def _demo() -> None:
    """
    Build a ledger with a known shape, compact it, then verify:
      1. compaction collapses sets+deletes for the same key to one entry
      2. tombstones survive the round trip
      3. CRDT state after reloading the snapshot is byte-identical to the
         state before compaction
      4. After --rotate, a fresh boot from snapshot + empty ledger gives
         the same state as the pre-rotation boot
    """
    import asyncio
    import shutil
    import tempfile

    from aether_core.mesh import MeshNode
    from aether_core.storage import ChronoLedger

    logging.basicConfig(level=logging.WARNING, format="%(message)s")

    print("=" * 72)
    print("Aether-Core :: Log Compaction :: self-test")
    print("=" * 72)

    workdir = Path(tempfile.mkdtemp(prefix="aether-compact-"))
    ledger_path = workdir / "ledger.jsonl"
    print(f"\nworkdir: {workdir}")

    async def run() -> None:
        # ---- Phase 1: write a workload with overwrites + a tombstone ----
        ledger = ChronoLedger(ledger_path)
        mesh = MeshNode("alpha", port=18301, on_op=ledger.on_op)
        await ledger.boot(mesh)
        await mesh.start()

        # Same key overwritten 5x -> compaction should keep ONE entry
        for v in ["v1", "v2", "v3", "v4", "v5"]:
            await mesh.set("user:profile:name", v)
        # Another key, set + then deleted -> compaction should keep the tombstone
        await mesh.set("session:token", "abcd")
        await mesh.delete("session:token")
        # Stable key set once
        await mesh.set("user:profile:city", "Mumbai")
        # Live keys at end: user:profile:name=v5, user:profile:city=Mumbai
        # Tombstone:        session:token

        await ledger.flush()
        canonical_fp = mesh.node.store.state_fingerprint()
        canonical_snapshot = mesh.snapshot()
        ledger_size_before = ledger_path.stat().st_size

        await mesh.stop()
        await ledger.close()

        print(f"\n[phase 1] ledger after writes: "
              f"{ledger_size_before} bytes, "
              f"{len(open(ledger_path).readlines())} records")
        print(f"  canonical snapshot: {canonical_snapshot}")
        assert "session:token" not in canonical_snapshot
        assert canonical_snapshot["user:profile:name"] == "v5"

        # ---- Phase 2: compact ----
        print("\n[phase 2] compacting...")
        result = compact(ledger_path)
        print(f"  records_read   : {result['records_read']} (expected 8)")
        print(f"  live keys      : {result['live_keys']}    (expected 2)")
        print(f"  tombstones     : {result['tombstones']}    (expected 1)")
        assert result["records_read"] == 8
        assert result["live_keys"] == 2
        assert result["tombstones"] == 1

        # Verify the snapshot file is well-formed JSON with the right shape
        snap_text = Path(result["snapshot_path"]).read_text()
        snap_obj  = json.loads(snap_text)
        assert snap_obj["version"] == SNAPSHOT_VERSION
        assert snap_obj["max_stamp"] is not None
        assert len(snap_obj["entries"]) == 3  # 2 live + 1 tombstone
        print(f"  snapshot json  : {result['snapshot_bytes']} bytes, "
              f"{len(snap_obj['entries'])} entries")

        # ---- Phase 3: cold boot from snapshot + ledger ----
        print("\n[phase 3] cold-boot with snapshot present...")
        ledger2 = ChronoLedger(ledger_path)
        mesh2 = MeshNode("alpha", port=18302, on_op=ledger2.on_op)
        replayed = await ledger2.boot(mesh2)
        fp2 = mesh2.node.store.state_fingerprint()
        print(f"  replayed         : {replayed} (expected 0 -- all ops covered by snapshot)")
        print(f"  snapshot entries : {ledger2.snapshot_entries}")
        print(f"  fingerprint match: {fp2 == canonical_fp}")
        assert fp2 == canonical_fp
        assert ledger2.snapshot_entries == 3
        await ledger2.close()

        # ---- Phase 4: rotation ----
        print("\n[phase 4] --rotate: archive ledger, start fresh...")
        result2 = compact(ledger_path, rotate=True)
        assert result2["ledger_archived_to"] is not None
        new_ledger_size = ledger_path.stat().st_size
        archive_exists = Path(result2["ledger_archived_to"]).exists()
        print(f"  archive created  : {archive_exists}")
        print(f"  new ledger size  : {new_ledger_size} bytes (expected 0)")
        assert archive_exists
        assert new_ledger_size == 0

        # ---- Phase 5: boot post-rotation ----
        print("\n[phase 5] boot from snapshot + empty ledger...")
        ledger3 = ChronoLedger(ledger_path)
        mesh3 = MeshNode("alpha", port=18303, on_op=ledger3.on_op)
        await ledger3.boot(mesh3)
        fp3 = mesh3.node.store.state_fingerprint()
        snap3 = mesh3.snapshot()
        print(f"  reconstructed    : {snap3}")
        print(f"  fingerprint match: {fp3 == canonical_fp}")
        assert fp3 == canonical_fp

        # And we can keep writing -- new ops go into the fresh ledger
        await mesh3.start()
        await mesh3.set("post:rotation", "yes")
        await ledger3.flush()
        await mesh3.stop()
        await ledger3.close()
        new_size = ledger_path.stat().st_size
        print(f"  post-write size  : {new_size} bytes (a fresh ledger, snapshot intact)")
        assert new_size > 0

    asyncio.run(run())

    shutil.rmtree(workdir, ignore_errors=True)
    print("\n" + "=" * 72)
    print("LOG COMPACTION: PROVEN")
    print("=" * 72)


if __name__ == "__main__":
    # If called with arguments, run the CLI. Otherwise run the self-test.
    if len(sys.argv) > 1:
        sys.exit(main())
    else:
        _demo()
