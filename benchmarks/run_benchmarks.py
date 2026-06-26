"""
Aether-Core :: public benchmark suite
=====================================

Measures the four dimensions developers care about most when deciding
whether to trust the engine for real workloads:

  1. Ledger write throughput      (ops/sec persisted with fsync)
  2. Ledger replay throughput     (ops/sec replayed on cold boot)
  3. Snapshot-boot speedup        (replay time with vs without snapshot)
  4. Gateway round-trip latency   (set on tab A -> echo on tab B, p50/p95/p99)
  5. Mesh convergence time        (N nodes, K writes -> all-replicas-equal)

The benchmark is fully self-contained: no external services, no fake
data sets, no rigged scenarios. Every workload is the same code path
the production gateway runs.

Usage::

    uv run aether-benchmark
    # or
    python -m benchmarks.run_benchmarks

Results are written to ``benchmarks/results.json`` for downstream
charting / regression detection.
"""
from __future__ import annotations

import asyncio
import json
import platform
import statistics
import sys
import tempfile
import time
import uuid
from pathlib import Path
from typing import Any

from aether_core.crdt import Operation, OpKind, HybridLogicalClock
from aether_core.gateway import ClientGateway
from aether_core.mesh import MeshNode
from aether_core.storage import ChronoLedger
from aether_core.compact import compact


def _machine_info() -> dict[str, Any]:
    return {
        "python":      sys.version.split()[0],
        "platform":    platform.platform(),
        "processor":   platform.processor() or platform.machine(),
        "executable":  sys.executable,
    }


# ---------------------------------------------------------------------------
# 1+2. Ledger write + replay throughput at three scales
# ---------------------------------------------------------------------------

async def bench_ledger(num_ops: int) -> dict[str, Any]:
    """
    Write ``num_ops`` to a fresh ledger, then cold-boot a second
    instance and replay. Returns durations + computed throughputs.

    Workload shape: realistic collaborative editing pattern where ~80%
    of writes overwrite existing keys (drags, edits, slider scrubs).
    The remaining 20% create new keys. This mirrors what a real
    topology or kanban session generates, and lets the snapshot
    benchmark show the actual compaction benefit.
    """
    workdir = Path(tempfile.mkdtemp(prefix="aether-bench-"))
    try:
        ledger_path = workdir / f"ledger_bench_{num_ops}.jsonl"
        ledger = ChronoLedger(ledger_path)
        mesh   = MeshNode("bench", port=0, on_op=ledger.on_op)
        await ledger.boot(mesh)
        await mesh.start()

        # Cardinality of the "active" key set. With this many unique
        # keys, ~80% of writes will overwrite one we've already written.
        # That is a realistic interactive-app shape (a few dozen objects
        # being mutated many times each).
        unique_keys = max(1, num_ops // 5)

        # ---- write phase ----------------------------------------------
        t0 = time.perf_counter()
        for i in range(num_ops):
            k = f"obj:{i % unique_keys:06d}"
            await mesh.set(k, {"i": i, "tag": "bench"})
        await ledger.flush()
        t_write = time.perf_counter() - t0

        write_ops_per_sec = num_ops / t_write
        ledger_size       = ledger_path.stat().st_size

        await mesh.stop()
        await ledger.close()

        # ---- replay phase ---------------------------------------------
        ledger2 = ChronoLedger(ledger_path)
        mesh2   = MeshNode("bench2", port=0, on_op=ledger2.on_op)
        t0 = time.perf_counter()
        replayed = await ledger2.boot(mesh2)
        t_replay = time.perf_counter() - t0
        replay_ops_per_sec = replayed / t_replay if t_replay > 0 else float("inf")

        # ---- compact + snapshot-boot phase ----------------------------
        # Run the offline compaction worker, then cold-boot AGAIN and
        # measure how much faster the snapshot path is.
        await ledger2.close()
        stats = compact(ledger_path)   # writes <ledger>.snapshot.json

        ledger3 = ChronoLedger(ledger_path)
        mesh3   = MeshNode("bench3", port=0, on_op=ledger3.on_op)
        t0 = time.perf_counter()
        await ledger3.boot(mesh3)
        t_snap_boot = time.perf_counter() - t0
        snap_skipped = ledger3.snapshot_skipped
        snap_loaded  = ledger3.snapshot_entries
        await ledger3.close()

        return {
            "num_ops":               num_ops,
            "unique_keys":           unique_keys,
            "ledger_bytes":          ledger_size,
            "write_seconds":         round(t_write, 4),
            "write_ops_per_sec":     round(write_ops_per_sec, 1),
            "replay_seconds":        round(t_replay, 4),
            "replay_ops_per_sec":    round(replay_ops_per_sec, 1),
            "snapshot_boot_seconds": round(t_snap_boot, 4),
            "snapshot_speedup":      (
                round(t_replay / t_snap_boot, 2) if t_snap_boot > 0 else None
            ),
            "snapshot_entries":      snap_loaded,
            "snapshot_skipped_ops":  snap_skipped,
            "compaction": {
                "records_read":   stats["records_read"],
                "live_keys":      stats["live_keys"],
                "tombstones":     stats["tombstones"],
                "snapshot_bytes": stats["snapshot_bytes"],
                "compaction_ratio": (
                    round(stats["snapshot_bytes"] / ledger_size, 3)
                    if ledger_size > 0 else None
                ),
            },
        }
    finally:
        import shutil
        shutil.rmtree(workdir, ignore_errors=True)


# ---------------------------------------------------------------------------
# 4. Gateway round-trip latency, two simulated browser clients
# ---------------------------------------------------------------------------

async def bench_gateway_roundtrip(num_samples: int = 500) -> dict[str, Any]:
    """
    Tab A writes a key; tab B observes the echo. Measure the wall-clock
    delta. Reports p50 / p95 / p99 / max.
    """
    from websockets import connect

    from aether_core._security import SecurityLimits

    workdir = Path(tempfile.mkdtemp(prefix="aether-bench-rt-"))
    try:
        # Bench mode uses generous limits so the harness itself is not
        # rate-limited. Production deployments should keep the defaults.
        bench_limits = SecurityLimits(
            messages_per_second=10_000,
            messages_burst=10_000,
            max_message_bytes=1_048_576,
            max_value_bytes=524_288,
        )

        ledger = ChronoLedger(workdir / "ledger_rt.jsonl")
        placeholder: dict[str, ClientGateway] = {}

        async def on_op(op, src):
            await ledger.on_op(op, src)
            gw = placeholder.get("gw")
            if gw is not None:
                await gw.on_op(op, src)

        mesh = MeshNode("rt", port=0, on_op=on_op, limits=bench_limits)
        gw   = ClientGateway(mesh, host="127.0.0.1", port=0, limits=bench_limits)
        placeholder["gw"] = gw

        await ledger.boot(mesh)
        await mesh.start()
        await gw.start()

        # Open two concurrent "browser" sessions.
        ws_a = await connect(gw.url)
        ws_b = await connect(gw.url)

        # Drain snapshots.
        for ws in (ws_a, ws_b):
            await ws.recv()  # hello
            await ws.recv()  # snapshot

        latencies_ms: list[float] = []
        for i in range(num_samples):
            key = f"rt:{i:06d}"
            tx  = time.perf_counter()
            await ws_a.send(json.dumps({
                "type": "set", "key": key, "value": tx,
            }))
            # Wait for the echo on ws_b. Note this includes the gateway's
            # await-on-mesh, the on_op fanout, AND the network round-trip,
            # so the measurement reflects what a real second tab observes.
            while True:
                raw = await ws_b.recv()
                msg = json.loads(raw)
                if msg.get("type") == "set" and msg.get("key") == key:
                    rx = time.perf_counter()
                    latencies_ms.append((rx - tx) * 1000.0)
                    break

        # Tear down.
        await ws_a.close()
        await ws_b.close()
        await gw.stop()
        await mesh.stop()
        await ledger.close()

        return {
            "samples":         num_samples,
            "p50_ms":          round(statistics.median(latencies_ms), 3),
            "p95_ms":          round(
                statistics.quantiles(latencies_ms, n=20)[18], 3
            ),
            "p99_ms":          round(
                statistics.quantiles(latencies_ms, n=100)[98], 3
            ),
            "max_ms":          round(max(latencies_ms), 3),
            "mean_ms":         round(statistics.mean(latencies_ms), 3),
            "stdev_ms":        round(statistics.stdev(latencies_ms), 3),
        }
    finally:
        import shutil
        shutil.rmtree(workdir, ignore_errors=True)


# ---------------------------------------------------------------------------
# 5. Mesh convergence: N nodes in a ring, one writer, time to all-equal
# ---------------------------------------------------------------------------

async def bench_mesh_convergence(num_nodes: int, num_writes: int) -> dict[str, Any]:
    """
    Spin up ``num_nodes`` nodes in a ring topology (each connected to
    its left neighbour). Issue ``num_writes`` from node 0. Measure the
    wall-clock time until every node observes every write.

    Ring is the worst-case linear topology for epidemic gossip --
    the message must traverse num_nodes - 1 hops. Real-world meshes
    are usually denser, so this is an upper bound.
    """
    base_port = 19_000  # unlikely to clash on a developer box
    nodes: list[MeshNode] = []
    try:
        for i in range(num_nodes):
            n = MeshNode(f"r{i}", port=base_port + i)
            await n.start()
            nodes.append(n)

        # Ring: r0 -> r1 -> r2 -> ... -> r(n-1) -> r0
        for i in range(num_nodes):
            j = (i + 1) % num_nodes
            await nodes[i].connect_to("127.0.0.1", base_port + j)

        # Allow handshakes to settle.
        await asyncio.sleep(0.05)

        # Write num_writes from node 0.
        keys = [f"w:{i:06d}" for i in range(num_writes)]
        t0 = time.perf_counter()
        for i, k in enumerate(keys):
            await nodes[0].set(k, i)

        # Spin until every node has every key. With epidemic gossip,
        # convergence in a ring of N takes O(N * msgsize) per hop.
        async def all_converged() -> bool:
            for n in nodes:
                snap = n.snapshot()
                if len(snap) < num_writes:
                    return False
            return True

        deadline = t0 + 30.0
        while time.perf_counter() < deadline:
            if await all_converged():
                break
            await asyncio.sleep(0.01)
        t_converge = time.perf_counter() - t0

        # Verify total convergence, fail loudly if not.
        for n in nodes:
            assert len(n.snapshot()) == num_writes, (
                f"node {n.id} sees {len(n.snapshot())} keys, "
                f"expected {num_writes} -- did not converge in time"
            )

        return {
            "num_nodes":         num_nodes,
            "num_writes":        num_writes,
            "topology":          "ring",
            "convergence_s":     round(t_converge, 4),
            "writes_per_sec":    round(num_writes / t_converge, 1),
        }
    finally:
        for n in nodes:
            try:
                await n.stop()
            except Exception:  # noqa: BLE001
                pass


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

async def _run_all() -> dict[str, Any]:
    print("=" * 72)
    print("Aether-Core :: public benchmark suite")
    print("=" * 72)
    machine = _machine_info()
    print(f"  python    : {machine['python']}")
    print(f"  platform  : {machine['platform']}")
    print(f"  processor : {machine['processor']}")

    results: dict[str, Any] = {
        "machine": machine,
        "ledger":  {},
        "gateway_roundtrip": None,
        "mesh_convergence":  [],
    }

    # ----- 1+2+3. Ledger throughput across scales --------------------
    for n in (1_000, 10_000, 50_000):
        print(f"\n[ledger] N={n:>6,}  writing + replaying + snapshot-booting ...")
        r = await bench_ledger(n)
        results["ledger"][str(n)] = r
        print(
            f"  write   : {r['write_ops_per_sec']:>10,.0f} ops/s  "
            f"({r['write_seconds']:.2f}s)"
        )
        print(
            f"  replay  : {r['replay_ops_per_sec']:>10,.0f} ops/s  "
            f"({r['replay_seconds']:.2f}s)"
        )
        if r["snapshot_speedup"] is not None:
            print(
                f"  snap-boot:{r['snapshot_boot_seconds']:>10.4f}s   "
                f"({r['snapshot_speedup']:.1f}x faster than full replay; "
                f"skipped {r['snapshot_skipped_ops']:,} ops)"
            )

    # ----- 4. Gateway round-trip latency -----------------------------
    print(f"\n[gateway] measuring round-trip latency (500 samples) ...")
    r = await bench_gateway_roundtrip(num_samples=500)
    results["gateway_roundtrip"] = r
    print(
        f"  p50={r['p50_ms']:.2f} ms  "
        f"p95={r['p95_ms']:.2f} ms  "
        f"p99={r['p99_ms']:.2f} ms  "
        f"max={r['max_ms']:.2f} ms"
    )

    # ----- 5. Mesh convergence ---------------------------------------
    for nodes, writes in ((3, 100), (5, 100), (10, 50)):
        print(f"\n[mesh] convergence: {nodes} nodes (ring), {writes} writes ...")
        r = await bench_mesh_convergence(nodes, writes)
        results["mesh_convergence"].append(r)
        print(
            f"  {r['num_nodes']:>2} nodes, {r['num_writes']:>3} writes  "
            f"-> converged in {r['convergence_s']:.3f}s "
            f"({r['writes_per_sec']:.0f} writes/s)"
        )

    return results


def main() -> None:
    results = asyncio.run(_run_all())

    out_path = Path(__file__).parent / "results.json"
    out_path.write_text(json.dumps(results, indent=2), encoding="utf-8")

    print("\n" + "=" * 72)
    print(f"BENCHMARK RESULTS WRITTEN TO {out_path}")
    print("=" * 72)


if __name__ == "__main__":
    main()
