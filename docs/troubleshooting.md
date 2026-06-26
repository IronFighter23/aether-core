# Troubleshooting

Common issues and their fixes, ordered roughly by how often they
come up in practice.

## Demo doesn't start

### `OSError: [Errno 98] Address already in use`

Another process is bound to one of the demo ports (8080, 8211, 8201).

**Diagnose** which port is busy:

```bash
# Linux / macOS
lsof -i :8080 -i :8211 -i :8201

# Windows
netstat -ano | findstr "8080 8211 8201"
```

**Fix** options:

- Stop the conflicting process.
- Edit `run_demo.py` and change the port constants at the top of
  the file.
- Re-run the demo.

### `ModuleNotFoundError: No module named 'aether_core'`

You're running `python` from outside the project root, or the venv
isn't activated.

**Fix:**

```bash
cd /path/to/aether-core
source .venv/bin/activate     # or .venv\Scripts\activate on Windows
python run_demo.py
```

Or use `uv run`, which handles activation for you:

```bash
uv run aether-demo
```

### `uv: command not found`

`uv` isn't installed. Either install it from
<https://docs.astral.sh/uv/>, or skip it:

```bash
python -m venv .venv
source .venv/bin/activate
pip install -e .
python run_demo.py
```

## Browser shows "disconnected · retrying…" forever

### The relay isn't running

Confirm `aether-demo` is still up in your terminal. If it crashed,
its output will explain why.

### The WebSocket URL is wrong

By default, demo pages connect to `ws://<hostname>:8211`. If you
visit the page via a non-default URL (e.g. `http://example.local:8080`
when the relay runs on `localhost`), the WebSocket URL will try
`ws://example.local:8211` and fail.

**Fix:** open the demo via the same hostname that the relay listens
on. For local development that's almost always
`http://localhost:8080`.

### The browser blocks `ws://` from an `https://` page

If you proxy the static site over HTTPS but the WebSocket still
points at plain `ws://`, browsers refuse "mixed content."

**Fix:** terminate TLS in front of the gateway too. See
[Deployment](deployment.md) for nginx/Caddy snippets that proxy
`/ws` to the gateway over WSS.

## State doesn't persist across restarts

### You're looking at the wrong file

The demo writes to `ledger_demo.jsonl` in the current working
directory. If you start the demo from different directories, each
will create its own ledger.

**Fix:** always start from the repo root, or set an explicit ledger
path in a custom launcher (see [Deployment](deployment.md)).

### The ledger file was deleted

If `ledger_demo.jsonl` is missing, the relay starts fresh. Browser
tabs still have their `localStorage` cache, but as soon as they
reconnect to the (now empty) relay, the gateway sends an empty
snapshot which wipes the local cache.

**Fix:** to avoid this in production, see the backup guidance in
[Deployment](deployment.md).

## State persists when I don't want it to

### Clear the offline cache

The browser caches everything in `localStorage`. To reset just one
tab without touching the durable ledger:

```js
// In DevTools console:
aether.clearCache();
location.reload();
```

Or use the Reset button if your app has one (kanban and markdown
demos do).

### Clear the durable ledger

Stop the relay, delete `ledger_demo.jsonl`, restart:

```bash
# Ctrl+C the running relay first
rm ledger_demo.jsonl ledger_demo.jsonl.snapshot.json
python run_demo.py
```

The relay restarts with no state, and every connected tab's
localStorage gets overwritten by the empty snapshot.

## Writes don't reach other tabs

### Check the rate limiter

If a tab fires a lot of writes very fast (e.g. a dragging script that
sends every mousemove), it can hit the rate limit and the gateway
will close the connection.

**Diagnose:** the relay logs show:

```
[gateway] client a3c4d4dd exceeded rate budget, closing
```

**Fix options:**

1. Debounce writes in the browser using
   [`requestAnimationFrame`](recipes.md#recipe-rate-limit-your-writes)
   so they max out at ~60 Hz.
2. Raise the limit by passing a custom `SecurityLimits` in your
   launcher (see [Python API](api-python.md)).

### Check the payload size

Values larger than 32 KiB (default) are rejected and never reach the
CRDT. The relay logs:

```
[gateway] dropping mutation with bad value: value size 50000 > cap 32768
```

**Fix:** either split the value across multiple keys, or raise
`max_value_bytes` in `SecurityLimits`. Splitting is usually correct —
huge values defeat the snapshot-boot optimisation.

### Check the connection cap

If you've exhausted the per-source connection limit (32 by default
per source IP), new tabs from the same IP receive close code 1013.

**Diagnose:** browser DevTools → Network → WS tab shows the
connection failed with code 1013.

**Fix:** close other tabs, or raise `max_connections_per_source`.

## "set" works but the value isn't what I expected

### Concurrent writes from different tabs

Two tabs writing the same key at the same instant: one wins by HLC
ordering. This is expected LWW behaviour. See
[Concepts](concepts.md#last-writer-wins-with-hlc-stamps).

**Fix** (if you don't want LWW): rephrase the data model so concurrent
writes don't target the same key. For example, instead of a single
`'comments'` array, use one key per comment (`'comment:<id>'`).
Concurrent writes to different keys never conflict.

### Read-modify-write race within one tab

```js
const current = aether.get('counter') ?? 0;   // 5
aether.set('counter', current + 1);            // 6

// ... but another tab also did the same, and now:
aether.get('counter') === 6;   // not 7!
```

Both tabs read 5, both write 6.

**Fix:** use the [list-backed counter](recipes.md#recipe-list-backed-counter-contention-safe)
pattern, or accept the occasional lost increment.

## Tests fail

### `test_protocol_block_in_readme` fails

You changed the wire protocol documentation in either `README.md` or
`aether_core/gateway.py` but didn't update the other. This is the
conformance test working as designed.

**Fix:** make both copies match the canonical block in
`tests/test_protocol_conformance.py::EXPECTED_PROTOCOL_BLOCK`. If
you intentionally changed the protocol, also update that constant.

### `test_rate_limit_closes_abuser` fails intermittently

The rate-limit test races against the gateway's close path. If it's
flaky in your environment, you may need to extend the wait timeout
or raise the test's burst size.

This test passes reliably on Linux + Python 3.12. If you hit
intermittent failures on macOS or older Python, please file an issue
with your `python --version` and `uname -a`.

### Conformance tests pass locally but fail in CI

CI often runs in environments with lower file-descriptor limits or
stricter SELinux/AppArmor. Common causes:

- **`Too many open files`** — raise `ulimit -n` to at least 1024.
- **Cannot bind to a port** — the conformance tests use ephemeral
  ports; this should never collide. If it does, you have something
  scanning ports on the CI host.

## Performance issues

### Boot is slow

The ledger has grown large. Run compaction:

```bash
python -m aether_core.compact ledger_demo.jsonl
```

Next boot will read the snapshot and skip already-covered records.
See [Benchmarks](../BENCHMARKS.md) for the speedup numbers.

### Browser is laggy with many keys

The default demo apps re-render the full DOM on every change. This is
fast enough for hundreds of keys but not thousands.

**Fix:** maintain a derived index (see
[Recipes — derived state caches](recipes.md#recipe-derived-state-caches))
and patch the DOM incrementally instead of full-rebuilding.

### Mesh convergence feels slow

Federated convergence depends on topology. A 10-node ring takes ~10
hops worst case (see [BENCHMARKS.md](../BENCHMARKS.md)). For faster
convergence, make the mesh denser — have each node connect to
multiple peers.

## Federation issues

### Peer connection refused

```
RuntimeError: bad handshake from ws://10.0.0.5:8201: ...
```

Likely causes:

- The peer isn't running (`telnet 10.0.0.5 8201` to confirm).
- A firewall is blocking the mesh port.
- The peer is running but its mesh interface is bound to `127.0.0.1`
  only. Check `AETHER_MESH_BIND`.

### Peer connects but doesn't sync

Both nodes need to be running and connected to each other (the
connection is duplex). If A connects to B but B isn't running, A
will keep retrying.

Also: the mesh port (8201) is **separate** from the gateway port
(8211). Browsers talk to the gateway; nodes talk to each other via
the mesh port. Make sure you're connecting to the right one.

### Duplicate node IDs

If two nodes use the same `node_id`, their HLC stamps collide and
the CRDT merge becomes non-deterministic. Symptoms: state seems to
randomly revert; writes from one node sometimes seem to "lose" to
older writes from the other.

**Fix:** every node must have a unique `node_id`. Use the hostname
or a UUID per deployment.

## Still stuck?

- Re-read [Getting Started](getting-started.md) — make sure you're
  not in a partial setup state.
- Check [Concepts](concepts.md) — the issue might be expected
  behaviour you didn't realise was the design.
- Open an issue:
  <https://github.com/IronFighter23/aether-core/issues> with:
  - Your OS and Python version
  - The exact command that failed
  - Full error output
  - A minimal reproducer if possible
