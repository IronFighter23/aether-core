# Getting Started

In 10 minutes you'll have a working real-time collaborative app on
your machine, powered by Aether-Core. No database, no backend
business logic, no API design — just a Python relay and HTML/JS that
talks to it.

By the end of this guide you'll have:

- The reference demos running locally
- Built your own minimal app (a shared counter) from scratch
- Understood how every byte of state flows through the system

## Prerequisites

You need exactly one tool installed:

- **Python 3.10 or newer** — check with `python --version`

Everything else (the websockets library, pytest for tests, etc.) is
pulled in automatically by `uv` or `pip`.

Optional but recommended:

- **[uv](https://docs.astral.sh/uv/)** — fast, modern Python package
  manager. Install with one command from the linked page.

## Part 1 — Run the reference demos (2 minutes)

Clone the repo and launch the demos:

```bash
git clone https://github.com/IronFighter23/aether-core
cd aether-core
uv sync               # creates a .venv with all dependencies
uv run aether-demo    # starts the demos
```

If you don't have `uv`:

```bash
python -m venv .venv
source .venv/bin/activate     # Windows: .venv\Scripts\activate
pip install -e .
python run_demo.py
```

You should see:

```
┌──────────────────────────────────────────────────────────────┐
│  Aether-Core V3 demo is live.                                │
│                                                              │
│    Topology   : http://localhost:8080/                       │
│    Kanban     : http://localhost:8080/demos/kanban.html      │
│    Markdown   : http://localhost:8080/demos/markdown.html    │
│    Gateway    : ws://localhost:8211                          │
│    Mesh peer  : ws://localhost:8201                          │
│    Ledger     : ledger_demo.jsonl                            │
└──────────────────────────────────────────────────────────────┘
```

Open <http://localhost:8080/demos/kanban.html> in **two browser tabs**
side by side. Drag a card in tab A — watch it move in tab B. That's
the engine working.

Now stop the server (Ctrl+C) and refresh either tab. The board still
renders. That's offline-first persistence — the topology is read from
`localStorage` even when the relay is gone. Restart the server and
the status indicator flips back to green within a few seconds.

## Part 2 — Build your own app (8 minutes)

Time to write something from scratch. We'll build a shared counter
that two tabs can increment, and you'll see exactly how little code
that takes.

### Step 1: create the HTML file

Make a new file `web/demos/counter.html` (the demo server picks up
anything under `web/` automatically):

```html
<!doctype html>
<html>
<head>
  <meta charset="utf-8">
  <title>Aether Counter</title>
  <style>
    body { font-family: sans-serif; padding: 40px; text-align: center; }
    .count { font-size: 96px; margin: 20px 0; }
    button { font-size: 24px; padding: 10px 20px; margin: 0 5px; }
    .status { color: #888; font-family: monospace; font-size: 12px; }
  </style>
</head>
<body>
  <h1>Shared counter</h1>
  <div class="count" id="count">0</div>
  <button id="dec">−</button>
  <button id="inc">+</button>
  <button id="reset">reset</button>
  <p class="status" id="status">connecting…</p>

  <script src="../aether.js"></script>
  <script>
    const aether = new Aether('ws://' + location.hostname + ':8211');
    const $count = document.getElementById('count');
    const $status = document.getElementById('status');

    function render() {
      $count.textContent = aether.get('counter') ?? 0;
    }

    document.getElementById('inc').onclick = () => {
      aether.set('counter', (aether.get('counter') ?? 0) + 1);
    };
    document.getElementById('dec').onclick = () => {
      aether.set('counter', (aether.get('counter') ?? 0) - 1);
    };
    document.getElementById('reset').onclick = () => {
      aether.delete('counter');
    };

    aether.on('counter', render);
    aether.onStatus((c) => $status.textContent = c ? 'connected' : 'offline');
    aether.ready().then(render);
  </script>
</body>
</html>
```

That is the complete app. Save the file. **No server changes needed.**

### Step 2: open it in two tabs

Without restarting anything, open
<http://localhost:8080/demos/counter.html> in two browser tabs.

- Click "+" in tab A — tab B updates instantly.
- Click "−" in tab B — tab A updates instantly.
- Click "reset" — the counter disappears in both tabs.
- Stop the server with Ctrl+C and refresh either tab — the counter
  still shows the last value.

You just built a real-time collaborative app with zero server code
and zero database. Read on to understand why.

### Step 3: understand what happened

Walk through your file line by line:

```js
const aether = new Aether('ws://' + location.hostname + ':8211');
```

Construct a client. It immediately:
1. Reads any cached state from `localStorage`.
2. Opens a WebSocket to the gateway on port 8211.
3. Waits for the server to send a snapshot.

```js
aether.set('counter', 42);
```

Three things happen in parallel:
1. The value is written to local memory (`aether.get('counter')` now returns 42).
2. The value is sent to the Python gateway over WebSocket.
3. The value is persisted to `localStorage` (debounced).

The gateway then:
1. Wraps the write in a CRDT operation with an HLC stamp.
2. Appends it to `ledger_demo.jsonl` on disk with `fsync`.
3. Broadcasts the operation to **every other connected tab**.

Every other tab's `aether.on('counter', ...)` watcher fires with the
new value.

```js
aether.on('counter', render);
```

This is a per-key watcher. The callback runs every time the value of
`counter` changes — whether the change came from this tab, another
tab, or a federated peer.

That's the whole system. Three primitives:

| Primitive | What it does |
|---|---|
| `aether.set(key, value)` | Write a value, sync to all peers, persist |
| `aether.get(key)` | Read the current local value |
| `aether.on(key, cb)` | Subscribe to changes |

Plus `aether.delete(key)` for removal and `aether.onAny(cb)` for
catch-all watchers. That's the entire user-facing API.

## Part 3 — Inspect the durable state

While the demo is still running, open a third terminal and look at
the ledger:

```bash
tail -f ledger_demo.jsonl
```

Click the counter buttons a few times. You'll see new lines appear in
real time, one per operation. Each line is a complete, immutable
record of one state change:

```json
{"kind":"set","key":"counter","value":42,"stamp":{"p":1735300100000000000,"l":0,"n":"alpha"}}
{"kind":"set","key":"counter","value":43,"stamp":{"p":1735300101000000000,"l":0,"n":"alpha"}}
```

Stop the server, delete `ledger_demo.jsonl`, restart, refresh the
tab. The counter is back to 0 — the durable state is gone, even
though `localStorage` still had it. The server's snapshot took
precedence as soon as the WebSocket reconnected.

Now restore the ledger (or repeat the writes) and the counter comes
back. The ledger is the source of truth; `localStorage` is the
offline-first cache; the in-memory state map is the working set.

## Where to next

You've built a working app and seen state flow through every layer.
Here's a suggested next path:

1. **[Concepts](concepts.md)** — the mental model behind what you
   just built. CRDTs, HLCs, why `set('counter', current + 1)` is safe
   even with concurrent users.
2. **[JavaScript API reference](api-javascript.md)** — every method
   you didn't use yet (presence, onAny, onStatus, clearCache, etc.).
3. **[Recipes](recipes.md)** — patterns for lists, multi-user
   counters, optimistic UI, schema versioning.
4. **[Deployment](deployment.md)** — when you're ready to put it
   behind TLS on a real server.

## Troubleshooting this tutorial

If `uv run aether-demo` doesn't start, see
[Troubleshooting](troubleshooting.md). The most common issues are:

- **Port 8080, 8211, or 8201 already in use** — another process is
  using one of the demo ports. Either stop that process or edit
  `run_demo.py` to pick different ports.
- **`uv: command not found`** — install uv from
  <https://docs.astral.sh/uv/> or use the `pip install -e .` path
  instead.
- **Browser shows "disconnected · retrying…" forever** — the gateway
  isn't running. Confirm `aether-demo` is still up in your terminal.
