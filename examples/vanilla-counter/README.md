# Vanilla counter

The smallest possible Aether-Core demo. One HTML file. No build step. No bundler.

## Run it

```bash
# Terminal 1 — start the relay
uv run aether-demo               # or: python -m aether_core.cli demo

# Terminal 2 — serve the HTML so the browser will load file paths
cd examples/vanilla-counter
python -m http.server 8000
```

Open <http://localhost:8000> in two browser windows. Click `+1` in one.
Watch the other tick up.

## What's actually happening

Three things:

1. `new Aether('ws://localhost:8211')` opens a WebSocket to the Python relay.
2. `aether.set('count', N)` writes the value through the CRDT layer. The
   relay applies it locally, fans it out to every other connected client,
   and persists it to disk so a relay restart doesn't lose the counter.
3. `aether.on('count', render)` subscribes to changes from anywhere — your
   own writes, other tabs' writes, restarts, anything.

There is no backend code beyond the relay binary, no API routes, no schema.
That's the entire point of "your frontend variables are the database".

## Variants to try

- Close one tab, increment in the other, then reopen the closed tab. It
  hydrates from `localStorage` first (instant), then catches up from the
  gateway snapshot.
- Stop the relay while a tab is running. The button still works
  (writes queue locally). Restart the relay and the queue flushes.
- Open a third tab and increment from each in turn. Every tab observes
  every write because the CRDT's HLC stamps produce a deterministic
  total order across writers.
