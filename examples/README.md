# Aether-Core examples

Each subdirectory here is a self-contained, copy-pasteable example. None of
them need a build step beyond `python -m aether_core.cli demo` (the relay)
and, for the React one, `npm install` in a fresh app.

| Example                                  | Lines of code | What it shows                                              |
| ---------------------------------------- | -------------:| -----------------------------------------------------------|
| [`vanilla-counter/`](./vanilla-counter/) | 20 lines      | One HTML file. Two browser tabs sync a counter in realtime. |
| [`react-counter/`](./react-counter/)     | 25 lines      | `useAether` hook. Drop into any React 18+ project.          |

## Run the relay first

In one terminal, from the repo root:

```bash
uv run aether-demo                  # OR
python -m aether_core.cli demo
```

This starts the Python gateway on `ws://localhost:8211` with no auth. To turn
auth on for these examples, see the [authentication recipe](../docs/recipes.md).

## Anatomy of the "no server code" claim

The examples below have **zero backend code**. The Python process is a
generic CRDT relay — you point your frontend at it and your variables
sync. No routes to define. No schema migrations. No serializers.

The thing that does the work is the CRDT math: every write carries a
Hybrid Logical Clock stamp, every node merges via the same idempotent
function, and the math is what guarantees convergence. The Python relay
is just the transport.
