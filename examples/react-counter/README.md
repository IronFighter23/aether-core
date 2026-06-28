# React counter

Same idea as `vanilla-counter`, but as a React component using the
`useAether` hook. The component looks like a normal `useState`, except
the state syncs across every browser tab connected to the same relay.

## Scaffold a project

The cleanest path is Vite — one command and you're done:

```bash
npm create vite@latest my-aether-app -- --template react
cd my-aether-app
npm install @nishantbhatte/aether-core
```

Then replace `src/App.jsx` with the contents of `App.jsx` in this folder.
Run:

```bash
# Terminal 1 — start the relay
uv run aether-demo

# Terminal 2 — start the React dev server
npm run dev
```

Open the URL Vite prints (usually <http://localhost:5173>) in two browser
tabs. Click `+1`. They sync.

## What's actually happening

```jsx
const [count, setCount] = useAether('count', 0, {
    url: 'ws://localhost:8211',
});
```

Reads like `useState`. Behaves like `useState`. Except:

- `count` is the value of the CRDT key `count` on the gateway. Other
  tabs read and write the same key.
- `setCount(v)` writes through the CRDT layer. Other tabs see it.
- The hook re-renders **only** when this specific key changes — so a
  large kanban board with 200 hooks doesn't re-render everyone on
  every cursor wiggle.

For the entire-state case (a board, a list of users, anything where
you want to react to *any* mutation), use `useAetherSnapshot` instead.

## Setting a default URL once

If you're going to call `useAether` from many components, set the URL
once at app startup:

```jsx
import { configureAether } from '@nishantbhatte/aether-core/react';

configureAether({ url: 'ws://localhost:8211' });
```

Then the `{ url }` option becomes optional on every `useAether` call.

## With authentication

If your gateway was started with `auth=AuthConfig(token='hunter2')`:

```jsx
configureAether({
    url: 'wss://your.host:8211',
    authToken: 'hunter2',
});
```

Use `wss://` (TLS) in production so the token is never sent in the
clear. The Python gateway accepts an `ssl_context=...` parameter for
that — see `docs/deployment.md` for the full setup.
