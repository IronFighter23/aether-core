# @nishantbhatte/aether-core

Zero-Transit sync engine for the browser. Your frontend variables *are* the database.

CRDT-backed, offline-first, real-time. No backend code to write — just connect to the Python relay and read/write keys.

```bash
npm install @nishantbhatte/aether-core
```

## Vanilla JS — 6 lines

```js
import Aether from '@nishantbhatte/aether-core';

const aether = new Aether('ws://localhost:8211');
await aether.ready();

aether.set('counter', (aether.get('counter') ?? 0) + 1);
aether.on('counter', (n) => console.log('counter is now', n));
```

Open the same page in two browser tabs. They sync. With **zero** backend code beyond `python -m aether_core.gateway`.

## React — `useAether` hook

```jsx
import { useAether } from '@nishantbhatte/aether-core/react';

function Counter() {
  const [count, setCount] = useAether('count', 0, {
    url: 'ws://localhost:8211',
  });
  return (
    <button onClick={() => setCount((count ?? 0) + 1)}>
      clicked {count} times
    </button>
  );
}
```

Same shape as `useState`. Same component, two tabs, real-time sync.

### App-wide config

Set the URL once instead of repeating it in every hook:

```jsx
import { configureAether, useAether } from '@nishantbhatte/aether-core/react';

configureAether({ url: 'ws://localhost:8211', authToken: 'optional-shared-secret' });

function Counter() {
  const [count, setCount] = useAether('count', 0);
  // ...
}
```

## With authentication

Pair the client `authToken` with `AuthConfig(token="...")` on the Python gateway. The token rides as a `?auth_token=...` query parameter on the WebSocket URL AND as a first-frame `{type:"auth", token:"..."}` message (whichever lands first). Use **wss://** in production so the token never crosses the wire in cleartext.

```js
new Aether('wss://your.host:8211', { authToken: process.env.AETHER_TOKEN });
```

## "Did my write win?" — `onSupersede`

LWW conflict resolution can override your write if a concurrent writer's HLC stamp is higher. The math doesn't lose data, but you may want to know when your write wasn't the final one:

```js
aether.onSupersede((key, attempted, actual) => {
  console.warn(`My write to ${key} lost the race: tried ${attempted}, got ${actual}`);
});
```

In React:

```jsx
import { useAetherSupersede } from '@nishantbhatte/aether-core/react';

function MyComponent() {
  useAetherSupersede((key, attempted, actual) => {
    toast(`Your edit to ${key} was overwritten`);
  }, []);
  // ...
}
```

## What's in the box

- **`Aether`** — the client class. Auto-reconnect, offline cache, BroadcastChannel cross-tab sync, optional auth.
- **`useAether`** — single-key React hook with `useState`-shaped API.
- **`useAetherSnapshot`** — full-state hook for kanban/board-style UIs.
- **`useAetherSupersede`** — be notified when one of your writes loses an LWW race.
- **`configureAether` / `getAether`** — app-level defaults and an imperative escape hatch.

## License

MIT © Nishant Bhatte. Backend lives at [github.com/IronFighter23/aether-core](https://github.com/IronFighter23/aether-core).
