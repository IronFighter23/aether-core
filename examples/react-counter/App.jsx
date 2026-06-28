// Aether-Core :: React counter (the "useState but synced" demo)
// ==============================================================
//
// Drop this file into any React 18+ project (Vite, CRA, Next, etc.)
// alongside the `@nishantbhatte/aether-core` package on npm.
// Open the app in two browser tabs. They sync.
//
// The useAether hook has the same shape as useState. That's the only
// thing you need to know. Everything else -- WebSocket reconnects,
// offline cache, conflict resolution -- happens inside the hook.

import { useAether, useAetherSupersede } from '@nishantbhatte/aether-core/react';

export default function Counter() {
    // Same shape as useState, but the value is shared across every
    // browser tab connected to the same gateway.
    const [count, setCount, { connected, ready }] = useAether('count', 0, {
        url: 'ws://localhost:8211',
        // For an auth-required gateway, also pass:
        //   authToken: 'your-shared-secret'
    });

    // OPTIONAL: react when one of your writes lost the LWW race to a
    // concurrent writer. The CRDT doesn't drop writes silently -- it
    // tells you when one of yours wasn't the final value.
    useAetherSupersede((key, attempted, actual) => {
        console.warn(
            `My write to ${key} lost (tried ${attempted}, ended at ${actual})`
        );
    }, []);

    if (!ready) return <p>loading…</p>;

    return (
        <main style={{ font: '16px/1.5 system-ui', maxWidth: 480, margin: '4rem auto', padding: '0 1rem' }}>
            <h1>Aether counter</h1>
            <p>Open this page in two tabs. Click the button. They sync.</p>

            <div style={{ font: '700 3rem/1 ui-monospace, monospace', margin: '1rem 0' }}>
                {count}
            </div>

            <button
                onClick={() => setCount((prev) => (prev ?? 0) + 1)}
                style={{ font: 'inherit', padding: '0.6rem 1rem', cursor: 'pointer',
                         border: '1px solid #444', borderRadius: 6, background: '#fff' }}
            >
                +1
            </button>{' '}
            <button
                onClick={() => setCount(0)}
                style={{ font: 'inherit', padding: '0.6rem 1rem', cursor: 'pointer',
                         border: '1px solid #444', borderRadius: 6, background: '#fff' }}
            >
                reset
            </button>

            <p style={{ font: '0.85rem/1 ui-monospace, monospace', color: connected ? '#1f7a1f' : '#b91c1c' }}>
                {connected ? 'connected ✓' : 'offline (cached)'}
            </p>
        </main>
    );
}
