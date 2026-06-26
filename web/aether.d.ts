/**
 * aether.d.ts
 * -----------
 * Type declarations for the Aether-Core browser client. Drop next to
 * aether.js and reference it via:
 *
 *   <script src="aether.js"></script>
 *   <script type="module">
 *     /// <reference path="./aether.d.ts" />
 *     const a: import('./aether').Aether = new Aether('ws://localhost:8211');
 *   </script>
 *
 * Or import via a bundler:
 *
 *   import type { Aether, AetherOptions } from './aether';
 *
 * Every method documented here is provably wired in
 * tests/test_protocol_conformance.py and in test_aether_offline.js,
 * so the type surface cannot silently drift from the runtime contract.
 */

/**
 * Any value the CRDT accepts. The relay is type-agnostic, but the
 * value MUST round-trip through JSON.stringify -- no Functions,
 * BigInts (without explicit serialisation), Symbols, circular
 * references, or class instances that don't define toJSON.
 */
export type AetherValue =
  | string
  | number
  | boolean
  | null
  | { [k: string]: AetherValue }
  | AetherValue[];

/**
 * Constructor options.
 */
export interface AetherOptions {
  /** Reconnect automatically after a connection loss. Default: true. */
  autoReconnect?: boolean;
  /** Cap on the exponential-backoff delay between reconnects (ms). Default: 5000. */
  maxReconnectDelayMs?: number;
  /** Persist state to localStorage for offline-first hydration. Default: true. */
  persist?: boolean;
  /**
   * Override the localStorage cache key. Defaults to
   * `aether::state::<gatewayUrl>` so two apps on the same origin
   * cannot collide.
   */
  cacheKey?: string;
}

/** Callback for a single-key watcher. */
export type KeyWatcher<T extends AetherValue = AetherValue> = (
  newValue: T | undefined,
  oldValue: T | undefined,
) => void;

/** Callback for the wildcard watcher (fires on every change). */
export type AnyWatcher = (
  key: string,
  newValue: AetherValue | undefined,
  oldValue: AetherValue | undefined,
) => void;

/** Callback for connection-status changes. */
export type StatusWatcher = (connected: boolean) => void;

/** Callback for remote cursor positions (presence). */
export type PresenceWatcher = (
  id: string,
  x: number,
  y: number,
  color: string,
) => void;

/** Callback for "client left" events. */
export type LeaveWatcher = (id: string) => void;

/** Unsubscribe handle returned by every subscription method. */
export type Unsubscribe = () => void;

/**
 * The Aether client. Constructed once per page; reused across the
 * lifetime of the document. ``ready()`` resolves either when the
 * gateway delivers its first snapshot OR when the offline cache
 * successfully hydrates, whichever happens first.
 */
export class Aether {
  /**
   * @param url   The gateway URL, e.g. ``ws://localhost:8211``.
   * @param opts  Optional configuration. See ``AetherOptions``.
   */
  constructor(url: string, opts?: AetherOptions);

  /** The gateway URL this instance is bound to. */
  readonly url: string;

  /** ``true`` once the WebSocket has completed its handshake. */
  readonly connected: boolean;

  /** Server-issued unique id for this client session. Null until connected. */
  readonly clientId: string | null;

  /** Server-issued cursor colour for this client. Null until connected. */
  readonly clientColor: string | null;

  // ---- Mutations -------------------------------------------------------

  /**
   * Write a value. Returns immediately; the gateway acknowledges
   * via an echo that triggers any watcher subscribed to ``key``.
   * Optimistic: also applies locally so the originating tab sees
   * the change without waiting for the round-trip.
   */
  set<T extends AetherValue>(key: string, value: T): void;

  /**
   * Delete a key. The Python side records a tombstone, so the
   * delete cannot be resurrected by a late-arriving stale write.
   */
  delete(key: string): void;

  // ---- Reads -----------------------------------------------------------

  /** Read the current local value for ``key``, or ``undefined``. */
  get<T extends AetherValue = AetherValue>(key: string): T | undefined;

  /** True if ``key`` currently has a value (not a tombstone). */
  has(key: string): boolean;

  /** Returns a fresh array of every currently-live key. */
  keys(): string[];

  /** Returns a plain-object copy of the entire state. */
  snapshot(): { [k: string]: AetherValue };

  // ---- Subscriptions ---------------------------------------------------

  /** Subscribe to changes on a single key. */
  on<T extends AetherValue = AetherValue>(
    key: string,
    callback: KeyWatcher<T>,
  ): Unsubscribe;

  /** Subscribe to every change. */
  onAny(callback: AnyWatcher): Unsubscribe;

  /** Subscribe to connection-status flips. Fires once with the current state. */
  onStatus(callback: StatusWatcher): Unsubscribe;

  // ---- Presence (ephemeral cursors) -----------------------------------

  /** Subscribe to remote cursor positions. */
  onPresence(callback: PresenceWatcher): Unsubscribe;

  /** Subscribe to "remote client left" events. */
  onPresenceLeave(callback: LeaveWatcher): Unsubscribe;

  /**
   * Broadcast the local cursor position. Cursor coordinates are
   * NEVER persisted -- they ride the WebSocket and the
   * BroadcastChannel only.
   */
  sendPresence(x: number, y: number): void;

  // ---- Lifecycle -------------------------------------------------------

  /**
   * Promise that resolves once the client has usable state to show.
   * Resolves immediately when the localStorage cache hydrates;
   * otherwise resolves on the first gateway snapshot.
   */
  ready(): Promise<void>;

  /** Tear down the connection and stop auto-reconnect. */
  close(): void;

  /**
   * Wipe the offline cache for this gateway. Does NOT clear the
   * in-memory state.
   */
  clearCache(): void;
}

/* ----------------------------------------------------------------------
 * Global declaration for plain <script src="aether.js"></script> usage.
 * ---------------------------------------------------------------------- */

declare global {
  interface Window {
    Aether: typeof Aether;
  }
  const Aether: typeof import('./aether').Aether;
}

export default Aether;
