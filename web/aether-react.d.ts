/**
 * aether-react.d.ts
 * -----------------
 * Type declarations for the React bindings of Aether-Core.
 *
 * The hooks are deliberately small. ``useAether`` is the primary
 * surface; ``useAetherSnapshot`` is the escape hatch when you really
 * do need the full state.
 */
import type { Aether, AetherOptions, AetherValue, SupersedeWatcher } from './aether';

/** Combined config -- AetherOptions plus the required URL. */
export interface AetherConfig extends AetherOptions {
  /** The gateway WebSocket URL, e.g. ``ws://localhost:8211``. */
  url: string;
}

/** Module-level default config, applied to every hook call. */
export function configureAether(config: AetherConfig): void;

/** Grab the singleton Aether client without subscribing to re-renders. */
export function getAether(opts?: Partial<AetherConfig>): Aether;

/**
 * Sentinel meta returned by ``useAether``. Re-rendering on every
 * keystroke would defeat the point of useSyncExternalStore, so the
 * object identity changes only when something inside it changes.
 */
export interface AetherMeta {
  /** True after the first snapshot (gateway OR cache) has applied. */
  ready: boolean;
  /** True when the underlying WebSocket is open. */
  connected: boolean;
  /** The shared Aether client. Useful for ``client.delete(key)``. */
  client: Aether;
}

/**
 * A setter that mirrors React's useState API: accepts either a new
 * value or an updater function. Passing ``undefined`` deletes the
 * key in the CRDT.
 */
export type AetherSetter<T> = (next: T | undefined | ((prev: T | undefined) => T | undefined)) => void;

/**
 * Bind a single CRDT key to a React component.
 *
 * @example
 *   const [count, setCount] = useAether('counter', 0);
 *   const [todos, setTodos] = useAether<Todo[]>('todos', []);
 */
export function useAether<T extends AetherValue = AetherValue>(
  key: string,
  defaultValue?: T,
  options?: Partial<AetherConfig>,
): [T, AetherSetter<T>, AetherMeta];

/**
 * Read the whole snapshot and re-render on every mutation.
 * Use sparingly; prefer per-key ``useAether`` when possible.
 */
export interface AetherSnapshot {
  data: { [k: string]: AetherValue };
  setKey:    (key: string, value: AetherValue) => void;
  deleteKey: (key: string) => void;
  ready: boolean;
  connected: boolean;
  client: Aether;
}
export function useAetherSnapshot(options?: Partial<AetherConfig>): AetherSnapshot;

/** Subscribe to supersede events from inside a React component. */
export function useAetherSupersede(
  callback: SupersedeWatcher,
  deps?: ReadonlyArray<unknown>,
  options?: Partial<AetherConfig>,
): void;

export default useAether;
