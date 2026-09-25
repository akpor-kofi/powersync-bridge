import { createContext, createElement, useContext, useEffect, useMemo, useSyncExternalStore, type ReactNode } from 'react';
import type {
  Bridge,
  FlaggerConfig,
  FlagResult,
  InfiniteListDefinition,
  InfiniteListResult,
  OnDemandOptions,
  OnDemandResult,
  OrderKey,
  Params,
  RacedListDefinition,
  RacedListResult,
  Store,
} from 'powersync-bridge';

// NOTE (R11, static assertion): this package never imports `useQuery` from TanStack.
// It renders from the engine store via useSyncExternalStore only.

const BridgeContext = createContext<Bridge | null>(null);

export interface BridgeProviderProps {
  bridge: Bridge;
  children?: ReactNode;
}

/** Mount once at app root. Crud-delta observation started at createBridge(), not here (F6-001). */
export function BridgeProvider({ bridge, children }: BridgeProviderProps) {
  return createElement(BridgeContext.Provider, { value: bridge }, children);
}

export function useBridge(): Bridge {
  const b = useContext(BridgeContext);
  if (!b) throw new Error('powersync-bridge: no <BridgeProvider> above this component');
  return b;
}

function useEpoch(bridge: Bridge): number {
  return useSyncExternalStore(bridge.epochStore.subscribe, bridge.epochStore.getSnapshot, bridge.epochStore.getSnapshot);
}

interface Retainable<S> {
  readonly store: Store<S>;
  retain(): void;
  release(): void;
}

/**
 * Acquire a ref-counted store for the current epoch and subscribe to it. Retain/release run in
 * an effect so StrictMode's double-invoke is a no-op pair (R10); the store is shared across
 * components that ask for the same definition + scope.
 */
function useRetained<S>(bridge: Bridge, key: string, acquire: () => Retainable<S>): S {
  const epoch = useEpoch(bridge);
  // eslint-disable-next-line react-hooks/exhaustive-deps
  const holder = useMemo(() => acquire(), [bridge, epoch, key]);
  useEffect(() => {
    holder.retain();
    return () => holder.release();
  }, [holder]);
  return useSyncExternalStore(holder.store.subscribe, holder.store.getSnapshot, holder.store.getSnapshot);
}

export function useRacedList<TItem, TScope, TApi, K extends OrderKey<TItem>>(
  def: RacedListDefinition<TItem, TScope, TApi, K>,
  scope: TScope,
): RacedListResult<TItem> {
  const bridge = useBridge();
  const key = `${def.id}:${JSON.stringify(scope ?? null)}`;
  return useRetained(bridge, key, () => bridge.raced(def, scope));
}

export interface UseInfiniteListResult<TItem> extends InfiniteListResult<TItem> {
  loadMore(): void;
  /** Clears API backoff and loads (R16). */
  retry(): void;
}

export function useInfiniteList<TItem, TScope, K extends OrderKey<TItem>, TCursor, TPartition>(
  def: InfiniteListDefinition<TItem, TScope, K, TCursor, TPartition>,
  scope: TScope,
): UseInfiniteListResult<TItem> {
  const bridge = useBridge();
  const epoch = useEpoch(bridge);
  const key = `${def.id}:${JSON.stringify(scope ?? null)}`;
  // eslint-disable-next-line react-hooks/exhaustive-deps
  const store = useMemo(() => bridge.infinite(def, scope), [bridge, epoch, key]);
  useEffect(() => {
    store.retain();
    return () => store.release();
  }, [store]);
  const result = useSyncExternalStore(store.store.subscribe, store.store.getSnapshot, store.store.getSnapshot);
  return useMemo(
    () => ({ ...result, loadMore: () => void store.loadMore(), retry: () => void store.retry() }),
    [result, store],
  );
}

/** The default "pull one row into the offline DB" primitive (§7). */
export function useOnDemandStream(name: string, params: Params | null, opts: OnDemandOptions = {}): OnDemandResult {
  const bridge = useBridge();
  const key = `${name}:${JSON.stringify(params)}:${JSON.stringify(opts)}`;
  return useRetained(bridge, key, () => bridge.onDemand(name, params, opts));
}

/** OPTIONAL: server-side flagging (§7). Prefer useOnDemandStream. */
export function useAutoSync(config: FlaggerConfig, ids: readonly string[]): FlagResult {
  const bridge = useBridge();
  const key = `${config.stream}:${JSON.stringify([...ids].sort())}`;
  return useRetained(bridge, key, () => bridge.autoSync(config, ids));
}
