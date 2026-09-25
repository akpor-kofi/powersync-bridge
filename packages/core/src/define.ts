import type { InfiniteListDefinition, OrderKey, RacedListDefinition } from './types';

type RacedInput<TItem, TScope, TApi, K extends OrderKey<TItem>> = Omit<RacedListDefinition<TItem, TScope, TApi, K>, 'kind'>;
type InfiniteInput<TItem, TScope, K extends OrderKey<TItem>, TCursor, TPartition> = Omit<
  InfiniteListDefinition<TItem, TScope, K, TCursor, TPartition>,
  'kind'
>;

/**
 * `TItem` is inferred from the query's awaited result (§8.1); `TApi` from `fetchSnapshot`.
 * Definitions close over the app's ORM instance; annotate the scope (and the page argument
 * for infinite lists) so the callback is not context-sensitive.
 */
export function defineRacedList<TItem, TScope, TApi, K extends OrderKey<TItem>>(
  input: RacedInput<TItem, TScope, TApi, K>,
): RacedListDefinition<TItem, TScope, TApi, K> {
  return { kind: 'raced', ...input };
}

export function defineInfiniteList<TItem, TScope, K extends OrderKey<TItem>, TCursor, TPartition = string>(
  input: InfiniteInput<TItem, TScope, K, TCursor, TPartition>,
): InfiniteListDefinition<TItem, TScope, K, TCursor, TPartition> {
  return { kind: 'infinite', ...input };
}
