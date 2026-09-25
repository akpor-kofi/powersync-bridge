/**
 * Identity is DERIVED from the JWT the connector returns, never supplied by the app (R15, [DX]).
 * The key is the canonical JSON of the stable claims — a cache key, not a hash.
 */

export const DEFAULT_VOLATILE_CLAIMS: readonly string[] = [
  // OIDC volatile set
  'exp', 'iat', 'nbf', 'jti', 'auth_time', 'at_hash', 'nonce', 'azp',
  // vendor session claims that rotate on refresh
  'sid', 'session_id', 'rat',
];

function base64UrlToString(input: string): string {
  const b64 = input.replace(/-/g, '+').replace(/_/g, '/');
  const padded = b64 + '='.repeat((4 - (b64.length % 4)) % 4);
  if (typeof atob === 'function') return decodeURIComponent(escape(atob(padded)));
  return Buffer.from(padded, 'base64').toString('utf8');
}

export function decodeJwtPayload(token: string): Record<string, unknown> {
  const parts = token.split('.');
  if (parts.length < 2 || !parts[1]) throw new Error('powersync-bridge: token is not a JWT');
  const parsed: unknown = JSON.parse(base64UrlToString(parts[1]));
  if (parsed === null || typeof parsed !== 'object' || Array.isArray(parsed)) {
    throw new Error('powersync-bridge: JWT payload is not an object');
  }
  return parsed as Record<string, unknown>;
}

function canonical(value: unknown): string {
  if (Array.isArray(value)) return `[${value.map(canonical).join(',')}]`;
  if (value !== null && typeof value === 'object') {
    const o = value as Record<string, unknown>;
    return `{${Object.keys(o).sort().map((k) => `${JSON.stringify(k)}:${canonical(o[k])}`).join(',')}}`;
  }
  return JSON.stringify(value);
}

export interface IdentityKeyOptions {
  readonly denyList?: readonly string[];
  /** Connection parameters opted in BY KEY (rotating values must be excluded explicitly). */
  readonly connectionParams?: Record<string, unknown>;
}

export function identityKeyFromClaims(claims: Record<string, unknown>, opts: IdentityKeyOptions = {}): string {
  const deny = new Set(opts.denyList ?? DEFAULT_VOLATILE_CLAIMS);
  const stable: Record<string, unknown> = {};
  for (const k of Object.keys(claims).sort()) {
    if (!deny.has(k)) stable[k] = claims[k];
  }
  return canonical({ claims: stable, params: opts.connectionParams ?? {} });
}

export function identityKeyFromToken(token: string, opts: IdentityKeyOptions = {}): string {
  return identityKeyFromClaims(decodeJwtPayload(token), opts);
}

/**
 * Tracks the current identity key and the global epoch. `observe()` with an unchanged key is a
 * no-op (token refreshes never churn); a changed key bumps the epoch synchronously and notifies
 * listeners BEFORE returning, so the store-root swap is part of the same synchronous operation.
 */
export class IdentityTracker {
  private key: string | null = null;
  private epochCounter = 0;
  private readonly listeners = new Set<(epoch: number, key: string | null) => void>();

  get epoch(): number {
    return this.epochCounter;
  }

  get currentKey(): string | null {
    return this.key;
  }

  observe(key: string | null): boolean {
    if (key === this.key) return false;
    this.key = key;
    this.epochCounter += 1;
    for (const l of this.listeners) l(this.epochCounter, key);
    return true;
  }

  /** A clear (hasSynced true → false) is an epoch signal even if the token is unchanged. */
  signalClear(): void {
    this.epochCounter += 1;
    for (const l of this.listeners) l(this.epochCounter, this.key);
  }

  onChange(l: (epoch: number, key: string | null) => void): () => void {
    this.listeners.add(l);
    return () => this.listeners.delete(l);
  }
}
