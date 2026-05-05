import type { Source } from './source.js';

/** Range requests with a gap less than or equal to this default will be coalesced. */
export const COALESCE_DEFAULT = 1024 * 1024;

/** No merged range will exceed this default size. */
export const MAX_RANGE_SIZE_DEFAULT = 16 * 1024 * 1024;

/** Up to this number of merged-range requests are dispatched in parallel. */
export const COALESCE_PARALLEL = 10;

export interface CoalesceOptions {
  /** Max gap (bytes) between two ranges before they're merged. Default: 1 MiB. */
  coalesce?: number;
  /** Max size (bytes) of any merged range. Default: 16 MiB. */
  maxRangeSize?: number;
  /** Forwarded to source.fetch */
  signal?: AbortSignal;
}

export interface ByteRange {
  offset: number;
  length: number;
}

/**
 * Fetch the given byte ranges from a source, coalescing nearby ranges into a smaller number
 * of underlying `source.fetch` calls. Returns one ArrayBuffer per input range, in input order.
 *
 * Internal helper — not exported from the package's public index.
 */
export async function coalesceRanges(
  source: Source,
  ranges: ByteRange[],
  options?: CoalesceOptions,
): Promise<ArrayBuffer[]> {
  if (ranges.length === 0) return [];
  void source;
  void options;
  throw new Error('not implemented');
}
