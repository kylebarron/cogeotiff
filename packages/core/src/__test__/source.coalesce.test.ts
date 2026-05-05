import assert from 'node:assert';
import { describe, it } from 'node:test';
import { coalesceRanges } from '../source.coalesce.js';
import type { Source } from '../source.js';

/**
 * Test source backed by an in-memory buffer. Records every fetch call so tests
 * can assert call counts and arguments.
 */
class RecordingSource implements Source {
  url = new URL('memory://recording');
  fetches: { offset: number; length: number }[] = [];
  /** Number of fetches in flight at any moment. Updated synchronously around the await. */
  inflight = 0;
  peakInflight = 0;
  /** If set, every fetch awaits this many microtasks before returning, exposing concurrency. */
  delayTicks = 0;

  constructor(private readonly buffer: Uint8Array) {}

  async fetch(offset: number, length: number): Promise<ArrayBuffer> {
    this.fetches.push({ offset, length });
    this.inflight++;
    if (this.inflight > this.peakInflight) this.peakInflight = this.inflight;
    try {
      for (let i = 0; i < this.delayTicks; i++) await Promise.resolve();
      const slice = this.buffer.slice(offset, offset + length);
      return slice.buffer.slice(slice.byteOffset, slice.byteOffset + slice.byteLength) as ArrayBuffer;
    } finally {
      this.inflight--;
    }
  }
}

/** Build a buffer where each byte equals (index % 256). Easy to verify slices. */
function makeBuffer(size: number): Uint8Array {
  const buf = new Uint8Array(size);
  for (let i = 0; i < size; i++) buf[i] = i & 0xff;
  return buf;
}

describe('coalesceRanges', () => {
  it('returns [] and makes no fetches for empty input', async () => {
    const source = new RecordingSource(makeBuffer(64));
    const result = await coalesceRanges(source, []);
    assert.deepEqual(result, []);
    assert.equal(source.fetches.length, 0);
  });

  it('fetches a single range as one source call', async () => {
    const source = new RecordingSource(makeBuffer(64));
    const result = await coalesceRanges(source, [{ offset: 10, length: 5 }]);

    assert.equal(result.length, 1);
    assert.deepEqual(new Uint8Array(result[0]), new Uint8Array([10, 11, 12, 13, 14]));
    assert.equal(source.fetches.length, 1);
    assert.deepEqual(source.fetches[0], { offset: 10, length: 5 });
  });
});
