import pLimit from "p-limit";
import { rpcCall } from "./rpc.js";
import {
  HOT_CHUNK_THRESHOLD,
  FULL_TX_LIMIT,
  MAX_INFLIGHT,
  MIN_SLOT_SPAN,
  REQUEST_TIMEOUT_MS,
} from "./constants.js";
import type {
  ChunkDef,
  FullTransaction,
  BalanceEntry,
  SignatureEntry,
  SignatureSampleSummary,
  HotBandDef,
} from "./types.js";

const globalLimiter = pLimit(MAX_INFLIGHT);

const BASE_CONFIG = {
  transactionDetails: "full" as const,
  sortOrder: "asc" as const,
  limit: FULL_TX_LIMIT,
  commitment: "finalized" as const,
  maxSupportedTransactionVersion: 0,
  encoding: "json" as const,
};

const MAX_PAGES_PER_CHUNK = 32;
const MAX_SIGNATURE_PAGES_PER_CHUNK = 4;
const TARGET_SIGNATURES_PER_BAND = 80;
const MAX_HOT_BANDS = 12;
const MAX_SIGNATURE_PAGES_PER_ULTRA_DENSE_CHUNK = 12;
const MAX_ULTRA_DENSE_HOT_BANDS = 24;
const SIGNATURES_BASE_CONFIG = {
  transactionDetails: "signatures" as const,
  sortOrder: "asc" as const,
  limit: 1000,
  commitment: "finalized" as const,
  maxSupportedTransactionVersion: 0,
};

function findIdx(tx: FullTransaction, address: string): number {
  const keys = tx.transaction.message.accountKeys;
  for (let i = 0; i < keys.length; i++) {
    const k = keys[i];
    if ((typeof k === "string" ? k : k.pubkey) === address) return i;
  }
  const loaded = tx.meta.loadedAddresses;
  if (loaded) {
    const base = keys.length;
    for (let i = 0; i < loaded.writable.length; i++)
      if (loaded.writable[i] === address) return base + i;
    for (let i = 0; i < loaded.readonly.length; i++)
      if (loaded.readonly[i] === address) return base + loaded.writable.length + i;
  }
  return -1;
}

function extract(txns: FullTransaction[], address: string): BalanceEntry[] {
  const out: BalanceEntry[] = [];
  for (let t = 0; t < txns.length; t++) {
    const tx = txns[t];
    if (!tx?.meta?.postBalances || !tx.transaction?.message?.accountKeys) continue;

    const idx = findIdx(tx, address);
    if (idx === -1) continue;

    const bal = tx.meta.postBalances[idx];
    if (bal === undefined) continue;

    out.push({
      slot: tx.slot,
      transactionIndex: tx.transactionIndex ?? 0,
      signature: tx.transaction.signatures[0] ?? "",
      blockTime: tx.blockTime ?? null,
      balanceLamports: bal,
    });
  }
  return out;
}

function compareEntries(a: BalanceEntry, b: BalanceEntry): number {
  if (a.slot !== b.slot) return a.slot - b.slot;
  if (a.transactionIndex !== b.transactionIndex) {
    return a.transactionIndex - b.transactionIndex;
  }
  return a.signature.localeCompare(b.signature);
}

async function sampleChunkSignatures(
  address: string,
  chunk: ChunkDef,
  timeoutMs: number,
  maxPages: number,
): Promise<SignatureSampleSummary> {
  const entries: SignatureEntry[] = [];
  const seenTokens = new Set<string>();
  let token: string | null = null;
  let pagesFetched = 0;

  do {
    const page: { data: SignatureEntry[]; paginationToken: string | null } = await rpcCall<SignatureEntry>(
      address,
      {
        ...SIGNATURES_BASE_CONFIG,
        paginationToken: token ?? undefined,
        filters: { slot: { gte: chunk.gte, lt: chunk.lt }, status: "any" },
      },
      1,
      timeoutMs,
    );
    pagesFetched++;

    if (page?.data?.length) {
      for (let i = 0; i < page.data.length; i++) entries.push(page.data[i]);
    } else {
      return { entries, hasMore: false, pagesFetched };
    }

    token = page.paginationToken;
    if (!token || seenTokens.has(token) || pagesFetched >= maxPages) {
      return { entries, hasMore: Boolean(token), pagesFetched };
    }
    seenTokens.add(token);
  } while (token);

  return { entries, hasMore: false, pagesFetched };
}

function buildHotBands(chunk: ChunkDef, sample: SignatureSampleSummary, maxBands: number): HotBandDef[] {
  if (sample.entries.length === 0) return [];

  const groupSize = Math.max(
    TARGET_SIGNATURES_PER_BAND,
    Math.ceil(sample.entries.length / maxBands),
  );

  const bands: HotBandDef[] = [];
  let idx = 0;
  while (idx < sample.entries.length) {
    const startSlot = sample.entries[idx].slot;
    let endIdx = Math.min(idx + groupSize - 1, sample.entries.length - 1);
    while (
      endIdx + 1 < sample.entries.length &&
      sample.entries[endIdx + 1].slot === sample.entries[endIdx].slot
    ) {
      endIdx++;
    }

    const endSlot = sample.entries[endIdx].slot + 1;
    if (endSlot > startSlot) {
      bands.push({
        gte: startSlot,
        lt: endSlot,
        observedSignatures: endIdx - idx + 1,
        exact: !sample.hasMore || endIdx < sample.entries.length - 1,
      });
    }
    idx = endIdx + 1;
  }

  if (sample.hasMore) {
    const tailStart = sample.entries[sample.entries.length - 1].slot + 1;
    if (tailStart < chunk.lt) {
      bands.push({
        gte: tailStart,
        lt: chunk.lt,
        observedSignatures: 0,
        exact: false,
      });
    }
  }

  const normalized: HotBandDef[] = [];
  for (let i = 0; i < bands.length; i++) {
    const band = bands[i];
    const gte = Math.max(chunk.gte, band.gte);
    const lt = Math.min(chunk.lt, band.lt);
    if (lt <= gte) continue;

    const prev = normalized[normalized.length - 1];
    if (prev && gte <= prev.lt) {
      prev.lt = Math.max(prev.lt, lt);
      prev.observedSignatures += band.observedSignatures;
      prev.exact = prev.exact && band.exact;
    } else {
      normalized.push({ ...band, gte, lt });
    }
  }

  return normalized;
}

async function refineChunkWithSignatures(
  address: string,
  chunk: ChunkDef,
  depth: number,
  timeoutMs: number,
  useSignatureHotBands: boolean,
): Promise<BalanceEntry[] | null> {
  const sample = await sampleChunkSignatures(
    address,
    chunk,
    timeoutMs,
    useSignatureHotBands ? MAX_SIGNATURE_PAGES_PER_ULTRA_DENSE_CHUNK : MAX_SIGNATURE_PAGES_PER_CHUNK,
  );
  const hotBands = buildHotBands(
    chunk,
    sample,
    useSignatureHotBands ? MAX_ULTRA_DENSE_HOT_BANDS : MAX_HOT_BANDS,
  );
  if (hotBands.length <= 1) return null;

  const childChunks = hotBands.map((band) => ({ gte: band.gte, lt: band.lt }));
  if (
    childChunks.length === 1 &&
    childChunks[0].gte === chunk.gte &&
    childChunks[0].lt === chunk.lt
  ) {
    return null;
  }

  const completed = await Promise.all(
    childChunks.map((child) => globalLimiter(() => fetchRetry(address, child, depth + 1, useSignatureHotBands))),
  );
  return mergeManySorted(completed);
}

async function fetchChunk(
  address: string,
  chunk: ChunkDef,
  depth: number,
  timeoutMs: number,
  useSignatureHotBands: boolean,
): Promise<BalanceEntry[]> {
  const span = chunk.lt - chunk.gte;
  const shouldRefineBeforeFull = useSignatureHotBands && depth < 6 && span > MIN_SLOT_SPAN;

  if (shouldRefineBeforeFull) {
    const refined = await refineChunkWithSignatures(address, chunk, depth, timeoutMs, useSignatureHotBands);
    if (refined) return refined;
  }

  const result = await rpcCall<FullTransaction>(
    address,
    { ...BASE_CONFIG, filters: { slot: { gte: chunk.gte, lt: chunk.lt }, status: "any" } },
    2,
    timeoutMs,
  );

  if (!result?.data) return [];

  const isHot = result.data.length >= HOT_CHUNK_THRESHOLD;
  const hasPagination = result.paginationToken !== null;
  const canSplit = span > MIN_SLOT_SPAN && depth < 10;
  const shouldSplit = canSplit && (isHot || result.data.length >= FULL_TX_LIMIT);
  const shouldPaginate = hasPagination && result.data.length >= FULL_TX_LIMIT;
  const shouldUseSignatureBands = useSignatureHotBands && result.data.length >= FULL_TX_LIMIT;

  if (shouldSplit) {
    if (shouldUseSignatureBands) {
      const refined = await refineChunkWithSignatures(address, chunk, depth, timeoutMs, useSignatureHotBands);
      if (refined) return refined;
    }

    const mid = chunk.gte + (span >> 1);
    const [left, right] = await Promise.all([
      globalLimiter(() => fetchRetry(address, { gte: chunk.gte, lt: mid }, depth + 1, useSignatureHotBands)),
      globalLimiter(() => fetchRetry(address, { gte: mid, lt: chunk.lt }, depth + 1, useSignatureHotBands)),
    ]);
    return mergeSorted(left, right);
  }

  let txns = result.data;
  if (shouldPaginate) {
    let token: string | null = result.paginationToken;
    const seenTokens = new Set<string>();
    let pagesFetched = 0;
    while (token && pagesFetched < MAX_PAGES_PER_CHUNK && !seenTokens.has(token)) {
      seenTokens.add(token);
      const page: { data: FullTransaction[]; paginationToken: string | null } =
        await rpcCall<FullTransaction>(address, {
          ...BASE_CONFIG,
          paginationToken: token,
          filters: { slot: { gte: chunk.gte, lt: chunk.lt }, status: "any" },
        });
      if (!page?.data?.length) break;
      for (let i = 0; i < page.data.length; i++) txns.push(page.data[i]);
      token = page.paginationToken;
      pagesFetched++;
    }
  }

  return extract(txns, address);
}

async function fetchRetry(
  address: string,
  chunk: ChunkDef,
  depth: number,
  useSignatureHotBands: boolean,
): Promise<BalanceEntry[]> {
  try {
    return await fetchChunk(address, chunk, depth, REQUEST_TIMEOUT_MS, useSignatureHotBands);
  } catch {
    return fetchChunk(address, chunk, depth, REQUEST_TIMEOUT_MS * 2, useSignatureHotBands);
  }
}

function mergeSorted(a: BalanceEntry[], b: BalanceEntry[]): BalanceEntry[] {
  if (a.length === 0) return b;
  if (b.length === 0) return a;
  const out = new Array<BalanceEntry>(a.length + b.length);
  let i = 0, j = 0, k = 0;
  while (i < a.length && j < b.length) {
    out[k++] = compareEntries(a[i], b[j]) <= 0 ? a[i++] : b[j++];
  }
  while (i < a.length) out[k++] = a[i++];
  while (j < b.length) out[k++] = b[j++];
  return out;
}

interface HeapNode {
  arrayIndex: number;
  entryIndex: number;
  entry: BalanceEntry;
}

function pushHeap(heap: HeapNode[], node: HeapNode) {
  heap.push(node);
  let idx = heap.length - 1;
  while (idx > 0) {
    const parent = Math.floor((idx - 1) / 2);
    if (compareEntries(heap[parent].entry, heap[idx].entry) <= 0) break;
    [heap[parent], heap[idx]] = [heap[idx], heap[parent]];
    idx = parent;
  }
}

function popHeap(heap: HeapNode[]): HeapNode | undefined {
  if (heap.length === 0) return undefined;
  const top = heap[0];
  const tail = heap.pop();
  if (heap.length > 0 && tail) {
    heap[0] = tail;
    let idx = 0;
    while (true) {
      const left = idx * 2 + 1;
      const right = left + 1;
      let smallest = idx;

      if (left < heap.length && compareEntries(heap[left].entry, heap[smallest].entry) < 0) {
        smallest = left;
      }
      if (right < heap.length && compareEntries(heap[right].entry, heap[smallest].entry) < 0) {
        smallest = right;
      }
      if (smallest === idx) break;
      [heap[idx], heap[smallest]] = [heap[smallest], heap[idx]];
      idx = smallest;
    }
  }
  return top;
}

function mergeManySorted(arrays: BalanceEntry[][]): BalanceEntry[] {
  const heap: HeapNode[] = [];
  let totalLength = 0;

  for (let i = 0; i < arrays.length; i++) {
    if (arrays[i].length > 0) {
      totalLength += arrays[i].length;
      pushHeap(heap, { arrayIndex: i, entryIndex: 0, entry: arrays[i][0] });
    }
  }

  if (totalLength === 0) return [];

  const merged = new Array<BalanceEntry>(totalLength);
  let outIdx = 0;

  while (heap.length > 0) {
    const node = popHeap(heap)!;
    merged[outIdx++] = node.entry;
    const nextIndex = node.entryIndex + 1;
    if (nextIndex < arrays[node.arrayIndex].length) {
      pushHeap(heap, {
        arrayIndex: node.arrayIndex,
        entryIndex: nextIndex,
        entry: arrays[node.arrayIndex][nextIndex],
      });
    }
  }

  return merged;
}

export async function fetchAndProcess(
  address: string,
  chunks: ChunkDef[],
  concurrency: number,
  useSignatureHotBands: boolean,
): Promise<BalanceEntry[]> {
  const rootLimiter = pLimit(concurrency);
  const completed: BalanceEntry[][] = [];

  await new Promise<void>((resolve, reject) => {
    let finished = 0;
    const total = chunks.length;
    if (total === 0) { resolve(); return; }

    for (const chunk of chunks) {
      rootLimiter(() => globalLimiter(() => fetchRetry(address, chunk, 0, useSignatureHotBands)))
        .then((entries) => {
          if (entries.length > 0) completed.push(entries);
          if (++finished === total) resolve();
        })
        .catch(reject);
    }
  });

  const merged = mergeManySorted(completed);
  if (merged.length <= 1) return merged;
  const seen = new Set<string>();
  const out: BalanceEntry[] = [];
  for (let i = 0; i < merged.length; i++) {
    const sig = merged[i].signature;
    if (!seen.has(sig)) { seen.add(sig); out.push(merged[i]); }
  }
  return out;
}
