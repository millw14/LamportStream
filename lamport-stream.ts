/**
 * LamportStream
 *
 * Ultra-low latency SOL balance history computation using Helius RPC.
 *
 * Key optimizations:
 *   - 2-call parallel discovery (no binary search)
 *   - Density-aware adaptive chunking + concurrency (8 / 24 / 48)
 *   - Proactive pre-splitting of predicted-hot chunks from scout density
 *   - Recursive hot-chunk subdivision (threshold=65, before pagination triggers)
 *   - MIN_SLOT_SPAN guard prevents wasteful micro-splits
 *   - Dual-layer concurrency control (root limiter + global MAX_INFLIGHT cap)
 *   - Undici connection pool (64 persistent keep-alive sockets, reused TLS)
 *   - Stable ordering by (slot, transactionIndex)
 *   - Explicit finalized / all-history semantics (includes failed txs by default)
 *   - v0 versioned tx support (loadedAddresses → postBalances index mapping)
 *   - Sparse fast-path: ≤100 txns → single call, zero chunking overhead
 *   - Conditional middle scout for dense / periodic candidates
 *   - K-way merge to reduce dense-wallet CPU overhead
 *
 * Optimized for lowest average latency across sparse, periodic, and dense wallets.
 *
 * Usage:  HELIUS_API_KEY=xxx npx tsx lamport-stream.ts <SOLANA_ADDRESS>
 * Output: stdout → JSON array of { slot, transactionIndex, signature, blockTime, balanceLamports }
 *         stderr → timing + RPC call count
 */

import { Pool } from "undici";
import pLimit from "p-limit";
import { pathToFileURL } from "node:url";

const RPC_ORIGIN = "https://mainnet.helius-rpc.com";
const HOT = 65;
const TARGET_TX = 50;
const MIN_C = 8;
const MAX_C = 64;
const MAX_INFLIGHT = 64;
const MIN_SPAN = 4;
const TIMEOUT = 5000;
const MAX_RETRY = 2;
const MAX_PAGES_PER_CHUNK = 32;
const MAX_SIGNATURE_PAGES_PER_CHUNK = 4;
const TARGET_SIGNATURES_PER_BAND = 80;
const MAX_HOT_BANDS = 12;
const MAX_SIGNATURE_PAGES_PER_ULTRA_DENSE_CHUNK = 12;
const MAX_ULTRA_DENSE_HOT_BANDS = 24;
const ULTRA_DENSE_SIGNATURE_THRESHOLD = 0.05;
const SCOUT_LIM = 1000;
const FULL_LIM = 100;

interface Entry {
  slot: number;
  transactionIndex: number;
  signature: string;
  blockTime: number | null;
  balanceLamports: number;
}

interface Chunk {
  gte: number;
  lt: number;
}

interface RpcCfg {
  transactionDetails?: "signatures" | "full";
  sortOrder?: "desc" | "asc";
  limit?: number;
  paginationToken?: string;
  commitment?: "finalized" | "confirmed";
  maxSupportedTransactionVersion?: number;
  encoding?: "json";
  filters?: {
    slot?: { gte?: number; lt?: number };
    status?: "succeeded" | "failed" | "any";
  };
}

interface RpcResult<T> {
  data: T[];
  paginationToken: string | null;
}

interface SignatureTx {
  slot: number;
  transactionIndex: number;
  signature: string;
  blockTime: number | null;
  err: unknown;
}

interface SignatureSampleSummary {
  entries: SignatureTx[];
  hasMore: boolean;
  pagesFetched: number;
}

interface HotBand {
  gte: number;
  lt: number;
  observedSignatures: number;
  exact: boolean;
}

export interface RpcBreakdown {
  totalCalls: number;
  fullCalls: number;
  signatureCalls: number;
}

const url = new URL(RPC_ORIGIN);
const pool = new Pool(url.origin, {
  connections: 64,
  pipelining: 1,
  keepAliveTimeout: 30_000,
  keepAliveMaxTimeout: 60_000,
});

let rpcId = 0;
let calls = 0;
let fullCalls = 0;
let signatureCalls = 0;

export interface LamportStreamOptions {
  apiKey?: string;
  rootConcurrencyOverride?: number;
  maxInflightOverride?: number;
  includeFailedTransactions?: boolean;
}

export function getRpcCallCount(): number {
  return calls;
}

export function getRpcBreakdown(): RpcBreakdown {
  return { totalCalls: calls, fullCalls, signatureCalls };
}

export function resetRpcCallCount(): void {
  calls = 0;
  fullCalls = 0;
  signatureCalls = 0;
}

function retryable(e: unknown): boolean {
  if (!(e instanceof Error)) return true;
  if (e.message.startsWith("HTTP 4")) return false;
  if (e.message.startsWith("RPC error -32")) return false;
  return true;
}

function resolveApiKey(options?: LamportStreamOptions): string {
  const apiKey = options?.apiKey ?? process.env.HELIUS_API_KEY;
  if (!apiKey) {
    throw new Error("HELIUS_API_KEY environment variable is required.");
  }
  return apiKey;
}

function buildRpcPath(apiKey: string): string {
  return `/?api-key=${encodeURIComponent(apiKey)}`;
}

function resolveStatusFilter(options?: LamportStreamOptions): "succeeded" | "failed" | "any" {
  return options?.includeFailedTransactions === false ? "succeeded" : "any";
}

async function rpc<T>(
  apiKey: string,
  addr: string,
  cfg: RpcCfg,
  retries = MAX_RETRY,
  timeoutMs = TIMEOUT,
): Promise<RpcResult<T>> {
  const body = JSON.stringify({
    jsonrpc: "2.0",
    id: ++rpcId,
    method: "getTransactionsForAddress",
    params: [addr, cfg],
  });

  for (let attempt = 0; attempt <= retries; attempt++) {
    try {
      calls++;
      if (cfg.transactionDetails === "signatures") signatureCalls++;
      else fullCalls++;
      const { statusCode, body: responseBody } = await pool.request({
        path: buildRpcPath(apiKey),
        method: "POST",
        headers: { "content-type": "application/json" },
        body,
        bodyTimeout: timeoutMs,
        headersTimeout: timeoutMs,
      });

      const text = await responseBody.text();
      if (statusCode !== 200) throw new Error(`HTTP ${statusCode}: ${text.slice(0, 200)}`);

      const json = JSON.parse(text);
      if (json.error) throw new Error(`RPC error ${json.error.code}: ${json.error.message}`);
      return json.result;
    } catch (e) {
      if (attempt === retries || !retryable(e)) throw e;
      await new Promise((resolve) => setTimeout(resolve, Math.min(100 * 2 ** attempt, 1000)));
    }
  }

  throw new Error("unreachable");
}

async function discover(
  apiKey: string,
  addr: string,
  statusFilter: "succeeded" | "failed" | "any",
) {
  const [newest, scout] = await Promise.all([
    rpc<any>(apiKey, addr, {
      transactionDetails: "signatures",
      sortOrder: "desc",
      limit: 1,
      commitment: "finalized",
      maxSupportedTransactionVersion: 0,
      filters: { status: statusFilter },
    }),
    rpc<any>(apiKey, addr, {
      transactionDetails: "signatures",
      sortOrder: "asc",
      limit: SCOUT_LIM,
      commitment: "finalized",
      maxSupportedTransactionVersion: 0,
      filters: { status: statusFilter },
    }),
  ]);

  if (!newest.data.length || !scout.data.length) {
    return {
      min: 0,
      max: 0,
      cnt: 0,
      range: 0,
      more: false,
      midCnt: 0,
      midRange: 0,
      midMin: 0,
      midMax: 0,
    };
  }

  const cnt = scout.data.length;
  let midCnt = 0;
  let midRange = 0;
  let midMin = 0;
  let midMax = 0;

  const min = scout.data[0].slot;
  const max = newest.data[0].slot;
  const range = scout.data[cnt - 1].slot - scout.data[0].slot + 1;
  const more = scout.paginationToken !== null;

  if (more || cnt >= 200) {
    const midpoint = min + Math.floor((max - min) / 2);
    const halfWindow = Math.max(1, Math.floor(range / 2));
    const midStart = Math.max(min, midpoint - halfWindow);
    const midEnd = Math.min(max + 1, midpoint + halfWindow);
    const mid = await rpc<any>(apiKey, addr, {
      transactionDetails: "signatures",
      sortOrder: "asc",
      limit: SCOUT_LIM,
      commitment: "finalized",
      maxSupportedTransactionVersion: 0,
      filters: {
        status: statusFilter,
        slot: { gte: midStart, lt: midEnd },
      },
    });
    if (mid.data.length > 0) {
      midCnt = mid.data.length;
      midMin = mid.data[0].slot;
      midMax = mid.data[mid.data.length - 1].slot;
      midRange = mid.data[mid.data.length - 1].slot - mid.data[0].slot + 1;
    }
  }

  return {
    min,
    max,
    cnt,
    range,
    more,
    midCnt,
    midRange,
    midMin,
    midMax,
  };
}

function plan(s: Awaited<ReturnType<typeof discover>>, options?: LamportStreamOptions) {
  const total = s.max - s.min;
  const totalSlotRange = Math.max(1, total + 1);
  const oldestDensity = s.range > 0 ? s.cnt / s.range : 0.000001;
  const midDensity = s.midCnt && s.midRange ? s.midCnt / s.midRange : 0;
  const density = Math.max(oldestDensity, midDensity || 0);
  const ultraDense = density >= ULTRA_DENSE_SIGNATURE_THRESHOLD;
  const sampledTx = s.cnt + s.midCnt;
  const sampledRange = s.range + s.midRange;
  const blendedDensity = sampledRange > 0 ? sampledTx / sampledRange : density;
  const estimatedTotalTx = Math.max(blendedDensity, 0.000001) * totalSlotRange;
  let conc: number;
  let lo: number;
  let hi: number;

  if (!s.more && s.cnt < 50) {
    conc = 8;
    lo = 4;
    hi = 8;
  } else if (estimatedTotalTx <= 250 && s.cnt < 100) {
    conc = 8;
    lo = 4;
    hi = 8;
  } else if (estimatedTotalTx <= 3000 && density < 0.01) {
    conc = 24;
    lo = 16;
    hi = 32;
  } else {
    conc = 40;
    lo = 48;
    hi = 64;
  }

  if (total <= 0) return { chunks: [{ gte: s.min, lt: s.max + 1 }] as Chunk[], conc, ultraDense };

  let chunkCount = Math.ceil(total / Math.max(1, Math.floor(TARGET_TX / Math.max(density, 1e-6))));
  chunkCount = Math.max(lo, Math.min(hi, chunkCount));
  chunkCount = Math.max(MIN_C, Math.min(MAX_C, chunkCount));

  const chunks: Chunk[] = [];
  const scoutEnd = s.min + s.range;
  const midStart = s.midMin;
  const midEnd = s.midMax > 0 ? s.midMax + 1 : 0;
  const hasMidWindow = s.midCnt > 0 && midEnd > midStart;
  const outerDensity = Math.max(oldestDensity * (s.more ? 0.15 : 0.5), 0.000001);

  const appendRangeChunks = (start: number, end: number, chunkBudget: number, localDensity: number) => {
    if (end <= start || chunkBudget <= 0) return;
    const step = Math.max(1, Math.ceil((end - start) / chunkBudget));
    for (let cursor = start; cursor < end; cursor += step) {
      const gte = cursor;
      const lt = Math.min(end, cursor + step);
      const span = lt - gte;
      if (localDensity * span > 90 && span > 1) {
        const mid = gte + (span >> 1);
        chunks.push({ gte, lt: mid }, { gte: mid, lt });
      } else {
        chunks.push({ gte, lt });
      }
    }
  };

  if (estimatedTotalTx > 10000 && hasMidWindow && s.range > 0 && totalSlotRange > s.range * 2) {
    const oldestBudget = Math.max(12, Math.round(hi * 0.2));
    const midBudget = Math.max(18, Math.round(hi * 0.35));
    const outsideBudget = Math.max(8, hi - oldestBudget - midBudget);
    const beforeMidRange = Math.max(0, midStart - scoutEnd);
    const afterMidRange = Math.max(0, s.max + 1 - midEnd);
    const outsideRange = beforeMidRange + afterMidRange;
    const beforeMidBudget = outsideRange > 0 ? Math.round(outsideBudget * (beforeMidRange / outsideRange)) : 0;
    const afterMidBudget = outsideBudget - beforeMidBudget;

    appendRangeChunks(s.min, scoutEnd, oldestBudget, oldestDensity);
    appendRangeChunks(scoutEnd, midStart, beforeMidBudget, outerDensity);
    appendRangeChunks(midStart, midEnd, midBudget, midDensity);
    appendRangeChunks(midEnd, s.max + 1, afterMidBudget, outerDensity);
    return { chunks, conc: options?.rootConcurrencyOverride ?? conc, ultraDense };
  }

  const step = Math.ceil(total / chunkCount);

  for (let i = 0; i < chunkCount; i++) {
    const gte = s.min + i * step;
    const lt = i === chunkCount - 1 ? s.max + 1 : s.min + (i + 1) * step;
    if (gte >= s.max + 1) break;

    const span = lt - gte;
    const isInOldestRange = gte < scoutEnd;
    const isInMidRange = hasMidWindow && gte >= midStart && gte < midEnd;
    const localDensity = isInMidRange
      ? midDensity
      : isInOldestRange
        ? oldestDensity
        : outerDensity;
    if (localDensity * span > 90 && span > 1) {
      const mid = gte + (span >> 1);
      chunks.push({ gte, lt: mid }, { gte: mid, lt });
    } else {
      chunks.push({ gte, lt });
    }
  }

  return { chunks, conc: options?.rootConcurrencyOverride ?? conc, ultraDense };
}

function addressIndex(tx: any, addr: string): number {
  const keys = tx.transaction.message.accountKeys;
  for (let i = 0; i < keys.length; i++) {
    const key = keys[i];
    if ((typeof key === "string" ? key : key.pubkey) === addr) return i;
  }

  const loaded = tx.meta?.loadedAddresses;
  if (loaded) {
    const base = keys.length;
    for (let i = 0; i < loaded.writable.length; i++) {
      if (loaded.writable[i] === addr) return base + i;
    }
    for (let i = 0; i < loaded.readonly.length; i++) {
      if (loaded.readonly[i] === addr) return base + loaded.writable.length + i;
    }
  }

  return -1;
}

function toEntries(txns: any[], addr: string): Entry[] {
  const out: Entry[] = [];
  for (let i = 0; i < txns.length; i++) {
    const tx = txns[i];
    if (!tx?.meta?.postBalances || !tx.transaction?.message?.accountKeys) continue;

    const index = addressIndex(tx, addr);
    if (index === -1) continue;

    const balanceLamports = tx.meta.postBalances[index];
    if (balanceLamports === undefined) continue;

    out.push({
      slot: tx.slot,
      transactionIndex: tx.transactionIndex ?? 0,
      signature: tx.transaction.signatures[0] ?? "",
      blockTime: tx.blockTime ?? null,
      balanceLamports,
    });
  }
  return out;
}

function compareEntries(a: Entry, b: Entry): number {
  if (a.slot !== b.slot) return a.slot - b.slot;
  if (a.transactionIndex !== b.transactionIndex) {
    return a.transactionIndex - b.transactionIndex;
  }
  return a.signature.localeCompare(b.signature);
}

function mergeSorted(a: Entry[], b: Entry[]): Entry[] {
  if (!a.length) return b;
  if (!b.length) return a;

  const out = new Array<Entry>(a.length + b.length);
  let i = 0;
  let j = 0;
  let k = 0;

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
  entry: Entry;
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

function mergeManySorted(arrays: Entry[][]): Entry[] {
  const heap: HeapNode[] = [];
  let totalLength = 0;

  for (let i = 0; i < arrays.length; i++) {
    if (arrays[i].length > 0) {
      totalLength += arrays[i].length;
      pushHeap(heap, { arrayIndex: i, entryIndex: 0, entry: arrays[i][0] });
    }
  }

  if (totalLength === 0) return [];

  const merged = new Array<Entry>(totalLength);
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

function buildFullCfg(statusFilter: "succeeded" | "failed" | "any"): RpcCfg {
  return {
    transactionDetails: "full",
    sortOrder: "asc",
    limit: FULL_LIM,
    commitment: "finalized",
    maxSupportedTransactionVersion: 0,
    encoding: "json",
    filters: { status: statusFilter },
  };
}

function buildSignaturesCfg(statusFilter: "succeeded" | "failed" | "any"): RpcCfg {
  return {
    transactionDetails: "signatures",
    sortOrder: "asc",
    limit: SCOUT_LIM,
    commitment: "finalized",
    maxSupportedTransactionVersion: 0,
    filters: { status: statusFilter },
  };
}

async function sampleChunkSignatures(
  apiKey: string,
  addr: string,
  chunk: Chunk,
  statusFilter: "succeeded" | "failed" | "any",
  timeoutMs: number,
  maxPages: number,
): Promise<SignatureSampleSummary> {
  const cfg = buildSignaturesCfg(statusFilter);
  const entries: SignatureTx[] = [];
  const seenTokens = new Set<string>();
  let token: string | null = null;
  let pagesFetched = 0;

  do {
    const page: RpcResult<SignatureTx> = await rpc<SignatureTx>(
      apiKey,
      addr,
      {
        ...cfg,
        paginationToken: token ?? undefined,
        filters: { ...cfg.filters, slot: { gte: chunk.gte, lt: chunk.lt } },
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

function buildHotBands(chunk: Chunk, sample: SignatureSampleSummary, maxBands: number): HotBand[] {
  if (sample.entries.length === 0) return [];

  const groupSize = Math.max(
    TARGET_SIGNATURES_PER_BAND,
    Math.ceil(sample.entries.length / maxBands),
  );

  const bands: HotBand[] = [];
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

  const normalized: HotBand[] = [];
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
  apiKey: string,
  addr: string,
  chunk: Chunk,
  depth: number,
  timeoutMs: number,
  statusFilter: "succeeded" | "failed" | "any",
  globalLimiter: ReturnType<typeof pLimit>,
  useSignatureHotBands: boolean,
): Promise<Entry[] | null> {
  const sample = await sampleChunkSignatures(
    apiKey,
    addr,
    chunk,
    statusFilter,
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
    childChunks.map((child) =>
      globalLimiter(() => fetchRetry(apiKey, addr, child, depth + 1, statusFilter, globalLimiter, useSignatureHotBands))),
  );
  return mergeManySorted(completed);
}

async function fetchChunk(
  apiKey: string,
  addr: string,
  chunk: Chunk,
  depth: number,
  timeoutMs: number,
  statusFilter: "succeeded" | "failed" | "any",
  globalLimiter: ReturnType<typeof pLimit>,
  useSignatureHotBands: boolean,
): Promise<Entry[]> {
  const span = chunk.lt - chunk.gte;
  const shouldRefineBeforeFull = useSignatureHotBands && depth < 6 && span > MIN_SPAN;

  if (shouldRefineBeforeFull) {
    const refined = await refineChunkWithSignatures(
      apiKey,
      addr,
      chunk,
      depth,
      timeoutMs,
      statusFilter,
      globalLimiter,
      useSignatureHotBands,
    );
    if (refined) return refined;
  }

  const fullCfg = buildFullCfg(statusFilter);
  const result = await rpc<any>(
    apiKey,
    addr,
    { ...fullCfg, filters: { ...fullCfg.filters, slot: { gte: chunk.gte, lt: chunk.lt } } },
    2,
    timeoutMs,
  );

  if (!result?.data) return [];

  const hot = result.data.length >= HOT;
  const paginated = result.paginationToken !== null;
  const shouldSplit = span > MIN_SPAN && depth < 10 && (hot || result.data.length >= FULL_LIM);
  const shouldPaginate = paginated && result.data.length >= FULL_LIM;
  const shouldUseSignatureBands = useSignatureHotBands && result.data.length >= FULL_LIM;

  if (shouldSplit) {
    if (shouldUseSignatureBands) {
      const refined = await refineChunkWithSignatures(
        apiKey,
        addr,
        chunk,
        depth,
        timeoutMs,
        statusFilter,
        globalLimiter,
        useSignatureHotBands,
      );
      if (refined) return refined;
    }

    const mid = chunk.gte + (span >> 1);
    const [left, right] = await Promise.all([
      globalLimiter(() => fetchRetry(apiKey, addr, { gte: chunk.gte, lt: mid }, depth + 1, statusFilter, globalLimiter, useSignatureHotBands)),
      globalLimiter(() => fetchRetry(apiKey, addr, { gte: mid, lt: chunk.lt }, depth + 1, statusFilter, globalLimiter, useSignatureHotBands)),
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
      const page: RpcResult<any> = await rpc<any>(
        apiKey,
        addr,
        {
          ...fullCfg,
          paginationToken: token,
          filters: { ...fullCfg.filters, slot: { gte: chunk.gte, lt: chunk.lt } },
        },
      );
      if (!page?.data?.length) break;
      for (let i = 0; i < page.data.length; i++) txns.push(page.data[i]);
      token = page.paginationToken;
      pagesFetched++;
    }
  }

  return toEntries(txns, addr);
}

async function fetchRetry(
  apiKey: string,
  addr: string,
  chunk: Chunk,
  depth: number,
  statusFilter: "succeeded" | "failed" | "any",
  globalLimiter: ReturnType<typeof pLimit>,
  useSignatureHotBands: boolean,
): Promise<Entry[]> {
  try {
    return await fetchChunk(apiKey, addr, chunk, depth, TIMEOUT, statusFilter, globalLimiter, useSignatureHotBands);
  } catch {
    return fetchChunk(apiKey, addr, chunk, depth, TIMEOUT * 2, statusFilter, globalLimiter, useSignatureHotBands);
  }
}

export async function getSOLBalanceOverTime(
  address: string,
  options?: LamportStreamOptions,
): Promise<Entry[]> {
  const apiKey = resolveApiKey(options);
  const statusFilter = resolveStatusFilter(options);
  const scout = await discover(apiKey, address, statusFilter);
  if (scout.cnt === 0) return [];

  if (!scout.more && scout.cnt <= FULL_LIM) {
    const fullCfg = buildFullCfg(statusFilter);
    const result = await rpc<any>(apiKey, address, {
      ...fullCfg,
    });
    return toEntries(result.data, address);
  }

  const { chunks, conc, ultraDense } = plan(scout, options);
  const globalLimiter = pLimit(options?.maxInflightOverride ?? MAX_INFLIGHT);
  const rootLimiter = pLimit(conc);
  const completed: Entry[][] = [];

  await new Promise<void>((resolve, reject) => {
    let finished = 0;
    const total = chunks.length;
    if (total === 0) {
      resolve();
      return;
    }

    for (const chunk of chunks) {
      rootLimiter(() => globalLimiter(() =>
        fetchRetry(apiKey, address, chunk, 0, statusFilter, globalLimiter, ultraDense)))
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
  const out: Entry[] = [];
  for (let i = 0; i < merged.length; i++) {
    if (!seen.has(merged[i].signature)) {
      seen.add(merged[i].signature);
      out.push(merged[i]);
    }
  }
  return out;
}

function isDirectRun(): boolean {
  const entry = process.argv[1];
  return Boolean(entry) && import.meta.url === pathToFileURL(entry).href;
}

async function main() {
  const address = process.argv[2];
  if (!address) {
    console.error("Usage: npx tsx lamport-stream.ts <ADDRESS>");
    process.exit(1);
  }

  calls = 0;
  const start = performance.now();
  try {
    const history = await getSOLBalanceOverTime(address);
    const elapsed = performance.now() - start;
    const breakdown = getRpcBreakdown();
    console.log(JSON.stringify(history));
    console.error(`${history.length} entries | ${elapsed.toFixed(0)}ms | ${calls} RPC calls (${breakdown.fullCalls} full, ${breakdown.signatureCalls} sig)`);
  } catch (err) {
    const elapsed = performance.now() - start;
    console.error(`Error after ${elapsed.toFixed(0)}ms (${calls} calls):`, err);
    process.exit(1);
  }
}

if (isDirectRun()) {
  const keepAlive = setInterval(() => {}, 1000);
  void main()
    .catch((err) => {
      console.error(err);
      process.exitCode = 1;
    })
    .finally(() => {
      clearInterval(keepAlive);
    });
}
