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
 *   - Dual-layer concurrency control (root limiter + global MAX_INFLIGHT=64)
 *   - Undici connection pool (64 persistent keep-alive sockets, reused TLS)
 *   - Stable ordering by (slot, transactionIndex)
 *   - v0 versioned tx support (loadedAddresses → postBalances index mapping)
 *   - Sparse fast-path: ≤100 txns → single call, zero chunking overhead
 *
 * Optimized for lowest average latency across sparse, periodic, and dense wallets.
 *
 * Usage:  HELIUS_API_KEY=xxx npx tsx lamport-stream.ts <SOLANA_ADDRESS>
 * Output: stdout → JSON array of { slot, transactionIndex, signature, blockTime, balanceLamports }
 *         stderr → timing + RPC call count
 */

import { Pool } from "undici";
import pLimit from "p-limit";

const API_KEY = process.env.HELIUS_API_KEY;
if (!API_KEY) throw new Error("HELIUS_API_KEY environment variable is required.");

const RPC_URL = `https://mainnet.helius-rpc.com/?api-key=${API_KEY}`;
const HOT = 65;
const TARGET_TX = 50;
const MIN_C = 8;
const MAX_C = 64;
const MAX_INFLIGHT = 64;
const MIN_SPAN = 4;
const TIMEOUT = 5000;
const MAX_RETRY = 2;
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
  maxSupportedTransactionVersion?: number;
  encoding?: "json";
  filters?: { slot?: { gte?: number; lt?: number } };
}

interface RpcResult<T> {
  data: T[];
  paginationToken: string | null;
}

const url = new URL(RPC_URL);
const pool = new Pool(url.origin, {
  connections: 64,
  pipelining: 1,
  keepAliveTimeout: 30_000,
  keepAliveMaxTimeout: 60_000,
});

const rpcPath = url.pathname + url.search;
let rpcId = 0;
let calls = 0;

function retryable(e: unknown): boolean {
  if (!(e instanceof Error)) return true;
  if (e.message.startsWith("HTTP 4")) return false;
  if (e.message.startsWith("RPC error -32")) return false;
  return true;
}

async function rpc<T>(
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
      const { statusCode, body: responseBody } = await pool.request({
        path: rpcPath,
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

async function discover(addr: string) {
  const [newest, scout] = await Promise.all([
    rpc<any>(addr, {
      transactionDetails: "signatures",
      sortOrder: "desc",
      limit: 1,
      maxSupportedTransactionVersion: 0,
    }),
    rpc<any>(addr, {
      transactionDetails: "signatures",
      sortOrder: "asc",
      limit: SCOUT_LIM,
      maxSupportedTransactionVersion: 0,
    }),
  ]);

  if (!newest.data.length || !scout.data.length) {
    return { min: 0, max: 0, cnt: 0, range: 0, more: false };
  }

  const cnt = scout.data.length;
  return {
    min: scout.data[0].slot,
    max: newest.data[0].slot,
    cnt,
    range: scout.data[cnt - 1].slot - scout.data[0].slot + 1,
    more: scout.paginationToken !== null,
  };
}

function plan(s: Awaited<ReturnType<typeof discover>>) {
  let conc: number;
  let lo: number;
  let hi: number;

  if (s.cnt < 50 && !s.more) {
    conc = 8;
    lo = 4;
    hi = 8;
  } else if (s.cnt <= 500 && !s.more) {
    conc = 24;
    lo = 16;
    hi = 32;
  } else {
    conc = 48;
    lo = 48;
    hi = 64;
  }

  const total = s.max - s.min;
  if (total <= 0) return { chunks: [{ gte: s.min, lt: s.max + 1 }] as Chunk[], conc };

  const density = s.range > 0 ? s.cnt / s.range : 0.0001;
  let chunkCount = Math.ceil(total / Math.max(1, Math.floor(TARGET_TX / Math.max(density, 1e-6))));
  chunkCount = Math.max(lo, Math.min(hi, chunkCount));
  chunkCount = Math.max(MIN_C, Math.min(MAX_C, chunkCount));

  const step = Math.ceil(total / chunkCount);
  const chunks: Chunk[] = [];
  const scoutEnd = s.min + s.range;
  const outerDensity = s.more ? density * 1.5 : density * 0.3;

  for (let i = 0; i < chunkCount; i++) {
    const gte = s.min + i * step;
    const lt = i === chunkCount - 1 ? s.max + 1 : s.min + (i + 1) * step;
    if (gte >= s.max + 1) break;

    const span = lt - gte;
    const localDensity = gte < scoutEnd ? density : outerDensity;
    if (localDensity * span > 90 && span > 1) {
      const mid = gte + (span >> 1);
      chunks.push({ gte, lt: mid }, { gte: mid, lt });
    } else {
      chunks.push({ gte, lt });
    }
  }

  return { chunks, conc };
}

const globalLimiter = pLimit(MAX_INFLIGHT);
const FULL_CFG: RpcCfg = {
  transactionDetails: "full",
  sortOrder: "asc",
  limit: FULL_LIM,
  maxSupportedTransactionVersion: 0,
  encoding: "json",
};

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

async function fetchChunk(addr: string, chunk: Chunk, depth: number, timeoutMs: number): Promise<Entry[]> {
  const result = await rpc<any>(
    addr,
    { ...FULL_CFG, filters: { slot: { gte: chunk.gte, lt: chunk.lt } } },
    2,
    timeoutMs,
  );

  if (!result?.data) return [];

  const hot = result.data.length >= HOT;
  const paginated = result.paginationToken !== null;
  const span = chunk.lt - chunk.gte;

  if ((hot || paginated) && span > MIN_SPAN && depth < 10) {
    const mid = chunk.gte + (span >> 1);
    const [left, right] = await Promise.all([
      globalLimiter(() => fetchRetry(addr, { gte: chunk.gte, lt: mid }, depth + 1)),
      globalLimiter(() => fetchRetry(addr, { gte: mid, lt: chunk.lt }, depth + 1)),
    ]);
    return mergeSorted(left, right);
  }

  let txns = result.data;
  if (paginated) {
    let token: string | null = result.paginationToken;
    while (token) {
      const page = await rpc<any>(addr, {
        ...FULL_CFG,
        paginationToken: token,
        filters: { slot: { gte: chunk.gte, lt: chunk.lt } },
      });
      if (!page?.data?.length) break;
      for (let i = 0; i < page.data.length; i++) txns.push(page.data[i]);
      token = page.paginationToken;
    }
  }

  return toEntries(txns, addr);
}

async function fetchRetry(addr: string, chunk: Chunk, depth: number): Promise<Entry[]> {
  try {
    return await fetchChunk(addr, chunk, depth, TIMEOUT);
  } catch {
    return fetchChunk(addr, chunk, depth, TIMEOUT * 2);
  }
}

export async function getSOLBalanceOverTime(address: string): Promise<Entry[]> {
  const scout = await discover(address);
  if (scout.cnt === 0) return [];

  if (scout.cnt < 50 && !scout.more && scout.cnt <= FULL_LIM) {
    const result = await rpc<any>(address, {
      transactionDetails: "full",
      sortOrder: "asc",
      limit: FULL_LIM,
      maxSupportedTransactionVersion: 0,
      encoding: "json",
    });
    return toEntries(result.data, address);
  }

  const { chunks, conc } = plan(scout);
  const rootLimiter = pLimit(conc);
  let merged: Entry[] = [];

  await new Promise<void>((resolve, reject) => {
    let finished = 0;
    const total = chunks.length;
    if (total === 0) {
      resolve();
      return;
    }

    for (const chunk of chunks) {
      rootLimiter(() => globalLimiter(() => fetchRetry(address, chunk, 0)))
        .then((entries) => {
          if (entries.length > 0) merged = mergeSorted(merged, entries);
          if (++finished === total) resolve();
        })
        .catch(reject);
    }
  });

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

const address = process.argv[2];
if (!address) {
  console.error("Usage: npx tsx lamport-stream.ts <ADDRESS>");
  process.exit(1);
}

calls = 0;
const start = performance.now();
getSOLBalanceOverTime(address).then((history) => {
  const elapsed = performance.now() - start;
  console.log(JSON.stringify(history));
  console.error(`${history.length} entries | ${elapsed.toFixed(0)}ms | ${calls} RPC calls`);
}).catch((err) => {
  const elapsed = performance.now() - start;
  console.error(`Error after ${elapsed.toFixed(0)}ms (${calls} calls):`, err);
  process.exit(1);
});
