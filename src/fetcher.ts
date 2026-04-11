import pLimit from "p-limit";
import { rpcCall } from "./rpc.js";
import {
  HOT_CHUNK_THRESHOLD,
  FULL_TX_LIMIT,
  MAX_INFLIGHT,
  MIN_SLOT_SPAN,
  REQUEST_TIMEOUT_MS,
} from "./constants.js";
import type { ChunkDef, FullTransaction, BalanceEntry } from "./types.js";

const globalLimiter = pLimit(MAX_INFLIGHT);

const BASE_CONFIG = {
  transactionDetails: "full" as const,
  sortOrder: "asc" as const,
  limit: FULL_TX_LIMIT,
  commitment: "finalized" as const,
  maxSupportedTransactionVersion: 0,
  encoding: "json" as const,
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

async function fetchChunk(
  address: string,
  chunk: ChunkDef,
  depth: number,
  timeoutMs: number,
): Promise<BalanceEntry[]> {
  const result = await rpcCall<FullTransaction>(
    address,
    { ...BASE_CONFIG, filters: { slot: { gte: chunk.gte, lt: chunk.lt }, status: "any" } },
    2,
    timeoutMs,
  );

  if (!result?.data) return [];

  const isHot = result.data.length >= HOT_CHUNK_THRESHOLD;
  const hasPagination = result.paginationToken !== null;
  const span = chunk.lt - chunk.gte;
  const canSplit = span > MIN_SLOT_SPAN && depth < 10;

  if ((isHot || hasPagination) && canSplit) {
    const mid = chunk.gte + (span >> 1);
    const [left, right] = await Promise.all([
      globalLimiter(() => fetchRetry(address, { gte: chunk.gte, lt: mid }, depth + 1)),
      globalLimiter(() => fetchRetry(address, { gte: mid, lt: chunk.lt }, depth + 1)),
    ]);
    return mergeSorted(left, right);
  }

  let txns = result.data;
  if (hasPagination) {
    let token: string | null = result.paginationToken;
    while (token) {
      const page: { data: FullTransaction[]; paginationToken: string | null } =
        await rpcCall<FullTransaction>(address, {
          ...BASE_CONFIG,
          paginationToken: token,
          filters: { slot: { gte: chunk.gte, lt: chunk.lt }, status: "any" },
        });
      if (!page?.data?.length) break;
      for (let i = 0; i < page.data.length; i++) txns.push(page.data[i]);
      token = page.paginationToken;
    }
  }

  return extract(txns, address);
}

async function fetchRetry(
  address: string,
  chunk: ChunkDef,
  depth: number,
): Promise<BalanceEntry[]> {
  try {
    return await fetchChunk(address, chunk, depth, REQUEST_TIMEOUT_MS);
  } catch {
    return fetchChunk(address, chunk, depth, REQUEST_TIMEOUT_MS * 2);
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
): Promise<BalanceEntry[]> {
  const rootLimiter = pLimit(concurrency);
  const completed: BalanceEntry[][] = [];

  await new Promise<void>((resolve, reject) => {
    let finished = 0;
    const total = chunks.length;
    if (total === 0) { resolve(); return; }

    for (const chunk of chunks) {
      rootLimiter(() => globalLimiter(() => fetchRetry(address, chunk, 0)))
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
