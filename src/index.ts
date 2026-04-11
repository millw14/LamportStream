import { discover } from "./discovery.js";
import { classify } from "./classifier.js";
import { computeChunks } from "./chunker.js";
import { fetchAndProcess } from "./fetcher.js";
import { rpcCall, getRpcCallCount, resetRpcCallCount } from "./rpc.js";
import { FULL_TX_LIMIT } from "./constants.js";
import { WalletDensity } from "./types.js";
import type { BalanceEntry, FullTransaction } from "./types.js";

export { getRpcCallCount, resetRpcCallCount };

export async function getSOLBalanceOverTime(address: string): Promise<BalanceEntry[]> {
  const scout = await discover(address);
  if (scout.scoutCount === 0) return [];

  const classification = classify(scout);

  if (classification.density === WalletDensity.Sparse && !scout.hasMore && scout.scoutCount <= FULL_TX_LIMIT) {
    const result = await rpcCall<FullTransaction>(address, {
      transactionDetails: "full",
      sortOrder: "asc",
      limit: FULL_TX_LIMIT,
      commitment: "finalized",
      maxSupportedTransactionVersion: 0,
      encoding: "json",
      filters: { status: "any" },
    });
    return extractInline(result.data, address);
  }

  const chunks = computeChunks(scout, classification);
  return fetchAndProcess(address, chunks, classification.concurrency, classification.useSignatureHotBands);
}

function extractInline(txns: FullTransaction[], address: string): BalanceEntry[] {
  const out: BalanceEntry[] = [];
  for (let t = 0; t < txns.length; t++) {
    const tx = txns[t];
    if (!tx?.meta?.postBalances || !tx.transaction?.message?.accountKeys) continue;

    const keys = tx.transaction.message.accountKeys;
    let idx = -1;
    for (let i = 0; i < keys.length; i++) {
      const k = keys[i];
      if ((typeof k === "string" ? k : k.pubkey) === address) { idx = i; break; }
    }
    if (idx === -1) {
      const loaded = tx.meta.loadedAddresses;
      if (loaded) {
        const base = keys.length;
        for (let i = 0; i < loaded.writable.length; i++) {
          if (loaded.writable[i] === address) { idx = base + i; break; }
        }
        if (idx === -1) {
          for (let i = 0; i < loaded.readonly.length; i++) {
            if (loaded.readonly[i] === address) { idx = base + loaded.writable.length + i; break; }
          }
        }
      }
    }
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
