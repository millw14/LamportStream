import { rpcCall } from "./rpc.js";
import { SCOUT_LIMIT } from "./constants.js";
import type { SignatureEntry, ScoutResult } from "./types.js";

export async function discover(address: string): Promise<ScoutResult> {
  const [newestResult, scoutResult] = await Promise.all([
    rpcCall<SignatureEntry>(address, {
      transactionDetails: "signatures",
      sortOrder: "desc",
      limit: 1,
      maxSupportedTransactionVersion: 0,
    }),
    rpcCall<SignatureEntry>(address, {
      transactionDetails: "signatures",
      sortOrder: "asc",
      limit: SCOUT_LIMIT,
      maxSupportedTransactionVersion: 0,
    }),
  ]);

  if (newestResult.data.length === 0 || scoutResult.data.length === 0) {
    return { minSlot: 0, maxSlot: 0, scoutCount: 0, scoutSlotRange: 0, hasMore: false };
  }

  const maxSlot = newestResult.data[0].slot;
  const minSlot = scoutResult.data[0].slot;
  const scoutCount = scoutResult.data.length;
  const lastScoutSlot = scoutResult.data[scoutCount - 1].slot;
  const scoutSlotRange = lastScoutSlot - minSlot + 1;
  const hasMore = scoutResult.paginationToken !== null;

  return { minSlot, maxSlot, scoutCount, scoutSlotRange, hasMore };
}
