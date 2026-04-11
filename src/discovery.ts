import { rpcCall } from "./rpc.js";
import { SCOUT_LIMIT } from "./constants.js";
import type { SignatureEntry, ScoutResult } from "./types.js";

export async function discover(address: string): Promise<ScoutResult> {
  const [newestResult, scoutResult] = await Promise.all([
    rpcCall<SignatureEntry>(address, {
      transactionDetails: "signatures",
      sortOrder: "desc",
      limit: 1,
      commitment: "finalized",
      maxSupportedTransactionVersion: 0,
      filters: { status: "any" },
    }),
    rpcCall<SignatureEntry>(address, {
      transactionDetails: "signatures",
      sortOrder: "asc",
      limit: SCOUT_LIMIT,
      commitment: "finalized",
      maxSupportedTransactionVersion: 0,
      filters: { status: "any" },
    }),
  ]);

  if (newestResult.data.length === 0 || scoutResult.data.length === 0) {
    return {
      minSlot: 0,
      maxSlot: 0,
      scoutCount: 0,
      scoutSlotRange: 0,
      hasMore: false,
      midScoutCount: 0,
      midScoutSlotRange: 0,
      midScoutMinSlot: 0,
      midScoutMaxSlot: 0,
    };
  }

  const maxSlot = newestResult.data[0].slot;
  const minSlot = scoutResult.data[0].slot;
  const scoutCount = scoutResult.data.length;
  const lastScoutSlot = scoutResult.data[scoutCount - 1].slot;
  const scoutSlotRange = lastScoutSlot - minSlot + 1;
  const hasMore = scoutResult.paginationToken !== null;
  let midScoutCount = 0;
  let midScoutSlotRange = 0;
  let midScoutMinSlot = 0;
  let midScoutMaxSlot = 0;

  if (hasMore || scoutCount >= 200) {
    const midpoint = minSlot + Math.floor((maxSlot - minSlot) / 2);
    const halfWindow = Math.max(1, Math.floor(scoutSlotRange / 2));
    const midStart = Math.max(minSlot, midpoint - halfWindow);
    const midEnd = Math.min(maxSlot + 1, midpoint + halfWindow);
    const midResult = await rpcCall<SignatureEntry>(address, {
      transactionDetails: "signatures",
      sortOrder: "asc",
      limit: SCOUT_LIMIT,
      commitment: "finalized",
      maxSupportedTransactionVersion: 0,
      filters: {
        status: "any",
        slot: { gte: midStart, lt: midEnd },
      },
    });

    if (midResult.data.length > 0) {
      midScoutCount = midResult.data.length;
      midScoutMinSlot = midResult.data[0].slot;
      midScoutMaxSlot = midResult.data[midResult.data.length - 1].slot;
      midScoutSlotRange = midResult.data[midResult.data.length - 1].slot - midResult.data[0].slot + 1;
    }
  }

  return {
    minSlot,
    maxSlot,
    scoutCount,
    scoutSlotRange,
    hasMore,
    midScoutCount,
    midScoutSlotRange,
    midScoutMinSlot,
    midScoutMaxSlot,
  };
}
