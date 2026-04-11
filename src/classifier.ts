import { WalletDensity } from "./types.js";
import type { ScoutResult } from "./types.js";

export interface ClassificationResult {
  density: WalletDensity;
  minChunks: number;
  maxChunks: number;
  concurrency: number;
}

function estimateReferenceDensity(scout: ScoutResult): number {
  const oldestDensity = scout.scoutSlotRange > 0
    ? scout.scoutCount / scout.scoutSlotRange
    : 0.000001;
  const midDensity = scout.midScoutCount && scout.midScoutSlotRange
    ? scout.midScoutCount / scout.midScoutSlotRange
    : 0;
  return Math.max(oldestDensity, midDensity || 0);
}

export function classify(scout: ScoutResult): ClassificationResult {
  const totalSlotRange = Math.max(1, scout.maxSlot - scout.minSlot + 1);
  const referenceDensity = estimateReferenceDensity(scout);
  const sampledTx = scout.scoutCount + (scout.midScoutCount ?? 0);
  const sampledRange = scout.scoutSlotRange + (scout.midScoutSlotRange ?? 0);
  const blendedDensity = sampledRange > 0 ? sampledTx / sampledRange : referenceDensity;
  const estimatedTotalTx = Math.max(blendedDensity, 0.000001) * totalSlotRange;

  if (!scout.hasMore && scout.scoutCount < 50) {
    return { density: WalletDensity.Sparse, minChunks: 4, maxChunks: 8, concurrency: 8 };
  }

  if (estimatedTotalTx <= 250 && scout.scoutCount < 100) {
    return { density: WalletDensity.Sparse, minChunks: 4, maxChunks: 8, concurrency: 8 };
  }

  if (estimatedTotalTx <= 3000 && referenceDensity < 0.01) {
    return { density: WalletDensity.Medium, minChunks: 16, maxChunks: 32, concurrency: 24 };
  }

  return { density: WalletDensity.Dense, minChunks: 48, maxChunks: 64, concurrency: 48 };
}
