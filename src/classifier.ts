import { WalletDensity } from "./types.js";
import type { ScoutResult } from "./types.js";

export interface ClassificationResult {
  density: WalletDensity;
  minChunks: number;
  maxChunks: number;
  concurrency: number;
}

export function classify(scout: ScoutResult): ClassificationResult {
  if (scout.scoutCount < 50 && !scout.hasMore) {
    return { density: WalletDensity.Sparse, minChunks: 4, maxChunks: 8, concurrency: 8 };
  }
  if (scout.scoutCount <= 500 && !scout.hasMore) {
    return { density: WalletDensity.Medium, minChunks: 16, maxChunks: 32, concurrency: 24 };
  }
  return { density: WalletDensity.Dense, minChunks: 48, maxChunks: 64, concurrency: 48 };
}
