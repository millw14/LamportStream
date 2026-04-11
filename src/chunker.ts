import { TARGET_TX_PER_CHUNK, MIN_CHUNKS, MAX_CHUNKS } from "./constants.js";
import type { ScoutResult, ChunkDef } from "./types.js";
import type { ClassificationResult } from "./classifier.js";

export function computeChunks(
  scout: ScoutResult,
  classification: ClassificationResult,
): ChunkDef[] {
  const totalSlotRange = scout.maxSlot - scout.minSlot;
  if (totalSlotRange <= 0) return [{ gte: scout.minSlot, lt: scout.maxSlot + 1 }];

  const estimatedTxPerSlot = scout.scoutSlotRange > 0
    ? scout.scoutCount / scout.scoutSlotRange
    : 0.0001;

  const chunkSlotSize = Math.max(
    1,
    Math.floor(TARGET_TX_PER_CHUNK / Math.max(estimatedTxPerSlot, 0.000001)),
  );

  let numChunks = Math.ceil(totalSlotRange / chunkSlotSize);
  numChunks = Math.max(classification.minChunks, Math.min(classification.maxChunks, numChunks));
  numChunks = Math.max(MIN_CHUNKS, Math.min(MAX_CHUNKS, numChunks));

  const actualChunkSize = Math.ceil(totalSlotRange / numChunks);
  const chunks: ChunkDef[] = [];

  const scoutEndSlot = scout.minSlot + scout.scoutSlotRange;
  const scoutDensity = scout.scoutSlotRange > 0 ? scout.scoutCount / scout.scoutSlotRange : 0;
  const restSlotRange = scout.maxSlot - scoutEndSlot;
  const expectedDensityOutside = scout.hasMore ? scoutDensity * 1.5 : scoutDensity * 0.3;

  for (let i = 0; i < numChunks; i++) {
    const gte = scout.minSlot + i * actualChunkSize;
    const lt = i === numChunks - 1
      ? scout.maxSlot + 1
      : scout.minSlot + (i + 1) * actualChunkSize;
    if (gte >= scout.maxSlot + 1) break;

    const chunkSpan = lt - gte;
    const isInScoutRange = gte < scoutEndSlot;
    const localDensity = isInScoutRange ? scoutDensity : expectedDensityOutside;
    const expectedTxns = localDensity * chunkSpan;

    if (expectedTxns > 90 && chunkSpan > 1) {
      const mid = gte + (chunkSpan >> 1);
      chunks.push({ gte, lt: mid });
      chunks.push({ gte: mid, lt });
    } else {
      chunks.push({ gte, lt });
    }
  }

  return chunks;
}
