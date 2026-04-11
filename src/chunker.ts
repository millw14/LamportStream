import { TARGET_TX_PER_CHUNK, MIN_CHUNKS, MAX_CHUNKS } from "./constants.js";
import type { ScoutResult, ChunkDef } from "./types.js";
import type { ClassificationResult } from "./classifier.js";

function appendRangeChunks(
  chunks: ChunkDef[],
  start: number,
  end: number,
  chunkBudget: number,
  density: number,
) {
  if (end <= start || chunkBudget <= 0) return;

  const step = Math.max(1, Math.ceil((end - start) / chunkBudget));
  for (let cursor = start; cursor < end; cursor += step) {
    const gte = cursor;
    const lt = Math.min(end, cursor + step);
    const span = lt - gte;
    const expectedTxns = density * span;

    if (expectedTxns > 90 && span > 1) {
      const mid = gte + (span >> 1);
      chunks.push({ gte, lt: mid });
      chunks.push({ gte: mid, lt });
    } else {
      chunks.push({ gte, lt });
    }
  }
}

export function computeChunks(
  scout: ScoutResult,
  classification: ClassificationResult,
): ChunkDef[] {
  const totalSlotRange = scout.maxSlot - scout.minSlot;
  if (totalSlotRange <= 0) return [{ gte: scout.minSlot, lt: scout.maxSlot + 1 }];

  const oldestDensity = scout.scoutSlotRange > 0
    ? scout.scoutCount / scout.scoutSlotRange
    : 0.0001;
  const midDensity = scout.midScoutCount && scout.midScoutSlotRange
    ? scout.midScoutCount / scout.midScoutSlotRange
    : 0;
  const referenceDensity = Math.max(oldestDensity, midDensity || 0);

  const chunkSlotSize = Math.max(
    1,
    Math.floor(TARGET_TX_PER_CHUNK / Math.max(referenceDensity, 0.000001)),
  );

  let numChunks = Math.ceil(totalSlotRange / chunkSlotSize);
  numChunks = Math.max(classification.minChunks, Math.min(classification.maxChunks, numChunks));
  numChunks = Math.max(MIN_CHUNKS, Math.min(MAX_CHUNKS, numChunks));

  const actualChunkSize = Math.ceil(totalSlotRange / numChunks);
  const chunks: ChunkDef[] = [];

  const scoutEndSlot = scout.minSlot + scout.scoutSlotRange;
  const midStartSlot = scout.midScoutMinSlot ?? 0;
  const midEndSlot = scout.midScoutMaxSlot ? scout.midScoutMaxSlot + 1 : 0;
  const hasMidWindow = (scout.midScoutCount ?? 0) > 0 && midEndSlot > midStartSlot;
  const expectedDensityOutside = Math.max(oldestDensity * (scout.hasMore ? 0.15 : 0.5), 0.000001);

  if (
    classification.density === "dense" &&
    hasMidWindow &&
    scout.scoutSlotRange > 0 &&
    totalSlotRange > scout.scoutSlotRange * 2
  ) {
    const oldestBudget = Math.max(12, Math.round(classification.maxChunks * 0.2));
    const midBudget = Math.max(18, Math.round(classification.maxChunks * 0.35));
    const outsideBudget = Math.max(8, classification.maxChunks - oldestBudget - midBudget);
    const beforeMidRange = Math.max(0, midStartSlot - scoutEndSlot);
    const afterMidRange = Math.max(0, scout.maxSlot + 1 - midEndSlot);
    const outsideRange = beforeMidRange + afterMidRange;
    const beforeMidBudget = outsideRange > 0 ? Math.round(outsideBudget * (beforeMidRange / outsideRange)) : 0;
    const afterMidBudget = outsideBudget - beforeMidBudget;

    appendRangeChunks(chunks, scout.minSlot, scoutEndSlot, oldestBudget, oldestDensity);
    appendRangeChunks(chunks, scoutEndSlot, midStartSlot, beforeMidBudget, expectedDensityOutside);
    appendRangeChunks(chunks, midStartSlot, midEndSlot, midBudget, midDensity);
    appendRangeChunks(chunks, midEndSlot, scout.maxSlot + 1, afterMidBudget, expectedDensityOutside);
    return chunks;
  }

  for (let i = 0; i < numChunks; i++) {
    const gte = scout.minSlot + i * actualChunkSize;
    const lt = i === numChunks - 1
      ? scout.maxSlot + 1
      : scout.minSlot + (i + 1) * actualChunkSize;
    if (gte >= scout.maxSlot + 1) break;

    const chunkSpan = lt - gte;
    const isInScoutRange = gte < scoutEndSlot;
    const isInMidRange = hasMidWindow && gte >= midStartSlot && gte < midEndSlot;
    const localDensity = isInMidRange
      ? midDensity
      : isInScoutRange
        ? oldestDensity
        : expectedDensityOutside;
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
