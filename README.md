# LamportStream

Ultra-low latency SOL balance history computation using Helius `getTransactionsForAddress`.

This repo is a competition entry for the Helius mini Solana dev weekend challenge: compute SOL balance over time at runtime, with no indexing, using only RPC.

## What it does

Given a wallet address, LamportStream returns a chronologically ordered balance history:

```json
[
  {
    "slot": 348219021,
    "transactionIndex": 17,
    "signature": "5Lx...",
    "blockTime": 1744461123,
    "balanceLamports": 182340000
  }
]
```

Each row represents the wallet's SOL balance immediately after a transaction touching that wallet.

## Why it is fast

LamportStream is optimized around the real bottleneck for this problem: minimizing slow RPC round trips while staying safe under dense wallet histories.

Key techniques:

- 2-call parallel discovery: newest slot + ascending scout pass
- Density-aware adaptive chunking
- Adaptive root concurrency: `8 / 24 / 48`
- Proactive hot-range pre-splitting from scout density
- Recursive hot-chunk subdivision before pagination becomes expensive
- Global `MAX_INFLIGHT=64` guard
- Undici connection pooling with keep-alive
- Stable merge ordering by `(slot, transactionIndex)`
- Versioned transaction support via `loadedAddresses`
- Sparse wallet fast-path: skip chunking for tiny histories

## Project layout

```text
lamport-stream.ts   # self-contained, gist-ready submission file
src/
  cli.ts            # command-line runner
  index.ts          # library entrypoint
  rpc.ts            # pooled HTTP transport + retry logic
  discovery.ts      # scout phase
  classifier.ts     # density classification
  chunker.ts        # adaptive slot-range planning
  fetcher.ts        # parallel fetch, recursive split, merge
  benchmark.ts      # benchmark and validation harness
  types.ts          # shared types
  constants.ts      # tuning knobs
```

## Requirements

- Node.js 22+
- A Helius API key with access to `getTransactionsForAddress`

Set the environment variable before running:

```powershell
$env:HELIUS_API_KEY="your_api_key_here"
```

## Install

```bash
npm install
```

## Run

Modular CLI:

```bash
npm start -- <SOLANA_ADDRESS>
```

Direct TypeScript entry:

```bash
npx tsx src/cli.ts <SOLANA_ADDRESS>
```

Gist-ready standalone file:

```bash
npx tsx lamport-stream.ts <SOLANA_ADDRESS>
```

## Benchmark

Run against sparse, periodic, and dense wallets:

```bash
npm run benchmark -- <sparse_addr> <periodic_addr> <dense_addr>
```

The benchmark reports:

- total elapsed time
- RPC call count
- duplicate signature detection
- stable ordering checks using `(slot, transactionIndex)`

## Implementation notes

### Correctness

- Preserves intra-slot ordering using `transactionIndex`
- Handles versioned transactions with address lookup tables
- Deduplicates by signature after merge
- Avoids CLI side effects when importing library code

### Performance

- Avoids binary search discovery
- Prevents recursive limiter starvation on dense wallets
- Keeps chunk sizing below the expensive pagination boundary when possible
- Fails fast on missing API key and non-retryable errors

## Submission file

If you want to submit just one file, use:

- `lamport-stream.ts`

It contains the full algorithm in a single self-contained file suitable for a gist or tweet reply.

## Scripts

```bash
npm start -- <address>
npm run build
npm run benchmark -- <sparse> <periodic> <dense>
```

## License

No license file is included in this repository yet.
