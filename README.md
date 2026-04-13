# LamportStream

Ultra-low latency SOL balance history computation using Helius `getTransactionsForAddress`.

This repo is a competition entry for the Helius mini Solana dev weekend challenge: compute SOL balance over time at runtime, with no indexing, using only RPC.

## Canonical file

The one file to submit, benchmark, or hand to Mert is:

- `lamport-stream.ts`

That file is the canonical artifact. The `src/` directory exists as support code for development, but the benchmark harness runs `lamport-stream.ts` directly.

## What it returns

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

Each row is the wallet's SOL balance immediately after a finalized transaction touching that address.

## Correctness semantics

LamportStream is explicit about history semantics:

- `commitment: "finalized"`
- `filters.status: "any"` by default
- failed transactions are included because they can still change SOL balance through fees
- ordering is preserved by `(slot, transactionIndex)`
- duplicate signatures are removed after merge
- versioned transactions are handled via `loadedAddresses`

If you want success-only history instead, the exported function accepts `includeFailedTransactions: false`.

## Why it is fast

LamportStream is optimized around the real bottleneck for this problem: minimizing slow RPC round trips while staying safe under dense wallet histories.

Key techniques:

- Compressed RPC responses (gzip, br, deflate via `node:zlib`)
- Bidirectional full-transaction discovery with overlap short-circuit
- Density-aware adaptive chunking (8 / 24 / 64 concurrency tiers, up to 128 chunks)
- Binary-split recursive subdivision (`MAX_SPLIT_DEPTH=8`) with linear pagination fallback
- Single `pLimit` concurrency control (`MAX_INFLIGHT=64`, undici pool `connections=128`)
- Stable `(slot, transactionIndex)` ordering with signature dedup
- v0 versioned tx support (`loadedAddresses` → `postBalances` index mapping)
- K-way merge over completed chunk outputs to reduce copy and GC overhead

## Project layout

```text
lamport-stream.ts   # canonical self-contained submission file
src/
  cli.ts            # secondary modular CLI runner
  index.ts          # secondary modular library entrypoint
  rpc.ts            # pooled HTTP transport + retry logic
  discovery.ts      # scout phase
  classifier.ts     # density classification
  chunker.ts        # adaptive slot-range planning
  fetcher.ts        # parallel fetch, recursive split, merge
  benchmark.ts      # benchmark matrix for the canonical file
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

Canonical submission file:

```bash
npm start -- <SOLANA_ADDRESS>
```

Direct TypeScript entry:

```bash
npx tsx lamport-stream.ts <SOLANA_ADDRESS>
```

Secondary modular runner:

```bash
npx tsx src/cli.ts <SOLANA_ADDRESS>
```

## Benchmark

Run the canonical submission file against sparse, periodic, and dense wallets:

```bash
npm run benchmark -- <sparse_addr> <periodic_addr> <dense_addr>
```

The benchmark sweeps:

- root concurrency: `40`, `48`, `56`
- inflight cap: `56`, `64`, `72`

The benchmark reports:

- elapsed time
- RPC call count
- duplicate signature detection
- stable ordering checks using `(slot, transactionIndex)`
- the best configuration across the provided wallet set

## Final submission config

Best config from benchmark sweeps:

- `rootConcurrencyOverride=56`, `maxInflightOverride=72`
- Benchmark median: ~2400ms avg across the three-wallet set

Reproducibility wallets:

| Label | Address |
|-------|---------|
| Sparse | `DpNXPNWvWoHaZ9P3WtfGCb2ZdLihW8VW1w1Ph4KDH9iG` |
| Periodic | `7EcDhSYGxXyscszYEp35KHN8vvw3svAuLKTzXwCFLtV` |
| Dense | `vines1vzrYbzLMRdu58ou5XTby4qAqVRLmqo36NKPTg` |

## Scripts

```bash
npm start -- <address>
npm run build
npm run benchmark -- <sparse> <periodic> <dense>
```

## License

No license file is included in this repository yet.
