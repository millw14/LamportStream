import {
  getSOLBalanceOverTime,
  getRpcCallCount,
  resetRpcCallCount,
  type LamportStreamOptions,
} from "../lamport-stream";

const TEST_WALLETS: { label: string; address: string }[] = [
  // Replace with real addresses for your testing
  { label: "sparse  (~few txns)",     address: process.argv[2] || "" },
  { label: "periodic (burst pattern)", address: process.argv[3] || "" },
  { label: "dense   (bot/trader)",     address: process.argv[4] || "" },
];

interface RunResult {
  config: string;
  label: string;
  address: string;
  entries: number;
  rpcCalls: number;
  elapsedMs: number;
  orderingOk: boolean;
  duplicateCount: number;
  error?: string;
}

function classifyError(err: unknown): string {
  const text = String(err instanceof Error ? err.message : err);
  if (text.includes("HELIUS_API_KEY")) return "missing_api_key";
  if (text.includes("HTTP 403")) return "auth_or_plan_error";
  if (text.includes("HTTP")) return "http_error";
  if (text.includes("RPC error")) return "rpc_error";
  return text.slice(0, 80);
}

async function benchOne(
  config: string,
  label: string,
  address: string,
  options: LamportStreamOptions,
): Promise<RunResult> {
  if (!address) {
    return {
      config,
      label,
      address: "(skipped)",
      entries: 0,
      rpcCalls: 0,
      elapsedMs: 0,
      orderingOk: true,
      duplicateCount: 0,
      error: "no_address",
    };
  }

  resetRpcCallCount();
  const start = performance.now();
  try {
    const history = await getSOLBalanceOverTime(address, options);
    const elapsed = performance.now() - start;
    const calls = getRpcCallCount();

    let ordered = true;
    const sigs = new Set<string>();
    for (let i = 0; i < history.length; i++) {
      if (i > 0) {
        const prev = history[i - 1];
        const curr = history[i];
        if (
          curr.slot < prev.slot ||
          (curr.slot === prev.slot && curr.transactionIndex < prev.transactionIndex)
        ) {
          ordered = false;
        }
      }
      sigs.add(history[i].signature);
    }
    const dupes = history.length - sigs.size;

    return {
      config,
      label,
      address: address.slice(0, 8) + "...",
      entries: history.length,
      rpcCalls: calls,
      elapsedMs: Math.round(elapsed),
      orderingOk: ordered,
      duplicateCount: dupes,
      error: !ordered ? "ordering_error" : dupes > 0 ? "duplicate_signatures" : undefined,
    };
  } catch (err: unknown) {
    const elapsed = performance.now() - start;
    return {
      config,
      label,
      address: address.slice(0, 8) + "...",
      entries: 0,
      rpcCalls: getRpcCallCount(),
      elapsedMs: Math.round(elapsed),
      orderingOk: true,
      duplicateCount: 0,
      error: classifyError(err),
    };
  }
}

async function main() {
  const active = TEST_WALLETS.filter((w) => w.address);
  if (active.length === 0) {
    console.error("Usage: npx tsx src/benchmark.ts <sparse_addr> [periodic_addr] [dense_addr]");
    process.exit(1);
  }

  console.error("=== LamportStream Benchmark ===\n");

  const rootConcurrencyValues = [40, 48, 56];
  const maxInflightValues = [56, 64, 72];
  const allResults: RunResult[] = [];

  for (const rootConcurrencyOverride of rootConcurrencyValues) {
    for (const maxInflightOverride of maxInflightValues) {
      const options: LamportStreamOptions = {
        rootConcurrencyOverride,
        maxInflightOverride,
      };
      const config = `root=${rootConcurrencyOverride}, inflight=${maxInflightOverride}`;
      console.error(`Config: ${config}`);
      const configResults: RunResult[] = [];

      for (const wallet of active) {
        console.error(`  Running: ${wallet.label} (${wallet.address.slice(0, 12)}...)`);
        const result = await benchOne(config, wallet.label, wallet.address, options);
        configResults.push(result);
        console.error(`    -> ${result.entries} entries | ${result.elapsedMs}ms | ${result.rpcCalls} RPC calls${result.error ? " | " + result.error : ""}`);
      }

      const totalMs = configResults.reduce((s, r) => s + r.elapsedMs, 0);
      const totalCalls = configResults.reduce((s, r) => s + r.rpcCalls, 0);
      const successfulRuns = configResults.filter((r) => !r.error).length;
      const avgMs = successfulRuns > 0 ? Math.round(totalMs / successfulRuns) : null;
      const orderingFailures = configResults.filter((r) => !r.orderingOk).length;
      const duplicateTotal = configResults.reduce((s, r) => s + r.duplicateCount, 0);

      console.error(`  Summary -> avg ${avgMs === null ? "n/a" : `${avgMs}ms`} | total ${totalMs}ms | ${totalCalls} RPC calls | order ${orderingFailures === 0 ? "ok" : orderingFailures} | dupes ${duplicateTotal}\n`);
      allResults.push(...configResults);
    }
  }

  const grouped = new Map<string, RunResult[]>();
  for (const result of allResults) {
    const bucket = grouped.get(result.config) ?? [];
    bucket.push(result);
    grouped.set(result.config, bucket);
  }

  let bestConfig = "";
  let bestAvgMs = Number.POSITIVE_INFINITY;
  for (const [config, results] of grouped.entries()) {
    const totalMs = results.reduce((s, r) => s + r.elapsedMs, 0);
    const successfulRuns = results.filter((r) => !r.error).length;
    const avgMs = successfulRuns > 0 ? Math.round(totalMs / successfulRuns) : Number.POSITIVE_INFINITY;
    if (avgMs < bestAvgMs) {
      bestAvgMs = avgMs;
      bestConfig = config;
    }
  }

  console.error("--- Summary ---");
  console.error(`Configs: ${grouped.size}`);
  console.error(`Best:    ${bestConfig || "none (no successful configs)"}`);
  console.error(`BestAvg: ${Number.isFinite(bestAvgMs) ? bestAvgMs : 0}ms per wallet`);

  console.log(JSON.stringify(allResults, null, 2));
}

const keepAlive = setInterval(() => {}, 1000);
main()
  .catch((err) => {
    console.error(err);
    process.exitCode = 1;
  })
  .finally(() => {
    clearInterval(keepAlive);
  });
