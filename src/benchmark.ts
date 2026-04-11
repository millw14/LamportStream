import { getSOLBalanceOverTime, getRpcCallCount, resetRpcCallCount } from "./index.js";

const TEST_WALLETS: { label: string; address: string }[] = [
  // Replace with real addresses for your testing
  { label: "sparse  (~few txns)",     address: process.argv[2] || "" },
  { label: "periodic (burst pattern)", address: process.argv[3] || "" },
  { label: "dense   (bot/trader)",     address: process.argv[4] || "" },
];

interface RunResult {
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

async function benchOne(label: string, address: string): Promise<RunResult> {
  if (!address) {
    return {
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
    const history = await getSOLBalanceOverTime(address);
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
      label,
      address: address.slice(0, 8) + "...",
      entries: 0,
      rpcCalls: getRpcCallCount(),
      elapsedMs: Math.round(elapsed),
      orderingOk: false,
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

  const results: RunResult[] = [];
  for (const wallet of active) {
    console.error(`Running: ${wallet.label} (${wallet.address.slice(0, 12)}...)`);
    const result = await benchOne(wallet.label, wallet.address);
    results.push(result);
    console.error(`  -> ${result.entries} entries | ${result.elapsedMs}ms | ${result.rpcCalls} RPC calls${result.error ? " | " + result.error : ""}\n`);
  }

  const totalMs = results.reduce((s, r) => s + r.elapsedMs, 0);
  const successfulRuns = results.filter((r) => !r.error || r.entries > 0).length;
  const avgMs = successfulRuns > 0 ? Math.round(totalMs / successfulRuns) : 0;
  const totalCalls = results.reduce((s, r) => s + r.rpcCalls, 0);
  const orderingFailures = results.filter((r) => !r.orderingOk).length;
  const duplicateTotal = results.reduce((s, r) => s + r.duplicateCount, 0);

  console.error("--- Summary ---");
  console.error(`Total:   ${totalMs}ms across ${results.length} wallets`);
  console.error(`Average: ${avgMs}ms per wallet`);
  console.error(`RPC:     ${totalCalls} total calls`);
  console.error(`Order:   ${orderingFailures === 0 ? "ok" : `${orderingFailures} failures`}`);
  console.error(`Dupes:   ${duplicateTotal}`);

  console.log(JSON.stringify(results, null, 2));
}

main();
