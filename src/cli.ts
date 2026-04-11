import { getSOLBalanceOverTime, getRpcCallCount, resetRpcCallCount } from "./index.js";

async function main() {
  const address = process.argv[2];
  if (!address) {
    console.error("Usage: npx tsx src/cli.ts <SOLANA_ADDRESS>");
    process.exit(1);
  }

  resetRpcCallCount();
  const start = performance.now();
  try {
    const history = await getSOLBalanceOverTime(address);
    const elapsed = performance.now() - start;
    console.log(JSON.stringify(history));
    console.error(`${history.length} entries | ${elapsed.toFixed(0)}ms | ${getRpcCallCount()} RPC calls`);
  } catch (err) {
    const elapsed = performance.now() - start;
    console.error(`Error after ${elapsed.toFixed(0)}ms (${getRpcCallCount()} calls):`, err);
    process.exit(1);
  }
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
