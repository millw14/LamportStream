import { Pool } from "undici";
import { RPC_URL, REQUEST_TIMEOUT_MS, MAX_RETRIES } from "./constants.js";
import type { RpcResponse, RpcRequestConfig } from "./types.js";

const url = new URL(RPC_URL);
const pool = new Pool(url.origin, {
  connections: 64,
  pipelining: 1,
  keepAliveTimeout: 30_000,
  keepAliveMaxTimeout: 60_000,
});

const pathWithQuery = url.pathname + url.search;

let rpcId = 0;
let _rpcCallCount = 0;

export function getRpcCallCount(): number { return _rpcCallCount; }
export function resetRpcCallCount(): void { _rpcCallCount = 0; }

function isRetryable(err: unknown): boolean {
  if (err instanceof Error) {
    const msg = err.message;
    if (msg.startsWith("HTTP 4")) return false;
    if (msg.startsWith("RPC error -32")) return false;
  }
  return true;
}

export async function rpcCall<T>(
  address: string,
  config: RpcRequestConfig,
  retries = MAX_RETRIES,
  timeoutMs = REQUEST_TIMEOUT_MS,
): Promise<{ data: T[]; paginationToken: string | null }> {
  const body = JSON.stringify({
    jsonrpc: "2.0",
    id: ++rpcId,
    method: "getTransactionsForAddress",
    params: [address, config],
  });

  for (let attempt = 0; attempt <= retries; attempt++) {
    try {
      _rpcCallCount++;
      const { statusCode, body: respBody } = await pool.request({
        path: pathWithQuery,
        method: "POST",
        headers: { "content-type": "application/json" },
        body,
        bodyTimeout: timeoutMs,
        headersTimeout: timeoutMs,
      });

      const text = await respBody.text();
      if (statusCode !== 200) {
        throw new Error(`HTTP ${statusCode}: ${text.slice(0, 200)}`);
      }
      const json = JSON.parse(text) as RpcResponse<T>;
      if (json.error) {
        throw new Error(`RPC error ${json.error.code}: ${json.error.message}`);
      }
      return json.result;
    } catch (err: unknown) {
      if (attempt === retries || !isRetryable(err)) throw err;
      await new Promise((r) => setTimeout(r, Math.min(100 * 2 ** attempt, 1000)));
    }
  }
  throw new Error("unreachable");
}
