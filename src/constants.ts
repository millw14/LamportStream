const HELIUS_API_KEY_FROM_ENV = process.env.HELIUS_API_KEY;

if (!HELIUS_API_KEY_FROM_ENV) {
  throw new Error("HELIUS_API_KEY environment variable is required.");
}

export const HELIUS_API_KEY = HELIUS_API_KEY_FROM_ENV;
export const RPC_URL = `https://mainnet.helius-rpc.com/?api-key=${HELIUS_API_KEY}`;

export const HOT_CHUNK_THRESHOLD = 65;
export const TARGET_TX_PER_CHUNK = 50;
export const MIN_CHUNKS = 8;
export const MAX_CHUNKS = 64;
export const MAX_INFLIGHT = 64;
export const MIN_SLOT_SPAN = 4;
export const REQUEST_TIMEOUT_MS = 5000;
export const MAX_RETRIES = 2;

export const SCOUT_LIMIT = 1000;
export const FULL_TX_LIMIT = 100;
