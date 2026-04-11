export enum WalletDensity {
  Sparse = "sparse",
  Medium = "medium",
  Dense = "dense",
}

export interface SlotFilter {
  gte?: number;
  gt?: number;
  lte?: number;
  lt?: number;
}

export interface RpcRequestConfig {
  transactionDetails?: "signatures" | "full";
  sortOrder?: "desc" | "asc";
  limit?: number;
  paginationToken?: string;
  commitment?: "finalized" | "confirmed";
  encoding?: "json" | "jsonParsed" | "base64" | "base58";
  maxSupportedTransactionVersion?: number;
  filters?: {
    slot?: SlotFilter;
    blockTime?: { gte?: number; gt?: number; lte?: number; lt?: number; eq?: number };
    signature?: { gte?: string; gt?: string; lte?: string; lt?: string };
    status?: "succeeded" | "failed" | "any";
    tokenAccounts?: "none" | "balanceChanged" | "all";
  };
}

export interface RpcRequest {
  jsonrpc: "2.0";
  id: number;
  method: "getTransactionsForAddress";
  params: [string, RpcRequestConfig];
}

export interface TransactionMeta {
  err: unknown;
  fee: number;
  preBalances: number[];
  postBalances: number[];
  preTokenBalances?: unknown[];
  postTokenBalances?: unknown[];
  logMessages?: string[];
  loadedAddresses?: {
    writable: string[];
    readonly: string[];
  };
  [key: string]: unknown;
}

export interface TransactionData {
  signatures: string[];
  message: {
    accountKeys: (string | { pubkey: string; signer: boolean; writable: boolean })[];
    [key: string]: unknown;
  };
  [key: string]: unknown;
}

export interface FullTransaction {
  transactionIndex: number;
  slot: number;
  blockTime: number | null;
  transaction: TransactionData;
  meta: TransactionMeta;
  [key: string]: unknown;
}

export interface SignatureEntry {
  slot: number;
  signature: string;
  blockTime: number | null;
  err: unknown;
}

export interface RpcResponse<T> {
  jsonrpc: string;
  id: number;
  result: {
    data: T[];
    paginationToken: string | null;
  };
  error?: { code: number; message: string };
}

export interface ChunkDef {
  gte: number;
  lt: number;
}

export interface ScoutResult {
  minSlot: number;
  maxSlot: number;
  scoutCount: number;
  scoutSlotRange: number;
  hasMore: boolean;
  midScoutCount?: number;
  midScoutSlotRange?: number;
}

export interface BalanceEntry {
  slot: number;
  transactionIndex: number;
  signature: string;
  blockTime: number | null;
  balanceLamports: number;
}
