import { Duration } from "./core";
import { Serdes, AnySerdes } from "../utils/serdes/serdes";
import { durationToSeconds } from "../utils/duration/duration";
// Type-only import to avoid a runtime circular dependency with errors/durable-error.
import type { DistributedMapItemError } from "../errors/durable-error/durable-error";

/** Terminal status of a distributed map run. @public */
export enum DistributedMapStatus {
  SUCCEEDED = "SUCCEEDED",
  FAILED = "FAILED",
  STOPPED = "STOPPED",
  TIMED_OUT = "TIMED_OUT",
  // Client-side sentinel for an unrecognized status from backend.
  UNKNOWN_TO_SDK_VERSION = "UNKNOWN_TO_SDK_VERSION",
}

/** Status of a single item in a collected distributed map result. @public */
export enum DistributedMapItemStatus {
  SUCCEEDED = "SUCCEEDED",
  FAILED = "FAILED",
  // Client-side sentinel for an unrecognized status from backend.
  UNKNOWN_TO_SDK_VERSION = "UNKNOWN_TO_SDK_VERSION",
}

/** Valid completion reasons for a distributed map run. @internal */
export const DISTRIBUTED_MAP_COMPLETION_REASONS = [
  "ALL_COMPLETED",
  "ITEM_LIMIT_REACHED",
  "STOPPED",
  "TIMED_OUT",
  "FAILURE_TOLERANCE_EXCEEDED",
  "SOURCE_FAILED",
  "DESTINATION_FAILED",
  "INLINE_RESULT_LIMIT_EXCEEDED",
  "INVALID_CONFIGURATION",
  "QUOTA_EXCEEDED",
  "KMS_ACCESS_DENIED",
  "INTERNAL_ERROR",
  // Client-side sentinel for an unrecognized reason from backend.
  "UNKNOWN_TO_SDK_VERSION",
] as const;

/** Reason a distributed map run reached its terminal status. @public */
export type DistributedMapCompletionReason =
  (typeof DISTRIBUTED_MAP_COMPLETION_REASONS)[number];

/** CSV field delimiter. @public */
export type CsvDelimiter = "COMMA" | "PIPE" | "SEMICOLON" | "SPACE" | "TAB";

/** Failure tolerance for the run, count and percentage are mutually exclusive. @public */
export type DistributedMapCompletionConfig =
  | {
      toleratedFailureCount: number;
      toleratedFailurePercentage?: never;
      minimumSampleSize?: never;
    }
  | {
      toleratedFailurePercentage: number;
      minimumSampleSize?: number;
      toleratedFailureCount?: never;
    };

/** Items supplied inline in the operation request. @public */
export interface DistributedMapInlineSourceConfig<TItem> {
  /** Items to process. The serialized array must be at most 1 MB. */
  items: TItem[];
  /** Serializes each item. Must match the item serdes the processor uses. Defaults to JSON. */
  serdes?: Serdes<TItem>;
}

/** CSV parsing options for an S3 source. @public */
export interface DistributedMapCsvConfig {
  /** Column names for a file with no header row. The first row is read as data. */
  headers?: readonly string[];
  /**
   * Column names of a file whose first row is a header row. Mutually exclusive with headers.
   * The service never receives them. A missing column reads as null.
   *
   * @example Type the row in the processor from the same names
   * ```ts
   * const columns = ["id", "name"] as const;
   * type OrderRow = { [K in (typeof columns)[number]]: string | null };
   * ```
   */
  expectedColumns?: readonly string[];
  /** Field delimiter. Defaults to COMMA. */
  delimiter?: CsvDelimiter;
}

/**
 * Items read from S3. Set exactly one of key or prefix.
 *
 * With key, the service reads one object and splits it into items by format.
 * With prefix and a format, the service reads every object under the prefix and splits each
 * one into items. With prefix and no format, each object's key and metadata is one item.
 *
 * @public
 */
export interface DistributedMapS3SourceConfig {
  bucket: string;
  /** Key of the one object to read. */
  key?: string;
  /** Prefix of the objects to read. */
  prefix?: string;
  /** How object content is split into items. Required with key. */
  format?: "JSON_LINES" | "JSON_ARRAY" | "CSV";
  /** CSV parsing options. Only valid when format is CSV. */
  csv?: DistributedMapCsvConfig;
  /** Account ID that must own the bucket, as exactly 12 digits. */
  expectedBucketOwner?: string;
}

/** Items paginated by a reader Lambda function. @public */
export interface DistributedMapReaderSourceConfig {
  functionName: string;
  /** State passed to the reader's first call. */
  initialState?: unknown;
  /** Serializes initialState. Must match the state serdes the reader uses. Defaults to JSON. */
  stateSerdes?: AnySerdes;
}

/** Where a distributed map run reads its items. Set exactly one of inline, s3 or reader. @public */
export interface DistributedMapSourceConfig<TItem> {
  inline?: DistributedMapInlineSourceConfig<TItem>;
  s3?: DistributedMapS3SourceConfig;
  reader?: DistributedMapReaderSourceConfig;
  /** Maximum number of items the service reads from the source. */
  maxItems?: number;
}

/** The Lambda function that processes batches of items. @public */
export interface DistributedMapProcessorConfig {
  functionName: string;
  /**
   * What the processor reports back. "batch" reports one outcome for the whole batch.
   * "itemFailures" reports the failed items. "itemResults" reports every item's output or error.
   * Defaults to "batch".
   */
  responseMode?: "batch" | "itemFailures" | "itemResults";
  /** Items per processor call, from 1 through 10000. */
  batchSize?: number;
  /** Retries per batch, a non-negative integer or "unlimited". 0 disables retries. Defaults to 2. */
  maxRetryAttempts?: number | "unlimited";
  /** Total retry time per batch, from 1 minute through 6 hours. Defaults to 6 hours. */
  maxRetryDuration?: Duration;
  /** Name prefix for durable processor executions, 1 to 36 characters. */
  durableExecutionNamePrefix?: string;
}

/** S3 destination for succeeded items. @public */
export interface DistributedMapSuccessDestinationConfig {
  bucket: string;
  prefix: string;
  /** Write each item's input. Defaults to false. */
  includeInput?: boolean;
  /** Write each item's output. Defaults to true. */
  includeOutput?: boolean;
  /** Account ID that must own the bucket, as exactly 12 digits. */
  expectedBucketOwner?: string;
}

/** S3 destination for failed items. @public */
export interface DistributedMapFailureDestinationConfig {
  bucket: string;
  prefix: string;
  /** Write each item's input. Defaults to true. */
  includeInput?: boolean;
  /** Write each item's error. Defaults to true. */
  includeError?: boolean;
  /** Account ID that must own the bucket, as exactly 12 digits. */
  expectedBucketOwner?: string;
}

/** Where the service writes item input, output, and error records. @public */
export interface DistributedMapDestinationConfig {
  onSuccess?: DistributedMapSuccessDestinationConfig;
  onFailure?: DistributedMapFailureDestinationConfig;
}

const TWELVE_DIGITS = /^[0-9]{12}$/;

function assertBucketOwner(owner?: string): void {
  if (owner !== undefined && !TWELVE_DIGITS.test(owner)) {
    throw new Error("expectedBucketOwner must be exactly 12 digits");
  }
}

function assertBucket(bucket: unknown): void {
  if (typeof bucket !== "string" || !bucket)
    throw new Error("bucket must be a non-empty string");
}

/** The service enforces the full Lambda name grammar. The SDK only bounds length. */
const FUNCTION_NAME_MAX = 170;

function assertFunctionName(name: unknown): void {
  if (typeof name !== "string" || !name)
    throw new Error("function name must be non-empty");
  if (name.length > FUNCTION_NAME_MAX) {
    throw new Error(
      `function name must be at most ${FUNCTION_NAME_MAX} characters`,
    );
  }
}

/** Validate a completion config's numeric bounds and mutual-exclusion rules. @internal */
export function assertCompletionConfig(
  c?: DistributedMapCompletionConfig,
): void {
  if (!c) return;
  const count = c.toleratedFailureCount;
  const pct = c.toleratedFailurePercentage;
  const sample = c.minimumSampleSize;
  if (count !== undefined && pct !== undefined) {
    throw new Error(
      "toleratedFailureCount and toleratedFailurePercentage are mutually exclusive",
    );
  }
  if (sample !== undefined && pct === undefined) {
    throw new Error(
      "minimumSampleSize is only valid with toleratedFailurePercentage",
    );
  }
  if (count !== undefined && (!Number.isInteger(count) || count < 0)) {
    throw new Error(
      `Invalid toleratedFailureCount: ${count}. Must be a non-negative integer.`,
    );
  }
  if (pct !== undefined && (pct < 0 || pct > 100)) {
    throw new Error(
      `Invalid toleratedFailurePercentage: ${pct}. Must be from 0 through 100.`,
    );
  }
  if (sample !== undefined && (!Number.isInteger(sample) || sample < 1)) {
    throw new Error(
      `Invalid minimumSampleSize: ${sample}. Must be at least 1.`,
    );
  }
}

function assertMaxItems(maxItems?: number): void {
  if (
    maxItems !== undefined &&
    (!Number.isInteger(maxItems) || maxItems <= 0)
  ) {
    throw new Error("maxItems must be a positive integer");
  }
}

function assertCsvConfig(csv: DistributedMapCsvConfig): void {
  if (csv.headers && csv.expectedColumns) {
    throw new Error("headers and expectedColumns are mutually exclusive");
  }
  for (const [label, cols] of [
    ["headers", csv.headers],
    ["expectedColumns", csv.expectedColumns],
  ] as const) {
    if (cols) {
      if (cols.length === 0) throw new Error(`${label} must be non-empty`);
      if (new Set(cols).size !== cols.length) {
        throw new Error(`${label} must not contain duplicates`);
      }
    }
  }
}

function assertS3SourceConfig(s3: DistributedMapS3SourceConfig): void {
  assertBucket(s3.bucket);
  assertBucketOwner(s3.expectedBucketOwner);
  if ((s3.key === undefined) === (s3.prefix === undefined)) {
    throw new Error("S3 source must set exactly one of key or prefix");
  }
  if (s3.key !== undefined) {
    if (!s3.key) throw new Error("S3 source key must be non-empty");
    if (!s3.format) throw new Error("S3 source with a key requires a format");
  }
  if (s3.csv) {
    if (s3.format !== "CSV")
      throw new Error("csv options are only valid when format is CSV");
    assertCsvConfig(s3.csv);
  }
}

/** Validate a source config. An array of items is always valid. @internal */
export function assertSourceConfig(source: unknown): void {
  if (Array.isArray(source)) return;
  if (typeof source !== "object" || source === null) {
    throw new Error(
      "Invalid source: must be a DistributedMapSourceConfig or an array of items.",
    );
  }
  const s = source as DistributedMapSourceConfig<unknown>;
  const set = [s.inline, s.s3, s.reader].filter((v) => v !== undefined).length;
  if (set !== 1)
    throw new Error(
      "Invalid source: must set exactly one of inline, s3 or reader.",
    );
  assertMaxItems(s.maxItems);
  if (s.inline && !Array.isArray(s.inline.items)) {
    throw new Error("inline source items must be an array");
  }
  if (s.s3) assertS3SourceConfig(s.s3);
  if (s.reader) assertFunctionName(s.reader.functionName);
}

const RESPONSE_MODES: ReadonlySet<string> = new Set([
  "batch",
  "itemFailures",
  "itemResults",
]);

/** Validate a processor config. @internal */
export function assertProcessorConfig(processor: unknown): void {
  if (typeof processor !== "object" || processor === null) {
    throw new Error(
      "Invalid processor: must be a DistributedMapProcessorConfig.",
    );
  }
  const p = processor as DistributedMapProcessorConfig;
  assertFunctionName(p.functionName);
  if (p.responseMode !== undefined && !RESPONSE_MODES.has(p.responseMode)) {
    throw new Error(
      `Invalid responseMode: ${String(p.responseMode)}. Must be "batch", "itemFailures" or "itemResults".`,
    );
  }
  const batchSize = p.batchSize;
  if (
    batchSize !== undefined &&
    (!Number.isInteger(batchSize) || batchSize < 1 || batchSize > 10000)
  ) {
    throw new Error("batchSize must be an integer between 1 and 10000");
  }
  const attempts = p.maxRetryAttempts;
  if (
    typeof attempts === "number" &&
    (!Number.isInteger(attempts) || attempts < 0)
  ) {
    throw new Error(
      'maxRetryAttempts must be a non-negative integer or "unlimited"',
    );
  }
  if (p.maxRetryDuration) {
    const seconds = durationToSeconds(p.maxRetryDuration);
    if (seconds < 60 || seconds > 6 * 60 * 60) {
      throw new Error("maxRetryDuration must be between 1 minute and 6 hours");
    }
  }
  const prefix = p.durableExecutionNamePrefix;
  if (prefix !== undefined && (prefix.length < 1 || prefix.length > 36)) {
    throw new Error(
      "durableExecutionNamePrefix must be between 1 and 36 characters",
    );
  }
}

/** Validate a destination config. @internal */
export function assertDestinationConfig(
  destination?: DistributedMapDestinationConfig,
): void {
  const success = destination?.onSuccess;
  if (success) {
    assertBucket(success.bucket);
    assertBucketOwner(success.expectedBucketOwner);
    if (!(success.includeInput ?? false) && !(success.includeOutput ?? true)) {
      throw new Error(
        "success destination must include at least one of input or output",
      );
    }
  }
  const failure = destination?.onFailure;
  if (failure) {
    assertBucket(failure.bucket);
    assertBucketOwner(failure.expectedBucketOwner);
    if (!(failure.includeInput ?? true) && !(failure.includeError ?? true)) {
      throw new Error(
        "failure destination must include at least one of input or error",
      );
    }
  }
}

/** Optional settings for a distributed map run. @public */
export type DistributedMapConfig<TOutput> = {
  destination?: DistributedMapDestinationConfig;
  completionConfig?: DistributedMapCompletionConfig;
  timeout?: Duration;
} & (
  | { collectResults?: false; resultSerdes?: never }
  | { collectResults: true; resultSerdes?: Serdes<TOutput> }
);

/** Summary of a resolved distributed map run. @public */
export interface DistributedMapSummary {
  distributedMapRunArn?: string;
  status: DistributedMapStatus;
  completionReason: DistributedMapCompletionReason;
  completionDetails?: string;
  hasFailure: boolean;
  successCount: number;
  failureCount: number;
  unprocessedCount: number;
  totalCount?: number;
  /** Throws {@link DistributedMapError} when the run did not fully succeed. */
  throwIfError(): void;
}

/** A single item outcome in a collected distributed map result. @public */
export interface DistributedMapResultItem<TOutput> {
  itemId: string;
  status: DistributedMapItemStatus;
  result?: TOutput;
  error?: DistributedMapItemError;
}

/** A resolved distributed map run with per-item results (collectResults: true). @public */
export interface DistributedMapResult<TOutput> extends DistributedMapSummary {
  all: Array<DistributedMapResultItem<TOutput>>;
  succeeded(): Array<
    DistributedMapResultItem<TOutput> & {
      status: DistributedMapItemStatus.SUCCEEDED;
    }
  >;
  failed(): Array<
    DistributedMapResultItem<TOutput> & {
      status: DistributedMapItemStatus.FAILED;
    }
  >;
  getResults(): Array<TOutput | undefined>;
  getErrors(): Array<DistributedMapItemError | undefined>;
}
