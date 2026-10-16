import { Duration } from "./core";
import { Serdes, AnySerdes } from "../utils/serdes/serdes";
import { durationToSeconds } from "../utils/duration/duration";
import type {
  DistributedMapDestinationEntryOptions,
  DistributedMapDestinationInclude,
  DistributedMapFunctionResponseType,
  DistributedMapProcessorOptions,
  DistributedMapCsvFormatOptions,
  DistributedMapS3Format,
  DistributedMapS3SourceOptions,
  ErrorObject,
} from "./wire";
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

/** Options for an inline source. @public */
export interface DistributedMapInlineSourceConfig<TItem> {
  /** Serializes each item. Must match the item serdes the processor uses. Defaults to JSON. */
  serdes?: Serdes<TItem>;
  /** Maximum number of items the service reads from the source. */
  maxItems?: number;
}

/** Options for an S3 source. @public */
export interface DistributedMapS3SourceConfig {
  /** Account ID that must own the bucket, as exactly 12 digits. */
  expectedBucketOwner?: string;
  /** Maximum number of items the service reads from the source. */
  maxItems?: number;
}

/**
 * Column options for a CSV source. A headered file is a file whose first row is a header row.
 * Pass headers for a file with no header row, or expectedColumns for a headered file.
 * The two options are mutually exclusive.
 *
 * @public
 */
export type DistributedMapCsvColumns =
  | {
      /** Column names for a file with no header row. The first row is read as data. */
      headers: readonly string[];
      expectedColumns?: never;
    }
  | {
      /**
       * Column names of a headered file. The service never receives them. A missing column
       * reads as null.
       *
       * @example Type the row in the processor from the same names
       * ```ts
       * const columns = ["id", "name"] as const;
       * type OrderRow = { [K in (typeof columns)[number]]: string | null };
       * ```
       */
      expectedColumns: readonly string[];
      headers?: never;
    }
  | { headers?: never; expectedColumns?: never };

/** Options for a CSV source. @public */
export type DistributedMapCsvConfig = DistributedMapS3SourceConfig &
  DistributedMapCsvColumns & {
    /** Field delimiter. Defaults to COMMA. */
    delimiter?: CsvDelimiter;
  };

/** Options for a reader function source. @public */
export interface DistributedMapReaderSourceConfig<TState> {
  /** State passed to the reader's first call. */
  initialState?: TState;
  /** Serializes initialState. Must match the state serdes the reader uses. Defaults to JSON. */
  stateSerdes?: Serdes<TState>;
  /** Maximum number of items the service reads from the source. */
  maxItems?: number;
}

/**
 * Normalized source that the handler converts to the checkpoint's source options. Inline items
 * and reader state are serialized when the operation starts, so they are kept unserialized here.
 *
 * @internal
 */
export type DistributedMapSourceDefinition =
  | {
      type: "INLINE";
      items: unknown[];
      serdes?: AnySerdes;
      maxItems?: number;
    }
  | { type: "S3"; s3: DistributedMapS3SourceOptions; maxItems?: number }
  | {
      type: "READER_FUNCTION";
      functionName: string;
      initialState?: unknown;
      stateSerdes?: AnySerdes;
      maxItems?: number;
    };

/**
 * Where a distributed map run reads its items. Build one with the factories on
 * {@link (DistributedMapSource:variable)}, or pass a plain array as an inline source.
 *
 * @public
 */
export interface DistributedMapSource<TItem> {
  /** @internal */
  readonly definition: DistributedMapSourceDefinition;
  /**
   * Carries the item type so distributedMap can infer it. Never set at runtime.
   * @internal
   */
  readonly __item?: TItem;
}

/** Options for a processor. @public */
export interface DistributedMapProcessorConfig {
  /** Items per processor call, from 1 through 10000. */
  batchSize?: number;
  /** Retries per batch, a non-negative integer or "unlimited". 0 disables retries. Defaults to 2. */
  maxRetryAttempts?: number | "unlimited";
  /** Total retry time per batch, from 1 minute through 6 hours. Defaults to 6 hours. */
  maxRetryDuration?: Duration;
  /** Name prefix for durable processor executions, 1 to 36 characters. */
  durableExecutionNamePrefix?: string;
}

/**
 * The Lambda function that processes batches of items. Build one with the factories on
 * {@link (DistributedMapProcessor:variable)}.
 *
 * @public
 */
export interface DistributedMapProcessor<TOutput> {
  /** @internal */
  readonly options: DistributedMapProcessorOptions;
  /**
   * Carries the output type so distributedMap can infer the result type. Never set at runtime.
   * @internal
   */
  readonly __output?: TOutput;
}

/** Options for an S3 destination for succeeded items. @public */
export interface DistributedMapSuccessDestinationConfig {
  /** Write each item's input. Defaults to false. */
  includeInput?: boolean;
  /** Write each item's output. Defaults to true. */
  includeOutput?: boolean;
  /** Account ID that must own the bucket, as exactly 12 digits. */
  expectedBucketOwner?: string;
}

/** Options for an S3 destination for failed items. @public */
export interface DistributedMapFailureDestinationConfig {
  /** Write each item's input. Defaults to true. */
  includeInput?: boolean;
  /** Write each item's error. Defaults to true. */
  includeError?: boolean;
  /** Account ID that must own the bucket, as exactly 12 digits. */
  expectedBucketOwner?: string;
}

/**
 * Where the service writes records for succeeded items. Build one with
 * `DistributedMapDestination.s3.successes`.
 *
 * @public
 */
export interface SuccessDestination {
  /** @internal */
  readonly options: DistributedMapDestinationEntryOptions;
  /**
   * Stops a SuccessDestination from being assigned to onFailure. Never set at runtime.
   * @internal
   */
  readonly __kind?: "success";
}

/**
 * Where the service writes records for failed items. Build one with
 * `DistributedMapDestination.s3.failures`.
 *
 * @public
 */
export interface FailureDestination {
  /** @internal */
  readonly options: DistributedMapDestinationEntryOptions;
  /**
   * Stops a FailureDestination from being assigned to onSuccess. Never set at runtime.
   * @internal
   */
  readonly __kind?: "failure";
}

/** Where the service writes item input, output, and error records. @public */
export interface DistributedMapDestinationConfig {
  onSuccess?: SuccessDestination;
  onFailure?: FailureDestination;
}

const TWELVE_DIGITS = /^[0-9]{12}$/;

function assertBucketOwner(owner?: string): void {
  if (owner !== undefined && !TWELVE_DIGITS.test(owner)) {
    throw new Error("expectedBucketOwner must be exactly 12 digits");
  }
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

const MAX_ITEMS_TO_READ = 100_000_000;

function assertMaxItems(maxItems?: number): void {
  if (
    maxItems !== undefined &&
    (!Number.isInteger(maxItems) ||
      maxItems < 1 ||
      maxItems > MAX_ITEMS_TO_READ)
  ) {
    throw new Error(
      `maxItems must be an integer from 1 through ${MAX_ITEMS_TO_READ}`,
    );
  }
}

const MAX_CSV_COLUMNS = 1000;
const MAX_CSV_COLUMN_LENGTH = 256;

function assertCsvColumns(csv: {
  headers?: readonly string[];
  expectedColumns?: readonly string[];
}): void {
  if (csv.headers && csv.expectedColumns) {
    throw new Error("headers and expectedColumns are mutually exclusive");
  }
  for (const [label, cols] of [
    ["headers", csv.headers],
    ["expectedColumns", csv.expectedColumns],
  ] as const) {
    if (cols) {
      if (cols.length === 0) throw new Error(`${label} must be non-empty`);
      if (cols.length > MAX_CSV_COLUMNS) {
        throw new Error(
          `${label} must hold at most ${MAX_CSV_COLUMNS} names, got ${cols.length}`,
        );
      }
      if (cols.some((c) => c.length > MAX_CSV_COLUMN_LENGTH)) {
        throw new Error(
          `each ${label} name must be at most ${MAX_CSV_COLUMN_LENGTH} characters`,
        );
      }
      if (new Set(cols).size !== cols.length) {
        throw new Error(`${label} must not contain duplicates`);
      }
    }
  }
}

/** Splits an s3:// URI into the bucket and the text after the first slash. */
function parseS3Uri(uri: string): { bucket: string; path: string } {
  if (typeof uri !== "string" || !uri.startsWith("s3://")) {
    throw new Error(`Invalid S3 URI (must start with s3://): ${String(uri)}`);
  }
  const withoutScheme = uri.slice("s3://".length);
  const slash = withoutScheme.indexOf("/");
  const bucket = slash === -1 ? withoutScheme : withoutScheme.slice(0, slash);
  if (!bucket) throw new Error(`Invalid S3 URI (missing bucket): ${uri}`);
  return { bucket, path: slash === -1 ? "" : withoutScheme.slice(slash + 1) };
}

type S3SourceOpts = DistributedMapS3SourceConfig & {
  headers?: readonly string[];
  expectedColumns?: readonly string[];
  delimiter?: CsvDelimiter;
};

/** Validates the options every S3 source accepts and splits the URI. */
function s3SourceBase(
  uri: string,
  format: DistributedMapS3Format | undefined,
  opts: S3SourceOpts,
): {
  base: Pick<DistributedMapS3SourceOptions, "Bucket" | "ExpectedBucketOwner">;
  path: string;
  csv?: DistributedMapCsvFormatOptions;
} {
  assertBucketOwner(opts.expectedBucketOwner);
  assertMaxItems(opts.maxItems);
  if (format === "CSV") assertCsvColumns(opts);
  const { bucket, path } = parseS3Uri(uri);
  return {
    base: {
      Bucket: bucket,
      ...(opts.expectedBucketOwner
        ? { ExpectedBucketOwner: opts.expectedBucketOwner }
        : {}),
    },
    path,
    ...(format === "CSV"
      ? {
          csv: {
            HeaderLocation: opts.headers ? "GIVEN" : "FIRST_ROW",
            ...(opts.headers ? { Headers: [...opts.headers] } : {}),
            Delimiter: opts.delimiter ?? "COMMA",
          },
        }
      : {}),
  };
}

/** Builds an OBJECT source, which reads the one object at the URI. */
function s3ObjectSource<TItem>(
  uri: string,
  format: DistributedMapS3Format,
  opts: S3SourceOpts = {},
): DistributedMapSource<TItem> {
  const { base, path, csv } = s3SourceBase(uri, format, opts);
  if (!path) throw new Error(`S3 object source requires a key: ${uri}`);
  return {
    definition: {
      type: "S3",
      maxItems: opts.maxItems,
      s3: {
        Type: "OBJECT",
        ...base,
        ObjectConfig: {
          Key: path,
          Format: format,
          ...(csv ? { CsvFormatOptions: csv } : {}),
        },
      },
    },
  };
}

/** Builds a LISTED_OBJECTS source, which reads every object under the prefix. */
function s3ListedObjectsSource<TItem>(
  prefixUri: string,
  format: DistributedMapS3Format,
  opts: S3SourceOpts = {},
): DistributedMapSource<TItem> {
  const { base, path, csv } = s3SourceBase(prefixUri, format, opts);
  return {
    definition: {
      type: "S3",
      maxItems: opts.maxItems,
      s3: {
        Type: "LISTED_OBJECTS",
        ...base,
        ListedObjectsConfig: {
          KeyPrefix: path,
          Format: format,
          ...(csv ? { CsvFormatOptions: csv } : {}),
        },
      },
    },
  };
}

/** Factories for a distributed map source. @public */
export const DistributedMapSource = {
  /** Items supplied in the operation request. The serialized items must total at most 1 MB. */
  inline<TItem>(
    items: TItem[],
    opts?: DistributedMapInlineSourceConfig<TItem>,
  ): DistributedMapSource<TItem> {
    if (!Array.isArray(items))
      throw new Error("inline source items must be an array");
    assertMaxItems(opts?.maxItems);
    return {
      definition: {
        type: "INLINE",
        items,
        serdes: opts?.serdes as AnySerdes | undefined,
        maxItems: opts?.maxItems,
      },
    };
  },

  /** Sources read from S3. Each factory fixes whether the URI is an object key or a prefix. */
  s3: {
    /** One S3 object. Each line is an item. */
    jsonLines<TItem = unknown>(
      uri: string,
      opts?: DistributedMapS3SourceConfig,
    ): DistributedMapSource<TItem> {
      return s3ObjectSource<TItem>(uri, "JSON_LINES", opts);
    },
    /** One S3 object holding a JSON array. Each element is an item. */
    jsonArray<TItem = unknown>(
      uri: string,
      opts?: DistributedMapS3SourceConfig,
    ): DistributedMapSource<TItem> {
      return s3ObjectSource<TItem>(uri, "JSON_ARRAY", opts);
    },
    /** One S3 object in CSV format. Each record is an item. */
    csv<TItem = Record<string, string | null>>(
      uri: string,
      opts?: DistributedMapCsvConfig,
    ): DistributedMapSource<TItem> {
      return s3ObjectSource<TItem>(uri, "CSV", opts);
    },
    /** Every object under a prefix. Each object's key and metadata is an item. Contents are not read. */
    objects<TItem = unknown>(
      prefixUri: string,
      opts?: DistributedMapS3SourceConfig,
    ): DistributedMapSource<TItem> {
      const { base, path } = s3SourceBase(prefixUri, undefined, opts ?? {});
      return {
        definition: {
          type: "S3",
          maxItems: opts?.maxItems,
          s3: {
            Type: "LISTED_OBJECT_METADATA",
            ...base,
            ListedObjectMetadataConfig: { KeyPrefix: path },
          },
        },
      };
    },
    /** Every object under a prefix. Each line of every object is an item. */
    flattenedJsonLines<TItem = unknown>(
      prefixUri: string,
      opts?: DistributedMapS3SourceConfig,
    ): DistributedMapSource<TItem> {
      return s3ListedObjectsSource<TItem>(prefixUri, "JSON_LINES", opts);
    },
    /** Every object under a prefix, each holding a JSON array. Each element is an item. */
    flattenedJsonArray<TItem = unknown>(
      prefixUri: string,
      opts?: DistributedMapS3SourceConfig,
    ): DistributedMapSource<TItem> {
      return s3ListedObjectsSource<TItem>(prefixUri, "JSON_ARRAY", opts);
    },
    /** Every object under a prefix in CSV format. Each record of every object is an item. */
    flattenedCsv<TItem = Record<string, string | null>>(
      prefixUri: string,
      opts?: DistributedMapCsvConfig,
    ): DistributedMapSource<TItem> {
      return s3ListedObjectsSource<TItem>(prefixUri, "CSV", opts);
    },
  },

  /** Sources read by calling a reader Lambda function. */
  reader: {
    /** A reader function that returns items one page at a time. */
    fromFunction<TItem = unknown, TState = unknown>(
      functionName: string,
      opts?: DistributedMapReaderSourceConfig<TState>,
    ): DistributedMapSource<TItem> {
      assertFunctionName(functionName);
      assertMaxItems(opts?.maxItems);
      return {
        definition: {
          type: "READER_FUNCTION",
          functionName,
          initialState: opts?.initialState,
          stateSerdes: opts?.stateSerdes as AnySerdes | undefined,
          maxItems: opts?.maxItems,
        },
      };
    },
  },
};

const UNLIMITED_RETRY_WIRE = -1;

function processor<TOutput>(
  functionName: string,
  responseTypes: DistributedMapFunctionResponseType[] | undefined,
  opts: DistributedMapProcessorConfig = {},
): DistributedMapProcessor<TOutput> {
  assertFunctionName(functionName);
  const batchSize = opts.batchSize;
  if (
    batchSize !== undefined &&
    (!Number.isInteger(batchSize) || batchSize < 1 || batchSize > 10000)
  ) {
    throw new Error("batchSize must be an integer between 1 and 10000");
  }
  const attempts = opts.maxRetryAttempts;
  if (
    attempts !== undefined &&
    attempts !== "unlimited" &&
    (!Number.isInteger(attempts) || attempts < 0)
  ) {
    throw new Error(
      'maxRetryAttempts must be a non-negative integer or "unlimited"',
    );
  }
  let maxRetryDurationSeconds: number | undefined;
  if (opts.maxRetryDuration) {
    maxRetryDurationSeconds = durationToSeconds(opts.maxRetryDuration);
    if (maxRetryDurationSeconds < 60 || maxRetryDurationSeconds > 6 * 60 * 60) {
      throw new Error("maxRetryDuration must be between 1 minute and 6 hours");
    }
  }
  const prefix = opts.durableExecutionNamePrefix;
  if (prefix !== undefined && (prefix.length < 1 || prefix.length > 36)) {
    throw new Error(
      "durableExecutionNamePrefix must be between 1 and 36 characters",
    );
  }
  return {
    options: {
      FunctionName: functionName,
      ...(responseTypes ? { FunctionResponseTypes: responseTypes } : {}),
      ...(batchSize !== undefined ? { BatchSize: batchSize } : {}),
      ...(attempts !== undefined
        ? {
            MaxRetryAttempts:
              attempts === "unlimited" ? UNLIMITED_RETRY_WIRE : attempts,
          }
        : {}),
      ...(maxRetryDurationSeconds !== undefined
        ? { MaxRetryDurationSeconds: maxRetryDurationSeconds }
        : {}),
      ...(prefix !== undefined ? { DurableExecutionNamePrefix: prefix } : {}),
    },
  };
}

/** Factories for a distributed map processor. @public */
export const DistributedMapProcessor = {
  /**
   * The function reports one outcome for the whole batch. Returning succeeds every item in the
   * batch, and throwing fails the batch. Pair it with a batch handler wrapper.
   */
  reportBatchOutcome(
    functionName: string,
    opts?: DistributedMapProcessorConfig,
  ): DistributedMapProcessor<never> {
    return processor<never>(functionName, undefined, opts);
  },
  /**
   * The function reports an outcome for each item: an output when the item succeeds, or an
   * error when it fails. Pair it with an item handler wrapper. TOutput is the item output type,
   * and defaults to never for a processor whose outputs are not collected.
   */
  reportItemOutcomes<TOutput = never>(
    functionName: string,
    opts?: DistributedMapProcessorConfig,
  ): DistributedMapProcessor<TOutput> {
    return processor<TOutput>(
      functionName,
      ["REPORT_BATCH_ITEM_OUTCOMES"],
      opts,
    );
  },
};

// The service rejects a destination KeyPrefix over 900 characters, which fails the checkpoint.
// So the SDK rejects it when the destination is built.
const MAX_DESTINATION_KEY_PREFIX_LENGTH = 900;

function s3Destination(
  prefixUri: string,
  include: DistributedMapDestinationInclude[],
  expectedBucketOwner: string | undefined,
): DistributedMapDestinationEntryOptions {
  assertBucketOwner(expectedBucketOwner);
  const { bucket, path } = parseS3Uri(prefixUri);
  if (path.length > MAX_DESTINATION_KEY_PREFIX_LENGTH) {
    throw new Error(
      `destination key prefix must be at most ${MAX_DESTINATION_KEY_PREFIX_LENGTH} characters (got ${path.length})`,
    );
  }
  return {
    Type: "S3",
    Include: include,
    S3DestinationConfig: {
      Bucket: bucket,
      KeyPrefix: path,
      ...(expectedBucketOwner
        ? { ExpectedBucketOwner: expectedBucketOwner }
        : {}),
    },
  };
}

/** Factories for distributed map destinations. @public */
export const DistributedMapDestination = {
  /** Destinations that write records to S3 objects under a prefix. */
  s3: {
    /** Writes a record for each succeeded item. */
    successes(
      prefixUri: string,
      opts?: DistributedMapSuccessDestinationConfig,
    ): SuccessDestination {
      const include: DistributedMapDestinationInclude[] = [];
      if (opts?.includeInput ?? false) include.push("INPUT");
      if (opts?.includeOutput ?? true) include.push("OUTPUT");
      if (include.length === 0) {
        throw new Error(
          "success destination must include at least one of input or output",
        );
      }
      return {
        options: s3Destination(prefixUri, include, opts?.expectedBucketOwner),
      };
    },
    /** Writes a record for each failed item. */
    failures(
      prefixUri: string,
      opts?: DistributedMapFailureDestinationConfig,
    ): FailureDestination {
      const include: DistributedMapDestinationInclude[] = [];
      if (opts?.includeInput ?? true) include.push("INPUT");
      if (opts?.includeError ?? true) include.push("ERROR");
      if (include.length === 0) {
        throw new Error(
          "failure destination must include at least one of input or error",
        );
      }
      return {
        options: s3Destination(prefixUri, include, opts?.expectedBucketOwner),
      };
    },
  },
};

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
  // Omitting the config means zero tolerated failures. A present config with neither
  // threshold would be left out of the checkpoint and resolve the same way, so reject it.
  if (count === undefined && pct === undefined) {
    throw new Error(
      "completionConfig must set toleratedFailureCount or toleratedFailurePercentage",
    );
  }
  if (count !== undefined && (!Number.isInteger(count) || count < 0)) {
    throw new Error(
      `Invalid toleratedFailureCount: ${count}. Must be a non-negative integer.`,
    );
  }
  if (pct !== undefined && (!Number.isInteger(pct) || pct < 0 || pct > 100)) {
    throw new Error(
      `Invalid toleratedFailurePercentage: ${pct}. Must be an integer from 0 through 100.`,
    );
  }
  if (sample !== undefined && (!Number.isInteger(sample) || sample < 1)) {
    throw new Error(
      `Invalid minimumSampleSize: ${sample}. Must be at least 1.`,
    );
  }
}

const SOURCE_TYPES: ReadonlySet<string> = new Set([
  "INLINE",
  "S3",
  "READER_FUNCTION",
]);

/**
 * Rejects a source that is neither an array nor built by a source factory. The factories
 * validate their own arguments, so this checks only the shape.
 *
 * @internal
 */
export function assertSource(source: unknown): void {
  if (Array.isArray(source)) return;
  const type = (source as DistributedMapSource<unknown> | undefined)?.definition
    ?.type;
  if (typeof type !== "string" || !SOURCE_TYPES.has(type)) {
    throw new Error(
      "Invalid source: must be an array of items or built with a DistributedMapSource factory.",
    );
  }
}

/**
 * Rejects a processor that was not built by a processor factory.
 *
 * @internal
 */
export function assertProcessor(processor: unknown): void {
  const name = (processor as DistributedMapProcessor<unknown> | undefined)
    ?.options?.FunctionName;
  if (typeof name !== "string" || !name) {
    throw new Error(
      "Invalid processor: must be built with a DistributedMapProcessor factory.",
    );
  }
}

/**
 * Rejects a destination that was not built by the matching destination factory. A success
 * record has no error and a failure record has no output, so the include list shows which
 * factory built the destination.
 *
 * @internal
 */
export function assertDestination(
  destination?: DistributedMapDestinationConfig,
): void {
  const check = (
    entry: SuccessDestination | FailureDestination | undefined,
    label: string,
    factory: string,
    forbidden: DistributedMapDestinationInclude,
  ): void => {
    if (entry === undefined) return;
    const include = entry?.options?.Include;
    if (!Array.isArray(include) || include.includes(forbidden)) {
      throw new Error(
        `Invalid destination: ${label} must be built with DistributedMapDestination.s3.${factory}.`,
      );
    }
  };
  check(destination?.onSuccess, "onSuccess", "successes", "ERROR");
  check(destination?.onFailure, "onFailure", "failures", "OUTPUT");
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
  /** True only when the service read the complete source. */
  sourceExhausted?: boolean;
  /** Why the run did not succeed, when the service recorded error details. */
  error?: ErrorObject;
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
