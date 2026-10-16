import {
  ExecutionContext,
  OperationSubType,
  DurablePromise,
  OperationLifecycleState,
  DistributedMapConfig,
  DistributedMapProcessor,
  DistributedMapSource,
  DistributedMapSummary,
  DistributedMapResult,
  DistributedMapResultItem,
  DistributedMapStatus,
  DistributedMapItemStatus,
  assertCompletionConfig,
  assertDestination,
  assertProcessor,
  assertSource,
} from "../../types";
import {
  DistributedMapError,
  DistributedMapItemError,
} from "../../errors/durable-error/durable-error";
import {
  OperationAction,
  OperationStatus,
  OperationType,
  DistributedMapOptions,
  DistributedMapSourceOptions,
  DistributedMapDestinationOptions,
  DistributedMapCompletionOptions,
  DistributedMapDetails,
  ErrorObject,
} from "../../types/wire";
import { log } from "../../utils/logger/logger";
import { Checkpoint } from "../../utils/checkpoint/checkpoint-helper";
import { defaultSerdes, AnySerdes } from "../../utils/serdes/serdes";
import { durationToSeconds } from "../../utils/duration/duration";
import {
  safeSerialize,
  safeDeserialize,
} from "../../errors/serdes-errors/serdes-errors";
import { validateReplayConsistency } from "../../utils/replay-validation/replay-validation";
import {
  DurableInstrumentationPlugin,
  OperationInfo,
} from "../../types/plugin";
import {
  backfillOperationInfo,
  toOperationInfo,
} from "../../utils/operation/operation";
import { hashId } from "../../utils/step-id-utils/step-id-utils";
import { TerminationManager } from "../../termination-manager/termination-manager";
import { TerminationReason } from "../../termination-manager/types";

type AnyDistributedMapConfig = DistributedMapConfig<unknown>;

const INLINE_SIZE_LIMIT = 1024 * 1024;
const READER_STATE_LIMIT = 32 * 1024;
const MAX_TIMEOUT_SECONDS = 7776000; // 90 days

/**
 * DistributedMapError for a run whose operation did not succeed. The message, cause and data
 * come from the run's recorded Error when the service sent one.
 */
function runDidNotSucceedError(
  status: string | undefined,
  error: ErrorObject | undefined,
): DistributedMapError {
  const ended = `Map run ended ${status ?? "unknown"}`;
  return new DistributedMapError(
    error?.ErrorMessage ? `${ended}: ${error.ErrorMessage}` : ended,
    error ? causeFrom(error) : undefined,
    error?.ErrorData,
  );
}

/**
 * Terminates the execution for an invalid configuration value. An invalid value is a
 * deterministic, non-retryable configuration error: terminating rather than throwing means a
 * misconfigured call fails cleanly instead of being retried by Lambda only to fail identically
 * again. Mirrors the map and parallel handler's treatment of the same error class.
 */
function terminateForConfig(
  message: string,
  terminationManager: TerminationManager,
): false {
  terminationManager.terminate({
    reason: TerminationReason.CONFIG_VALIDATION_ERROR,
    message,
    error: new Error(message),
  });
  return false;
}

/**
 * Validates the call-site configuration. Returns `true` when the config is valid, or `false`
 * when it terminated the execution (the caller must then stop).
 */
function validateConfig(
  source: unknown,
  processor: unknown,
  maxConcurrency: number,
  config: AnyDistributedMapConfig | undefined,
  terminationManager: TerminationManager,
): boolean {
  if (!Number.isInteger(maxConcurrency) || maxConcurrency < 1) {
    return terminateForConfig(
      `Invalid maxConcurrency: ${maxConcurrency}. Must be an integer greater than zero.`,
      terminationManager,
    );
  }
  try {
    assertSource(source);
    assertProcessor(processor);
    assertDestination(config?.destination);
    assertCompletionConfig(config?.completionConfig);
  } catch (error) {
    return terminateForConfig((error as Error).message, terminationManager);
  }
  if (config?.timeout) {
    const seconds = durationToSeconds(config.timeout);
    if (seconds <= 0 || seconds > MAX_TIMEOUT_SECONDS) {
      return terminateForConfig(
        `Invalid timeout: ${seconds} seconds. Must be positive and at most 90 days.`,
        terminationManager,
      );
    }
  }
  return true;
}

/** Fire onOperationEnd, attaching an error if the status is not SUCCEEDED. */
async function fireOperationEnd(
  plugin: DurableInstrumentationPlugin,
  operationInfo: OperationInfo,
  isReplay: boolean,
  status: OperationStatus | undefined,
  runError: ErrorObject | undefined,
): Promise<void> {
  const opError =
    status === OperationStatus.SUCCEEDED
      ? undefined
      : runDidNotSucceedError(status, runError);
  await plugin.onOperationEnd?.({
    ...operationInfo,
    isReplay,
    ...(opError ? { error: opError } : {}),
  });
}

async function serializeInlineItems(
  items: unknown[],
  serdes: AnySerdes,
  stepId: string,
  name: string | undefined,
  context: ExecutionContext,
): Promise<string[]> {
  const wireItems: string[] = [];
  for (const item of items) {
    const serialized = await safeSerialize(
      serdes,
      item,
      stepId,
      name,
      context.terminationManager,
      context.durableExecutionArn,
    );
    wireItems.push(serialized as string);
  }
  const total = Buffer.byteLength(JSON.stringify(wireItems), "utf8");
  if (total > INLINE_SIZE_LIMIT) {
    throw new Error(
      `inline source exceeds the 1 MB limit (serialized size: ${total} bytes)`,
    );
  }
  return wireItems;
}

async function toDistributedMapSourceOptions(
  source: DistributedMapSource<unknown> | unknown[],
  defaultS: AnySerdes,
  stepId: string,
  name: string | undefined,
  context: ExecutionContext,
): Promise<DistributedMapSourceOptions> {
  if (Array.isArray(source)) {
    return {
      Type: "INLINE",
      InlineSourceConfig: {
        Items: await serializeInlineItems(
          source,
          defaultS,
          stepId,
          name,
          context,
        ),
      },
    };
  }
  const def = source.definition;
  if (def.type === "INLINE") {
    return {
      Type: "INLINE",
      MaxItemsToRead: def.maxItems,
      InlineSourceConfig: {
        Items: await serializeInlineItems(
          def.items,
          def.serdes ?? defaultS,
          stepId,
          name,
          context,
        ),
      },
    };
  }
  if (def.type === "S3") {
    return {
      Type: "S3",
      MaxItemsToRead: def.maxItems,
      S3SourceConfig: { ...def.s3 },
    };
  }
  const readerConfig: NonNullable<
    DistributedMapSourceOptions["ReaderFunctionSourceConfig"]
  > = {
    FunctionName: def.functionName,
  };
  if (def.initialState !== undefined && def.initialState !== null) {
    const state = await safeSerialize(
      def.stateSerdes ?? defaultS,
      def.initialState,
      stepId,
      name,
      context.terminationManager,
      context.durableExecutionArn,
    );
    if (Buffer.byteLength(state ?? "", "utf8") > READER_STATE_LIMIT) {
      throw new Error("reader initialState exceeds the 32 KB limit");
    }
    readerConfig.InitialState = state as string;
  }
  return {
    Type: "READER_FUNCTION",
    MaxItemsToRead: def.maxItems,
    ReaderFunctionSourceConfig: readerConfig,
  };
}

/** @internal */
export function toDistributedMapDestinationOptions(
  config: AnyDistributedMapConfig,
): DistributedMapDestinationOptions | undefined {
  const dest = config.destination;
  if (!dest?.onSuccess && !dest?.onFailure) return undefined;
  return {
    ...(dest.onSuccess ? { OnSuccess: dest.onSuccess.options } : {}),
    ...(dest.onFailure ? { OnFailure: dest.onFailure.options } : {}),
  };
}

async function buildOptions(
  source: DistributedMapSource<unknown> | unknown[],
  processor: DistributedMapProcessor<unknown>,
  maxConcurrency: number,
  config: AnyDistributedMapConfig,
  defaultS: AnySerdes,
  stepId: string,
  name: string | undefined,
  context: ExecutionContext,
): Promise<DistributedMapOptions> {
  const completion = config.completionConfig;
  const destination = toDistributedMapDestinationOptions(config);
  const completionWire: DistributedMapCompletionOptions = {};
  if (completion) {
    if (
      "toleratedFailureCount" in completion &&
      completion.toleratedFailureCount !== undefined
    ) {
      completionWire.ToleratedFailureCount = completion.toleratedFailureCount;
    }
    if (
      "toleratedFailurePercentage" in completion &&
      completion.toleratedFailurePercentage !== undefined
    ) {
      completionWire.ToleratedFailurePercentage =
        completion.toleratedFailurePercentage;
    }
    if (
      "minimumSampleSize" in completion &&
      completion.minimumSampleSize !== undefined
    ) {
      completionWire.MinimumSampleSize = completion.minimumSampleSize;
    }
  }
  return {
    MaxConcurrency: maxConcurrency,
    Source: await toDistributedMapSourceOptions(
      source,
      defaultS,
      stepId,
      name,
      context,
    ),
    Processor: { ...processor.options },
    ...(destination ? { Destination: destination } : {}),
    ...(Object.keys(completionWire).length > 0
      ? { CompletionConfig: completionWire }
      : {}),
    ...(config.collectResults === true
      ? { ResultCollection: { Mode: "INLINE" } }
      : {}),
    ...(config.timeout
      ? { TimeoutSeconds: durationToSeconds(config.timeout) }
      : {}),
  };
}

const DISTRIBUTED_MAP_STATUS_SET: ReadonlySet<string> = new Set(
  Object.values(DistributedMapStatus),
);

/** Builds an Error whose name and stack come from a recorded ErrorObject. */
function causeFrom(error: ErrorObject): Error {
  const cause = new Error(error.ErrorMessage);
  cause.name = error.ErrorType || "Error";
  cause.stack = error.StackTrace?.join("\n");
  return cause;
}

function parseStatus(value: string | undefined): DistributedMapStatus {
  if (value === undefined) {
    throw new DistributedMapError(
      "missing distributed map status from backend",
    );
  }
  if (DISTRIBUTED_MAP_STATUS_SET.has(value)) {
    return value as DistributedMapStatus;
  }
  // Unrecognized status from backend, don't throw.
  return DistributedMapStatus.UNKNOWN_TO_SDK_VERSION;
}

function parseItemStatus(value: string | undefined): DistributedMapItemStatus {
  if (value === undefined) {
    throw new DistributedMapError(
      "missing distributed map item status from backend",
    );
  }
  if (
    value === DistributedMapItemStatus.SUCCEEDED ||
    value === DistributedMapItemStatus.FAILED
  ) {
    return value;
  }
  // Unrecognized status from backend, don't throw.
  return DistributedMapItemStatus.UNKNOWN_TO_SDK_VERSION;
}

/** @internal */
export async function buildSummary(
  details: DistributedMapDetails,
  config: AnyDistributedMapConfig,
  stepId: string,
  name: string | undefined,
  context: ExecutionContext,
  defaultS: AnySerdes,
  operationStatus: string | undefined,
): Promise<DistributedMapSummary> {
  // The run status is the operation's own terminal status; details does not carry it.
  const status = parseStatus(operationStatus);
  const failureCount = details.FailureCount ?? 0;
  const arn = details.DistributedMapRunArn;
  const runError = details.Error;
  // SourceExhausted is required at terminal once the run exists. It is absent only when the
  // operation failed before a run was created, which is also when the run ARN is absent.
  if (arn !== undefined && details.SourceExhausted === undefined) {
    throw new DistributedMapError(
      "missing distributed map SourceExhausted from backend",
    );
  }

  const throwRunLevel = (): void => {
    if (status !== DistributedMapStatus.SUCCEEDED) {
      throw runDidNotSucceedError(status, runError);
    }
  };

  const summary: DistributedMapSummary = {
    distributedMapRunArn: arn,
    status,
    sourceExhausted: details.SourceExhausted,
    error: runError,
    hasFailure: failureCount > 0,
    successCount: details.SuccessCount ?? 0,
    failureCount,
    unprocessedCount: details.UnprocessedCount ?? 0,
    totalCount: details.TotalCount,
    throwIfError(): void {
      throwRunLevel();
      if (failureCount > 0) {
        throw new DistributedMapError(
          `Map run succeeded but ${failureCount} item(s) permanently failed`,
        );
      }
    },
  };

  if (config.collectResults !== true) return summary;

  const all: Array<DistributedMapResultItem<unknown>> = [];
  for (const wire of details.Results ?? []) {
    let result: unknown;
    if (wire.Output !== undefined && wire.Output !== null) {
      result = await safeDeserialize(
        config.resultSerdes ?? defaultS,
        wire.Output,
        stepId,
        name,
        context.terminationManager,
        context.durableExecutionArn,
      );
    }
    let error: DistributedMapItemError | undefined;
    if (wire.Error) {
      error = new DistributedMapItemError(
        wire.Error.ErrorMessage,
        causeFrom(wire.Error),
        wire.Error.ErrorData,
      );
    }
    all.push({
      itemId: wire.ItemId ?? "",
      status: parseItemStatus(wire.Status),
      result,
      error,
    });
  }

  const succeededItems = (): Array<
    DistributedMapResultItem<unknown> & {
      status: DistributedMapItemStatus.SUCCEEDED;
    }
  > =>
    all.filter(
      (
        i,
      ): i is DistributedMapResultItem<unknown> & {
        status: DistributedMapItemStatus.SUCCEEDED;
      } => i.status === DistributedMapItemStatus.SUCCEEDED,
    );
  const failedItems = (): Array<
    DistributedMapResultItem<unknown> & {
      status: DistributedMapItemStatus.FAILED;
    }
  > =>
    all.filter(
      (
        i,
      ): i is DistributedMapResultItem<unknown> & {
        status: DistributedMapItemStatus.FAILED;
      } => i.status === DistributedMapItemStatus.FAILED,
    );

  const result: DistributedMapResult<unknown> = {
    ...summary,
    all,
    succeeded: succeededItems,
    failed: failedItems,
    getResults: () => succeededItems().map((i) => i.result),
    getErrors: () => failedItems().map((i) => i.error),
    // For inline collection, throw the first failed item's error, otherwise use the summary rule.
    throwIfError(): void {
      if (status === DistributedMapStatus.SUCCEEDED && failureCount > 0) {
        const firstFailed = all.find(
          (i) => i.status === DistributedMapItemStatus.FAILED,
        );
        if (firstFailed) {
          throw (
            firstFailed.error ??
            new DistributedMapError(`item ${firstFailed.itemId} failed`)
          );
        }
      }
      summary.throwIfError();
    },
  };
  return result;
}

export const createDistributedMapHandler = (
  context: ExecutionContext,
  checkpoint: Checkpoint,
  createStepId: () => string,
  parentId?: string,
  checkAndUpdateReplayMode?: () => void,
  getDefaultSerdes?: () => AnySerdes,
  plugin: DurableInstrumentationPlugin = {},
): {
  <TItem, TOutput>(
    source: DistributedMapSource<TItem> | TItem[],
    processor: DistributedMapProcessor<TOutput>,
    maxConcurrency: number,
    config: DistributedMapConfig<TOutput> & { collectResults: true },
  ): DurablePromise<DistributedMapResult<TOutput>>;
  <TItem, TOutput>(
    name: string | undefined,
    source: DistributedMapSource<TItem> | TItem[],
    processor: DistributedMapProcessor<TOutput>,
    maxConcurrency: number,
    config: DistributedMapConfig<TOutput> & { collectResults: true },
  ): DurablePromise<DistributedMapResult<TOutput>>;
  <TItem, TOutput>(
    source: DistributedMapSource<TItem> | TItem[],
    processor: DistributedMapProcessor<TOutput>,
    maxConcurrency: number,
    config?: DistributedMapConfig<TOutput>,
  ): DurablePromise<DistributedMapSummary>;
  <TItem, TOutput>(
    name: string | undefined,
    source: DistributedMapSource<TItem> | TItem[],
    processor: DistributedMapProcessor<TOutput>,
    maxConcurrency: number,
    config?: DistributedMapConfig<TOutput>,
  ): DurablePromise<DistributedMapSummary>;
} => {
  function distributedMapHandler<TItem, TOutput>(
    arg0: string | undefined | DistributedMapSource<TItem> | TItem[],
    arg1:
      | DistributedMapSource<TItem>
      | TItem[]
      | DistributedMapProcessor<TOutput>,
    arg2: DistributedMapProcessor<TOutput> | number,
    arg3?: number | AnyDistributedMapConfig,
    arg4?: AnyDistributedMapConfig,
  ): DurablePromise<DistributedMapSummary | DistributedMapResult<TOutput>> {
    const isNameFirst = typeof arg0 === "string" || arg0 === undefined;
    const name = isNameFirst ? (arg0 as string | undefined) : undefined;
    const source = (isNameFirst ? arg1 : arg0) as
      | DistributedMapSource<TItem>
      | TItem[];
    const processor = (
      isNameFirst ? arg2 : arg1
    ) as DistributedMapProcessor<TOutput>;
    const maxConcurrency = (isNameFirst ? arg3 : arg2) as number;
    const config = (isNameFirst ? arg4 : arg3) as
      | AnyDistributedMapConfig
      | undefined;

    const resolvedConfig: AnyDistributedMapConfig = config ?? {};
    const defaultS = getDefaultSerdes ? getDefaultSerdes() : defaultSerdes;
    const stepId = createStepId();

    const opInfo = {
      id: hashId(stepId),
      name,
      type: OperationType.DISTRIBUTED_MAP,
      subType: OperationSubType.DISTRIBUTED_MAP,
      parentId: parentId ? hashId(parentId) : undefined,
    };

    let isCompleted = false;

    // An invalid config terminates the execution rather than throwing. If it terminated, stop
    // here with a never-settling promise so we don't proceed.
    if (
      !validateConfig(
        source,
        processor,
        maxConcurrency,
        resolvedConfig,
        context.terminationManager,
      )
    ) {
      return new DurablePromise<
        DistributedMapSummary | DistributedMapResult<TOutput>
      >(() => new Promise<never>(() => {}));
    }

    const phase1Promise = (async (): Promise<void> => {
      log("🗺️", "Distributed map phase 1:", { stepId, name });

      let stepData = context.getStepData(stepId);

      validateReplayConsistency(
        stepId,
        {
          type: OperationType.DISTRIBUTED_MAP,
          name,
          subType: OperationSubType.DISTRIBUTED_MAP,
        },
        stepData,
        context,
      );

      // A SUCCEEDED operation carries the run outcome in DistributedMapDetails.
      if (
        stepData?.Status === OperationStatus.SUCCEEDED ||
        stepData?.Status === OperationStatus.FAILED ||
        stepData?.Status === OperationStatus.TIMED_OUT ||
        stepData?.Status === OperationStatus.STOPPED
      ) {
        checkAndUpdateReplayMode?.();
        checkpoint.markOperationState(
          stepId,
          OperationLifecycleState.COMPLETED,
          {
            metadata: {
              stepId,
              name,
              type: OperationType.DISTRIBUTED_MAP,
              subType: OperationSubType.DISTRIBUTED_MAP,
              parentId,
            },
          },
        );
        const operationInfo = toOperationInfo(stepData);
        backfillOperationInfo(operationInfo, opInfo);
        const updated = context.isOperationUpdatedBetweenInvocation(opInfo.id);
        await fireOperationEnd(
          plugin,
          operationInfo,
          !updated,
          stepData?.Status,
          stepData?.DistributedMapDetails?.Error,
        );
        isCompleted = true;
        return;
      }

      if (!stepData) {
        const options = await buildOptions(
          source as DistributedMapSource<unknown> | unknown[],
          processor,
          maxConcurrency,
          resolvedConfig,
          defaultS,
          stepId,
          name,
          context,
        );
        await checkpoint.checkpoint(stepId, {
          Id: stepId,
          ParentId: parentId,
          Action: OperationAction.START,
          SubType: OperationSubType.DISTRIBUTED_MAP,
          Type: OperationType.DISTRIBUTED_MAP,
          Name: name,
          DistributedMapOptions: options,
        });
        stepData = context.getStepData(stepId);
        const operationInfo = toOperationInfo(stepData);
        backfillOperationInfo(operationInfo, opInfo);
        await plugin.onOperationStart?.({ ...operationInfo, isReplay: false });
      } else {
        const operationInfo = toOperationInfo(stepData);
        backfillOperationInfo(operationInfo, opInfo);
        await plugin.onOperationStart?.({ ...operationInfo, isReplay: true });
      }

      checkpoint.markOperationState(
        stepId,
        OperationLifecycleState.IDLE_NOT_AWAITED,
        {
          metadata: {
            stepId,
            name,
            type: OperationType.DISTRIBUTED_MAP,
            subType: OperationSubType.DISTRIBUTED_MAP,
            parentId,
          },
        },
      );

      log("✅", "Distributed map phase 1 complete:", { stepId });
    })();

    phase1Promise.catch(() => {});

    return new DurablePromise(async () => {
      await phase1Promise;

      const resolve = async (): Promise<
        DistributedMapSummary | DistributedMapResult<TOutput>
      > => {
        const stepData = context.getStepData(stepId);
        // Every terminal state resolves with the summary, matching map and parallel.
        // throwIfError() is how a caller opts into raising.
        const details = stepData?.DistributedMapDetails;
        if (!details) {
          throw new DistributedMapError(
            "DISTRIBUTED_MAP operation completed but carried no DistributedMapDetails",
          );
        }
        return (await buildSummary(
          details,
          resolvedConfig,
          stepId,
          name,
          context,
          defaultS,
          stepData?.Status,
        )) as DistributedMapSummary | DistributedMapResult<TOutput>;
      };

      if (isCompleted) {
        return resolve();
      }

      log("🗺️", "Distributed map phase 2:", { stepId });
      checkpoint.markOperationAwaited(stepId);
      await checkpoint.waitForStatusChange(stepId);

      checkAndUpdateReplayMode?.();
      checkpoint.markOperationState(stepId, OperationLifecycleState.COMPLETED);
      const stepData = context.getStepData(stepId);
      const operationInfo = toOperationInfo(stepData);
      backfillOperationInfo(operationInfo, opInfo);
      await fireOperationEnd(
        plugin,
        operationInfo,
        false,
        stepData?.Status,
        stepData?.DistributedMapDetails?.Error,
      );

      return resolve();
    });
  }

  // The overloads pick the return type from `collectResults`, which the single implementation signature cannot express.
  return distributedMapHandler as ReturnType<
    typeof createDistributedMapHandler
  >;
};
