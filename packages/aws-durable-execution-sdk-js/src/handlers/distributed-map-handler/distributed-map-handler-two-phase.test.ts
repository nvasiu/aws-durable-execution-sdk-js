import { createDistributedMapHandler } from "./distributed-map-handler";
import {
  ExecutionContext,
  DistributedMapProcessor,
  DistributedMapSource,
  DistributedMapSummary,
} from "../../types";
import { DurablePromise } from "../../types/durable-promise";
import { OperationStatus } from "../../types/wire";
import { Checkpoint } from "../../utils/checkpoint/checkpoint-helper";

jest.mock("../../utils/logger/logger");

describe("Distributed map handler two-phase execution", () => {
  let mockContext: ExecutionContext;
  let mockCheckpoint: Checkpoint;
  let createStepId: () => string;
  const processor = DistributedMapProcessor.reportBatchOutcome("proc-fn");

  beforeEach(() => {
    jest.clearAllMocks();
    mockContext = {
      getStepData: jest.fn().mockReturnValue(null),
      terminationManager: { terminate: jest.fn() },
      durableExecutionArn: "test-arn",
      isOperationUpdatedBetweenInvocation: jest.fn().mockReturnValue(false),
    } as any;
    mockCheckpoint = {
      checkpoint: jest.fn().mockResolvedValue(undefined),
      markOperationState: jest.fn(),
      markOperationAwaited: jest.fn(),
      waitForStatusChange: jest.fn().mockResolvedValue(undefined),
    } as any;
    createStepId = jest.fn().mockReturnValue("step-1");
  });

  it("terminates for maxConcurrency=0 without throwing synchronously", async () => {
    const handler = createDistributedMapHandler(
      mockContext,
      mockCheckpoint,
      createStepId,
    );

    // The returned promise never settles (the execution is terminated), so we
    // don't await it -- just assert the termination was requested.
    void handler([], processor, 0);
    await Promise.resolve();

    expect(mockContext.terminationManager.terminate).toHaveBeenCalledWith(
      expect.objectContaining({
        reason: "CONFIG_VALIDATION_ERROR",
        message: expect.stringContaining("Invalid maxConcurrency: 0"),
      }),
    );
  });

  it("terminates for a non-array non-source value without throwing synchronously", async () => {
    const handler = createDistributedMapHandler(
      mockContext,
      mockCheckpoint,
      createStepId,
    );

    let promise: DurablePromise<DistributedMapSummary> | undefined;
    expect(() => {
      promise = handler(
        { not: "a source" } as any,
        processor,
        5,
      ) as DurablePromise<DistributedMapSummary>;
    }).not.toThrow();
    expect(promise).toBeInstanceOf(DurablePromise);

    await Promise.resolve();

    expect(mockContext.terminationManager.terminate).toHaveBeenCalledWith(
      expect.objectContaining({
        reason: "CONFIG_VALIDATION_ERROR",
        message: expect.stringContaining("Invalid source"),
      }),
    );
  });

  it("rejects on await when an inline source exceeds the 1 MB limit", async () => {
    const handler = createDistributedMapHandler(
      mockContext,
      mockCheckpoint,
      createStepId,
    );

    const oversized = [{ blob: "x".repeat(1_200_000) }];
    const promise = handler(
      oversized,
      processor,
      2,
    ) as DurablePromise<DistributedMapSummary>;

    await expect(promise).rejects.toThrow("exceeds the 1 MB limit");
  });

  it("sends a non-JSON serdes output as the inline item, unchanged", async () => {
    (mockContext.getStepData as jest.Mock)
      .mockReturnValueOnce(null)
      .mockReturnValue({
        Status: OperationStatus.SUCCEEDED,
        DistributedMapDetails: {
          SuccessCount: 1,
          FailureCount: 0,
        },
      });

    const handler = createDistributedMapHandler(
      mockContext,
      mockCheckpoint,
      createStepId,
    );

    const source = DistributedMapSource.inline([1], {
      serdes: {
        serialize: async (): Promise<string> => "not json {",
        deserialize: async (d: string | undefined): Promise<number> =>
          d as unknown as number,
      },
    });
    const result = await (handler(
      source,
      processor,
      2,
    ) as DurablePromise<DistributedMapSummary>);

    const update = (mockCheckpoint.checkpoint as jest.Mock).mock.calls[0][1];
    expect(
      update.DistributedMapOptions.Source.InlineSourceConfig.Items,
    ).toEqual(["not json {"]);
    expect(result.status).toBe("SUCCEEDED");
  });

  it("resolves a FAILED run with its counts instead of throwing", async () => {
    (mockContext.getStepData as jest.Mock)
      .mockReturnValueOnce(null)
      .mockReturnValue({
        Status: OperationStatus.FAILED,
        DistributedMapDetails: {
          TotalCount: 10,
          SuccessCount: 7,
          FailureCount: 3,
          UnprocessedCount: 0,
        },
      });

    const handler = createDistributedMapHandler(
      mockContext,
      mockCheckpoint,
      createStepId,
    );

    const result = await (handler(
      [],
      processor,
      2,
    ) as DurablePromise<DistributedMapSummary>);

    expect(result.status).toBe("FAILED");
    expect(result.successCount).toBe(7);
    expect(result.failureCount).toBe(3);
    expect(result.totalCount).toBe(10);
    expect(result.hasFailure).toBe(true);
    // Raising is opt-in, the same contract map and parallel use.
    expect(() => result.throwIfError()).toThrow("Map run ended FAILED");
  });

  it("checkpoints START in phase 1 then resolves after phase-2 waitForStatusChange", async () => {
    (mockContext.getStepData as jest.Mock)
      .mockReturnValueOnce(null) // phase 1: no step data yet -> triggers START checkpoint
      .mockReturnValue({
        Status: OperationStatus.SUCCEEDED,
        DistributedMapDetails: {
          SuccessCount: 0,
          FailureCount: 0,
        },
      });

    const handler = createDistributedMapHandler(
      mockContext,
      mockCheckpoint,
      createStepId,
    );

    const promise = handler(
      [],
      processor,
      2,
    ) as DurablePromise<DistributedMapSummary>;
    const result = await promise;

    expect(mockCheckpoint.checkpoint).toHaveBeenCalledTimes(1);
    expect(mockCheckpoint.markOperationAwaited).toHaveBeenCalledWith("step-1");
    expect(mockCheckpoint.waitForStatusChange).toHaveBeenCalledWith("step-1");
    expect(result.status).toBe("SUCCEEDED");
  });
});
