import { createDistributedMapHandler } from "./distributed-map-handler";
import { ExecutionContext } from "../../types";
import { OperationStatus } from "../../types/wire";
import { Checkpoint } from "../../utils/checkpoint/checkpoint-helper";
import { DurableInstrumentationPlugin } from "../../types/plugin";

jest.mock("../../utils/logger/logger");

describe("Distributed map handler - plugin hooks", () => {
  let mockContext: ExecutionContext;
  let mockCheckpoint: Checkpoint;
  let createStepId: () => string;
  let mockPlugin: jest.Mocked<DurableInstrumentationPlugin>;
  const processor = { functionName: "proc-fn" };

  const succeededDetails = {
    Status: "SUCCEEDED",
    CompletionReason: "ALL_COMPLETED",
    SuccessCount: 0,
    FailureCount: 0,
  };

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
    mockPlugin = {
      onOperationStart: jest.fn(),
      onOperationEnd: jest.fn(),
      onOperationAttemptStart: jest.fn(),
      onOperationAttemptEnd: jest.fn(),
    };
  });

  const makeHandler = () =>
    createDistributedMapHandler(
      mockContext,
      mockCheckpoint,
      createStepId,
      undefined,
      jest.fn(),
      undefined,
      mockPlugin,
    );

  it("calls onOperationEnd with no error on a replay-succeeded operation", async () => {
    (mockContext.getStepData as jest.Mock).mockReturnValue({
      Status: OperationStatus.SUCCEEDED,
      DistributedMapDetails: succeededDetails,
    });

    await makeHandler()([], processor, 2);

    expect(mockPlugin.onOperationEnd).toHaveBeenCalledTimes(1);
    expect(mockPlugin.onOperationEnd).toHaveBeenCalledWith(
      expect.objectContaining({ isReplay: true }),
    );
    expect(mockPlugin.onOperationEnd).toHaveBeenCalledWith(
      expect.not.objectContaining({ error: expect.anything() }),
    );
  });

  it("calls onOperationEnd with no error on a phase-2 succeeded operation", async () => {
    (mockContext.getStepData as jest.Mock)
      .mockReturnValueOnce(null) // phase 1: no step data -> START checkpoint
      .mockReturnValue({
        Status: OperationStatus.SUCCEEDED,
        DistributedMapDetails: succeededDetails,
      });

    await makeHandler()([], processor, 2);

    expect(mockPlugin.onOperationStart).toHaveBeenCalledTimes(1);
    expect(mockPlugin.onOperationStart).toHaveBeenCalledWith(
      expect.objectContaining({ isReplay: false }),
    );
    expect(mockPlugin.onOperationEnd).toHaveBeenCalledTimes(1);
    expect(mockPlugin.onOperationEnd).toHaveBeenCalledWith(
      expect.objectContaining({ isReplay: false }),
    );
    expect(mockPlugin.onOperationEnd).toHaveBeenCalledWith(
      expect.not.objectContaining({ error: expect.anything() }),
    );
  });

  it.each([
    ["FAILED", OperationStatus.FAILED],
    ["STOPPED", OperationStatus.STOPPED],
    ["TIMED_OUT", OperationStatus.TIMED_OUT],
  ])(
    "calls onOperationEnd with an error on an operation-level %s status",
    async (_label, status) => {
      (mockContext.getStepData as jest.Mock).mockReturnValue({
        Status: status,
        DistributedMapDetails: { CompletionReason: "ALL_COMPLETED" },
      });

      // The operation resolves with the summary; only the plugin sees the error.
      const result = await makeHandler()([], processor, 2);
      expect(result.status).toBe(_label);

      expect(mockPlugin.onOperationEnd).toHaveBeenCalledTimes(1);
      expect(mockPlugin.onOperationEnd).toHaveBeenCalledWith(
        expect.objectContaining({ error: expect.any(Error) }),
      );
    },
  );

  it("does not throw when plugin hooks are undefined", async () => {
    (mockContext.getStepData as jest.Mock).mockReturnValue({
      Status: OperationStatus.SUCCEEDED,
      DistributedMapDetails: succeededDetails,
    });

    const handler = createDistributedMapHandler(
      mockContext,
      mockCheckpoint,
      createStepId,
      undefined,
      jest.fn(),
      undefined,
      {},
    );

    const result = await handler([], processor, 2);
    expect(result.status).toBe("SUCCEEDED");
  });
});
