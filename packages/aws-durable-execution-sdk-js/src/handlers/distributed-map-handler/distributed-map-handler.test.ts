import {
  buildSummary,
  toDistributedMapDestinationOptions,
} from "./distributed-map-handler";
import {
  DistributedMapDestination,
  DistributedMapItemStatus,
  DistributedMapResult,
  DistributedMapStatus,
} from "../../types";
import { DistributedMapDetails } from "../../types/wire";
import { AnySerdes, defaultSerdes } from "../../utils/serdes/serdes";
import {
  DistributedMapError,
  DistributedMapItemError,
} from "../../errors/durable-error/durable-error";

// Minimal context: the success decode path never touches the termination manager.
const fakeContext = {
  terminationManager: {} as any,
  durableExecutionArn: "arn:test",
} as any;

function detailsWith(
  results: NonNullable<DistributedMapDetails["Results"]>,
): DistributedMapDetails {
  return {
    SuccessCount: results.length,
    FailureCount: 0,
    UnprocessedCount: 0,
    Results: results,
  };
}

describe("Distributed map handler second-pass fixes", () => {
  it("runs a custom item serdes on the decode path when collecting results", async () => {
    // Custom serdes only defines decode: doubles the JSON number it reads back.
    const doublingSerdes: AnySerdes = {
      serialize: async (v) => JSON.stringify(v),
      deserialize: async (data) => (JSON.parse(data as string) as number) * 2,
    };
    const details = detailsWith([
      { ItemId: "0", Status: "SUCCEEDED", Output: "21" },
    ]);

    const result = (await buildSummary(
      details,
      { collectResults: true, resultSerdes: doublingSerdes },
      "step-0",
      undefined,
      fakeContext,
      defaultSerdes,
      "SUCCEEDED",
    )) as DistributedMapResult<number>;

    expect(result.getResults()).toEqual([42]);
  });

  it("round-trips falsy per-item outputs without dropping them", async () => {
    const details = detailsWith([
      { ItemId: "0", Status: "SUCCEEDED", Output: "0" },
      { ItemId: "1", Status: "SUCCEEDED", Output: "false" },
      { ItemId: "2", Status: "SUCCEEDED", Output: '""' },
      { ItemId: "3", Status: "SUCCEEDED", Output: "[]" },
      { ItemId: "4", Status: "SUCCEEDED", Output: "{}" },
    ]);

    const result = (await buildSummary(
      details,
      { collectResults: true },
      "step-0",
      undefined,
      fakeContext,
      defaultSerdes,
      "SUCCEEDED",
    )) as DistributedMapResult<unknown>;

    expect(result.getResults()).toEqual([0, false, "", [], {}]);
    expect(result.getResults()).toHaveLength(5);
  });
});

describe("Distributed map handler enum parsing", () => {
  const cfg = {};

  it("resolves a completed run that carries only counts", async () => {
    const summary = await buildSummary(
      { FailureCount: 0 },
      cfg,
      "step-0",
      undefined,
      fakeContext,
      defaultSerdes,
      "SUCCEEDED",
    );

    expect(summary.status).toBe("SUCCEEDED");
    expect(summary.sourceExhausted).toBeUndefined();
    expect(summary.error).toBeUndefined();
  });

  it("throws when a created run carries no SourceExhausted", async () => {
    await expect(
      buildSummary(
        { DistributedMapRunArn: "arn:run", FailureCount: 0 },
        cfg,
        "step-0",
        undefined,
        fakeContext,
        defaultSerdes,
        "SUCCEEDED",
      ),
    ).rejects.toThrow("missing distributed map SourceExhausted from backend");
  });

  it("accepts a created run that carries SourceExhausted", async () => {
    const summary = await buildSummary(
      {
        DistributedMapRunArn: "arn:run",
        SourceExhausted: true,
        FailureCount: 0,
      },
      cfg,
      "step-0",
      undefined,
      fakeContext,
      defaultSerdes,
      "SUCCEEDED",
    );
    expect(summary.distributedMapRunArn).toBe("arn:run");
    expect(summary.sourceExhausted).toBe(true);
  });

  it("passes SourceExhausted and the run Error through", async () => {
    const error = {
      ErrorType: "SourceReadError",
      ErrorMessage: "object not found",
    };
    const summary = await buildSummary(
      { SourceExhausted: false, Error: error, FailureCount: 0 },
      cfg,
      "step-0",
      undefined,
      fakeContext,
      defaultSerdes,
      "FAILED",
    );

    expect(summary.sourceExhausted).toBe(false);
    expect(summary.error).toEqual(error);
  });

  it("derives the run status from the operation status", async () => {
    const summary = await buildSummary(
      { FailureCount: 0 },
      cfg,
      "step-0",
      undefined,
      fakeContext,
      defaultSerdes,
      "SUCCEEDED",
    );

    expect(summary.status).toBe("SUCCEEDED");
  });

  it("reports a non-succeeded operation status as the run status", async () => {
    const summary = await buildSummary(
      { FailureCount: 1 },
      cfg,
      "step-0",
      undefined,
      fakeContext,
      defaultSerdes,
      "FAILED",
    );

    expect(summary.status).toBe("FAILED");
  });

  it("maps an unknown run Status to the UNKNOWN_TO_SDK_VERSION sentinel", async () => {
    const summary = await buildSummary(
      {},
      cfg,
      "step-0",
      undefined,
      fakeContext,
      defaultSerdes,
      "NONSENSE",
    );
    expect(summary.status).toBe(DistributedMapStatus.UNKNOWN_TO_SDK_VERSION);
    expect(() => summary.throwIfError()).toThrow(DistributedMapError);
  });

  it("throws on a missing run Status", async () => {
    await expect(
      buildSummary(
        {},
        cfg,
        "step-0",
        undefined,
        fakeContext,
        defaultSerdes,
        undefined,
      ),
    ).rejects.toThrow(DistributedMapError);
  });

  it("maps an unknown per-item Status to the UNKNOWN_TO_SDK_VERSION sentinel", async () => {
    const result = (await buildSummary(
      {
        FailureCount: 0,
        Results: [{ ItemId: "0", Status: "BOGUS" }],
      },
      { collectResults: true },
      "step-0",
      undefined,
      fakeContext,
      defaultSerdes,
      "SUCCEEDED",
    )) as DistributedMapResult<unknown>;
    expect(result.all[0].status).toBe(
      DistributedMapItemStatus.UNKNOWN_TO_SDK_VERSION,
    );
    expect(result.succeeded()).toHaveLength(0);
    expect(result.failed()).toHaveLength(0);
  });

  it("throws on a missing per-item Status when collecting results", async () => {
    await expect(
      buildSummary(
        {
          FailureCount: 0,
          Results: [{ ItemId: "0" }],
        },
        { collectResults: true },
        "step-0",
        undefined,
        fakeContext,
        defaultSerdes,
        "SUCCEEDED",
      ),
    ).rejects.toThrow(DistributedMapError);
  });
});

describe("Distributed map handler collected result access", () => {
  it("keeps a succeeded item with no output in getResults as undefined", async () => {
    const result = (await buildSummary(
      {
        SuccessCount: 1,
        FailureCount: 0,
        Results: [{ ItemId: "0", Status: "SUCCEEDED" }],
      },
      { collectResults: true },
      "step-0",
      undefined,
      fakeContext,
      defaultSerdes,
      "SUCCEEDED",
    )) as DistributedMapResult<unknown>;

    expect(result.getResults()).toEqual([undefined]);
    expect(result.succeeded()).toHaveLength(1);
    expect(result.all).toHaveLength(1);
  });

  it("keeps a failed item with no error in getErrors as undefined", async () => {
    const result = (await buildSummary(
      {
        SuccessCount: 0,
        FailureCount: 1,
        Results: [{ ItemId: "0", Status: "FAILED" }],
      },
      { collectResults: true },
      "step-0",
      undefined,
      fakeContext,
      defaultSerdes,
      "SUCCEEDED",
    )) as DistributedMapResult<unknown>;

    expect(result.failed()).toHaveLength(1);
    expect(result.getErrors()).toEqual([undefined]);
    expect(result.all).toHaveLength(1);
  });

  it("builds a failed item's error as a DistributedMapItemError", async () => {
    const result = (await buildSummary(
      {
        SuccessCount: 0,
        FailureCount: 1,
        Results: [
          {
            ItemId: "0",
            Status: "FAILED",
            Error: {
              ErrorType: "BoomError",
              ErrorMessage: "kaboom",
              ErrorData: "data",
            },
          },
        ],
      },
      { collectResults: true },
      "step-0",
      undefined,
      fakeContext,
      defaultSerdes,
      "SUCCEEDED",
    )) as DistributedMapResult<unknown>;

    const [error] = result.getErrors();
    expect(error).toBeInstanceOf(DistributedMapItemError);
    expect(error).toBeInstanceOf(DistributedMapError);
    expect(error?.errorType).toBe("DistributedMapItemError");
    expect(error?.message).toBe("kaboom");
    expect(error?.errorData).toBe("data");
    expect(error?.cause?.name).toBe("BoomError");
    expect(error?.cause?.message).toBe("kaboom");
  });
});

describe("Distributed map throwIfError", () => {
  const succeededDetails = (
    results: NonNullable<DistributedMapDetails["Results"]>,
    failureCount: number,
  ): DistributedMapDetails => ({
    SuccessCount: results.filter((r) => r.Status === "SUCCEEDED").length,
    FailureCount: failureCount,
    UnprocessedCount: 0,
    Results: results,
  });

  const buildResult = (
    details: DistributedMapDetails,
    opStatus = "SUCCEEDED",
  ) =>
    buildSummary(
      details,
      { collectResults: true },
      "step-0",
      undefined,
      fakeContext,
      defaultSerdes,
      opStatus,
    ) as Promise<DistributedMapResult<unknown>>;

  it("throws the first failed item's error under inline collection", async () => {
    const result = await buildResult(
      succeededDetails(
        [
          {
            ItemId: "0",
            Status: "FAILED",
            Error: { ErrorType: "BoomError", ErrorMessage: "kaboom" },
          },
        ],
        1,
      ),
    );
    let thrown: unknown;
    try {
      result.throwIfError();
    } catch (e) {
      thrown = e;
    }
    expect(thrown).toBe(result.getErrors()[0]);
    expect(thrown).toBeInstanceOf(DistributedMapItemError);
    expect((thrown as DistributedMapItemError).message).toBe("kaboom");
  });

  it("names the failed item when it carries no error detail", async () => {
    const result = await buildResult(
      succeededDetails([{ ItemId: "7", Status: "FAILED" }], 1),
    );
    expect(() => result.throwIfError()).toThrow("item 7 failed");
  });

  it("falls back to the failure count when no failed item is retained", async () => {
    const result = await buildResult(succeededDetails([], 2));
    expect(() => result.throwIfError()).toThrow(
      "Map run succeeded but 2 item(s) permanently failed",
    );
  });

  it("throws a run-level error for a non-succeeded run", async () => {
    const result = await buildResult(
      {
        SuccessCount: 0,
        FailureCount: 1,
        UnprocessedCount: 0,
        Results: [{ ItemId: "0", Status: "FAILED" }],
      },
      "FAILED",
    );
    expect(() => result.throwIfError()).toThrow("Map run ended FAILED");
  });

  it("throws the run Error's message, type and data for a non-succeeded run", async () => {
    const result = await buildResult(
      {
        Error: {
          ErrorType: "SourceReadError",
          ErrorMessage: "object not found",
          ErrorData: '{"key":"k"}',
          StackTrace: ["at reader"],
        },
        SuccessCount: 0,
        FailureCount: 0,
        UnprocessedCount: 0,
      },
      "FAILED",
    );
    let thrown: DistributedMapError | undefined;
    try {
      result.throwIfError();
    } catch (e) {
      thrown = e as DistributedMapError;
    }
    expect(thrown).toBeInstanceOf(DistributedMapError);
    expect(thrown?.message).toBe("Map run ended FAILED: object not found");
    expect(thrown?.cause?.name).toBe("SourceReadError");
    expect(thrown?.cause?.stack).toBe("at reader");
    expect(thrown?.errorData).toBe('{"key":"k"}');
  });

  it("does not throw on a clean success", async () => {
    const result = await buildResult(
      succeededDetails([{ ItemId: "0", Status: "SUCCEEDED", Output: "1" }], 0),
    );
    expect(() => result.throwIfError()).not.toThrow();
  });

  it("throws from the summary alone when result collection is off", async () => {
    const summary = await buildSummary(
      {
        SuccessCount: 0,
        FailureCount: 3,
      },
      {},
      "step-0",
      undefined,
      fakeContext,
      defaultSerdes,
      "SUCCEEDED",
    );
    expect(() => summary.throwIfError()).toThrow(
      "Map run succeeded but 3 item(s) permanently failed",
    );
  });
});

describe("Distributed map destination to wire mapping", () => {
  it("sends the destinations the factories built", () => {
    const wire = toDistributedMapDestinationOptions({
      destination: {
        onSuccess: DistributedMapDestination.s3.successes("s3://b/out/"),
        onFailure: DistributedMapDestination.s3.failures("s3://b/err/"),
      },
    });
    expect(wire?.OnSuccess?.Include).toEqual(["OUTPUT"]);
    expect(wire?.OnSuccess?.S3DestinationConfig?.KeyPrefix).toBe("out/");
    expect(wire?.OnFailure?.Include).toEqual(["INPUT", "ERROR"]);
  });

  it("sends no destination when neither is set", () => {
    expect(toDistributedMapDestinationOptions({})).toBeUndefined();
    expect(
      toDistributedMapDestinationOptions({ destination: {} }),
    ).toBeUndefined();
  });
});
