import {
  buildSummary,
  toDistributedMapDestinationOptions,
  toDistributedMapProcessorOptions,
  toDistributedMapS3SourceOptions,
} from "./distributed-map-handler";
import {
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
    CompletionReason: "ALL_COMPLETED",
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

  it("serializes a processor retry config with only a duration set", () => {
    const wire = toDistributedMapProcessorOptions({
      functionName: "proc-fn",
      maxRetryDuration: { minutes: 5 },
    });

    expect(wire.MaxRetryDurationSeconds).toBe(300);
    expect(wire.MaxRetryAttempts).toBeUndefined();
  });
});

describe("Distributed map handler enum parsing", () => {
  const cfg = {};

  it("throws when CompletionReason is missing on a completed run", async () => {
    await expect(
      buildSummary(
        { FailureCount: 0 },
        cfg,
        "step-0",
        undefined,
        fakeContext,
        defaultSerdes,
        "SUCCEEDED",
      ),
    ).rejects.toThrow(DistributedMapError);
  });

  it("maps an unrecognized CompletionReason to the UNKNOWN_TO_SDK_VERSION sentinel", async () => {
    const summary = await buildSummary(
      { CompletionReason: "BRAND_NEW_REASON", FailureCount: 0 },
      cfg,
      "step-0",
      undefined,
      fakeContext,
      defaultSerdes,
      "SUCCEEDED",
    );

    expect(summary.completionReason).toBe("UNKNOWN_TO_SDK_VERSION");
  });

  it("passes a known CompletionReason through unchanged", async () => {
    const summary = await buildSummary(
      { CompletionReason: "ALL_COMPLETED", FailureCount: 0 },
      cfg,
      "step-0",
      undefined,
      fakeContext,
      defaultSerdes,
      "SUCCEEDED",
    );

    expect(summary.completionReason).toBe("ALL_COMPLETED");
    expect(summary.status).toBe("SUCCEEDED");
  });

  it("derives the run status from the operation status", async () => {
    const summary = await buildSummary(
      { CompletionReason: "ALL_COMPLETED", FailureCount: 0 },
      cfg,
      "step-0",
      undefined,
      fakeContext,
      defaultSerdes,
      "SUCCEEDED",
    );

    expect(summary.status).toBe("SUCCEEDED");
    expect(summary.completionReason).toBe("ALL_COMPLETED");
  });

  it("reports a non-succeeded operation status as the run status", async () => {
    const summary = await buildSummary(
      { CompletionReason: "FAILURE_TOLERANCE_EXCEEDED", FailureCount: 1 },
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
      { CompletionReason: "ALL_COMPLETED" },
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
        { CompletionReason: "ALL_COMPLETED" },
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
        CompletionReason: "ALL_COMPLETED",
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
          CompletionReason: "ALL_COMPLETED",
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
        CompletionReason: "ALL_COMPLETED",
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
        CompletionReason: "ALL_COMPLETED",
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
        CompletionReason: "ALL_COMPLETED",
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
    CompletionReason: "ALL_COMPLETED",
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
        CompletionReason: "FAILURE_TOLERANCE_EXCEEDED",
        SuccessCount: 0,
        FailureCount: 1,
        UnprocessedCount: 0,
        Results: [{ ItemId: "0", Status: "FAILED" }],
      },
      "FAILED",
    );
    expect(() => result.throwIfError()).toThrow("Map run ended FAILED");
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
        CompletionReason: "ALL_COMPLETED",
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

describe("Distributed map config to wire mapping", () => {
  it("maps a key to an object source", () => {
    expect(
      toDistributedMapS3SourceOptions({
        bucket: "b",
        key: "k",
        format: "JSON_ARRAY",
      }),
    ).toEqual({
      Bucket: "b",
      Key: "k",
      Format: "JSON_ARRAY",
    });
  });

  it("maps a prefix with a format to a flattened prefix source", () => {
    expect(
      toDistributedMapS3SourceOptions({
        bucket: "b",
        prefix: "p/",
        format: "JSON_LINES",
      }),
    ).toEqual({
      Bucket: "b",
      KeyPrefix: "p/",
      Transform: "LOAD_AND_FLATTEN",
      Format: "JSON_LINES",
    });
  });

  it("maps a prefix with no format to an object listing", () => {
    expect(
      toDistributedMapS3SourceOptions({ bucket: "b", prefix: "p/" }),
    ).toEqual({
      Bucket: "b",
      KeyPrefix: "p/",
      Transform: "NONE",
    });
  });

  it("defaults a CSV source to the first row as header and a COMMA delimiter", () => {
    expect(
      toDistributedMapS3SourceOptions({ bucket: "b", key: "k", format: "CSV" })
        .CsvFormatOptions,
    ).toEqual({
      HeaderLocation: "FIRST_ROW",
      Delimiter: "COMMA",
    });
  });

  it("maps CSV headers to a GIVEN header location", () => {
    expect(
      toDistributedMapS3SourceOptions({
        bucket: "b",
        key: "k",
        format: "CSV",
        csv: { headers: ["a"], delimiter: "TAB" },
      }).CsvFormatOptions,
    ).toEqual({ HeaderLocation: "GIVEN", Headers: ["a"], Delimiter: "TAB" });
  });

  it("sends no response type when responseMode is absent", () => {
    expect(
      toDistributedMapProcessorOptions({ functionName: "fn" })
        .FunctionResponseTypes,
    ).toBeUndefined();
    expect(
      toDistributedMapProcessorOptions({
        functionName: "fn",
        responseMode: "itemResults",
      }).FunctionResponseTypes,
    ).toEqual(["REPORT_BATCH_ITEM_RESULTS"]);
  });

  it("applies the destination include defaults", () => {
    const wire = toDistributedMapDestinationOptions({
      destination: {
        onSuccess: { bucket: "b", prefix: "out/" },
        onFailure: { bucket: "b", prefix: "err/" },
      },
    });
    expect(wire?.OnSuccess?.Include).toEqual(["OUTPUT"]);
    expect(wire?.OnSuccess?.S3DestinationConfig?.KeyPrefix).toBe("out/");
    expect(wire?.OnFailure?.Include).toEqual(["INPUT", "ERROR"]);
  });
});
