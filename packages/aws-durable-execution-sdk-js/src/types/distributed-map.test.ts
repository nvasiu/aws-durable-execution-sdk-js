import {
  assertSourceConfig,
  assertProcessorConfig,
  assertDestinationConfig,
  assertCompletionConfig,
  DistributedMapCompletionConfig,
  DistributedMapS3SourceConfig,
} from "./distributed-map";

const s3Source = (s3: Partial<DistributedMapS3SourceConfig>): unknown => ({
  s3: { bucket: "my-bucket", ...s3 },
});

describe("Distributed map function-name validation", () => {
  it("accepts a bare name, a qualified name, and a full ARN within the limit", () => {
    expect(() =>
      assertProcessorConfig({ functionName: "my-fn" }),
    ).not.toThrow();
    expect(() =>
      assertProcessorConfig({ functionName: "my-fn:PROD" }),
    ).not.toThrow();
    expect(() =>
      assertProcessorConfig({
        functionName: "arn:aws:lambda:us-east-1:123456789012:function:my-fn",
      }),
    ).not.toThrow();
    expect(() =>
      assertProcessorConfig({ functionName: "a".repeat(170) }),
    ).not.toThrow();
  });

  it("rejects an empty name", () => {
    expect(() => assertProcessorConfig({ functionName: "" })).toThrow(
      /non-empty/,
    );
    expect(() => assertSourceConfig({ reader: { functionName: "" } })).toThrow(
      /non-empty/,
    );
  });

  it("rejects a name longer than 170 characters", () => {
    expect(() =>
      assertProcessorConfig({ functionName: "a".repeat(171) }),
    ).toThrow(/170/);
    expect(() =>
      assertSourceConfig({ reader: { functionName: "a".repeat(171) } }),
    ).toThrow(/170/);
  });
});

describe("Distributed map durableExecutionNamePrefix validation", () => {
  it("accepts a prefix of 1 to 36 characters", () => {
    expect(() =>
      assertProcessorConfig({
        functionName: "fn",
        durableExecutionNamePrefix: "p",
      }),
    ).not.toThrow();
    expect(() =>
      assertProcessorConfig({
        functionName: "fn",
        durableExecutionNamePrefix: "a".repeat(36),
      }),
    ).not.toThrow();
  });

  it("rejects an empty or over-long prefix", () => {
    expect(() =>
      assertProcessorConfig({
        functionName: "fn",
        durableExecutionNamePrefix: "",
      }),
    ).toThrow(/between 1 and 36/);
    expect(() =>
      assertProcessorConfig({
        functionName: "fn",
        durableExecutionNamePrefix: "a".repeat(37),
      }),
    ).toThrow(/between 1 and 36/);
  });
});

describe("Distributed map processor option validation", () => {
  it("rejects a batchSize outside 1 to 10000", () => {
    expect(() =>
      assertProcessorConfig({ functionName: "fn", batchSize: 0 }),
    ).toThrow(/between 1 and 10000/);
    expect(() =>
      assertProcessorConfig({ functionName: "fn", batchSize: 10001 }),
    ).toThrow(/between 1 and 10000/);
  });

  it("rejects a retry duration outside 1 minute to 6 hours", () => {
    expect(() =>
      assertProcessorConfig({
        functionName: "fn",
        maxRetryDuration: { seconds: 30 },
      }),
    ).toThrow(/1 minute and 6 hours/);
  });

  it("rejects a negative retry attempt count", () => {
    expect(() =>
      assertProcessorConfig({ functionName: "fn", maxRetryAttempts: -1 }),
    ).toThrow(/non-negative integer/);
  });

  it('accepts "unlimited" and a non-negative integer attempt count', () => {
    expect(() =>
      assertProcessorConfig({
        functionName: "fn",
        maxRetryAttempts: "unlimited",
      }),
    ).not.toThrow();
    expect(() =>
      assertProcessorConfig({ functionName: "fn", maxRetryAttempts: 0 }),
    ).not.toThrow();
  });

  it("rejects an unknown responseMode", () => {
    expect(() =>
      assertProcessorConfig({ functionName: "fn", responseMode: "everything" }),
    ).toThrow(/Invalid responseMode/);
  });

  it("rejects a processor that is not an object", () => {
    expect(() => assertProcessorConfig("fn")).toThrow(/Invalid processor/);
  });
});

describe("Distributed map source shape validation", () => {
  it("accepts an array of items", () => {
    expect(() => assertSourceConfig([1, 2])).not.toThrow();
  });

  it("accepts each source kind", () => {
    expect(() => assertSourceConfig({ inline: { items: [1] } })).not.toThrow();
    expect(() =>
      assertSourceConfig(s3Source({ key: "k", format: "JSON_LINES" })),
    ).not.toThrow();
    expect(() =>
      assertSourceConfig({ reader: { functionName: "reader-fn" } }),
    ).not.toThrow();
  });

  it("rejects a source with no kind or more than one kind", () => {
    expect(() => assertSourceConfig({})).toThrow(
      /exactly one of inline, s3 or reader/,
    );
    expect(() =>
      assertSourceConfig({
        inline: { items: [] },
        reader: { functionName: "fn" },
      }),
    ).toThrow(/exactly one of inline, s3 or reader/);
  });

  it("rejects a non-object source", () => {
    expect(() => assertSourceConfig(42)).toThrow(/Invalid source/);
  });

  it("rejects inline items that are not an array", () => {
    expect(() => assertSourceConfig({ inline: { items: "x" } })).toThrow(
      /must be an array/,
    );
  });

  it("rejects a non-positive or non-integer maxItems", () => {
    expect(() =>
      assertSourceConfig({ inline: { items: [] }, maxItems: 0 }),
    ).toThrow(/positive integer/);
    expect(() =>
      assertSourceConfig({ inline: { items: [] }, maxItems: 1.5 }),
    ).toThrow(/positive integer/);
  });
});

describe("Distributed map S3 source validation", () => {
  it("rejects an empty bucket", () => {
    expect(() =>
      assertSourceConfig(s3Source({ bucket: "", key: "k", format: "CSV" })),
    ).toThrow(/bucket/);
  });

  it("rejects both or neither of key and prefix", () => {
    expect(() =>
      assertSourceConfig(
        s3Source({ key: "k", prefix: "p/", format: "JSON_LINES" }),
      ),
    ).toThrow(/exactly one of key or prefix/);
    expect(() =>
      assertSourceConfig(s3Source({ format: "JSON_LINES" })),
    ).toThrow(/exactly one of key or prefix/);
  });

  it("rejects an empty key, and a key with no format", () => {
    expect(() =>
      assertSourceConfig(s3Source({ key: "", format: "JSON_LINES" })),
    ).toThrow(/key must be non-empty/);
    expect(() => assertSourceConfig(s3Source({ key: "k" }))).toThrow(
      /requires a format/,
    );
  });

  it("accepts a prefix with no format", () => {
    expect(() => assertSourceConfig(s3Source({ prefix: "p/" }))).not.toThrow();
  });

  it("rejects csv options when the format is not CSV", () => {
    expect(() =>
      assertSourceConfig(
        s3Source({ key: "k", format: "JSON_LINES", csv: { headers: ["a"] } }),
      ),
    ).toThrow(/only valid when format is CSV/);
  });

  it("rejects headers and expectedColumns set together", () => {
    expect(() =>
      assertSourceConfig(
        s3Source({
          key: "k",
          format: "CSV",
          csv: { headers: ["a"], expectedColumns: ["a"] },
        }),
      ),
    ).toThrow(/mutually exclusive/);
  });

  it("accepts as const column arrays", () => {
    const columns = ["id", "name"] as const;
    const s3: DistributedMapS3SourceConfig = {
      bucket: "my-bucket",
      key: "k",
      format: "CSV",
      csv: { expectedColumns: columns },
    };
    expect(() => assertSourceConfig({ s3 })).not.toThrow();
    expect(() =>
      assertSourceConfig(
        s3Source({ key: "k", format: "CSV", csv: { headers: columns } }),
      ),
    ).not.toThrow();
  });

  it("rejects empty or duplicate CSV columns", () => {
    expect(() =>
      assertSourceConfig(
        s3Source({ key: "k", format: "CSV", csv: { headers: [] } }),
      ),
    ).toThrow(/non-empty/);
    expect(() =>
      assertSourceConfig(
        s3Source({ key: "k", format: "CSV", csv: { headers: ["a", "a"] } }),
      ),
    ).toThrow(/duplicates/);
  });

  it("rejects an expectedBucketOwner that is not exactly 12 digits", () => {
    expect(() =>
      assertSourceConfig(
        s3Source({
          key: "k",
          format: "JSON_LINES",
          expectedBucketOwner: "12345",
        }),
      ),
    ).toThrow(/12 digits/);
    expect(() =>
      assertSourceConfig(
        s3Source({
          key: "k",
          format: "JSON_LINES",
          expectedBucketOwner: "12345678901a",
        }),
      ),
    ).toThrow(/12 digits/);
  });
});

describe("Distributed map destination validation", () => {
  it("accepts an absent destination and the default include settings", () => {
    expect(() => assertDestinationConfig(undefined)).not.toThrow();
    expect(() =>
      assertDestinationConfig({
        onSuccess: { bucket: "b", prefix: "out/" },
        onFailure: { bucket: "b", prefix: "err/" },
      }),
    ).not.toThrow();
  });

  it("rejects a success destination that includes neither input nor output", () => {
    expect(() =>
      assertDestinationConfig({
        onSuccess: {
          bucket: "b",
          prefix: "out/",
          includeInput: false,
          includeOutput: false,
        },
      }),
    ).toThrow(/at least one of input or output/);
  });

  it("rejects a failure destination that includes neither input nor error", () => {
    expect(() =>
      assertDestinationConfig({
        onFailure: {
          bucket: "b",
          prefix: "err/",
          includeInput: false,
          includeError: false,
        },
      }),
    ).toThrow(/at least one of input or error/);
  });

  it("rejects an empty bucket and a bad expectedBucketOwner", () => {
    expect(() =>
      assertDestinationConfig({ onSuccess: { bucket: "", prefix: "out/" } }),
    ).toThrow(/bucket/);
    expect(() =>
      assertDestinationConfig({
        onFailure: { bucket: "b", prefix: "err/", expectedBucketOwner: "1" },
      }),
    ).toThrow(/12 digits/);
  });
});

describe("Distributed map completion config validation", () => {
  it("accepts an absent config, a count form, and a percentage form", () => {
    expect(() => assertCompletionConfig(undefined)).not.toThrow();
    expect(() =>
      assertCompletionConfig({ toleratedFailureCount: 0 }),
    ).not.toThrow();
    expect(() =>
      assertCompletionConfig({
        toleratedFailurePercentage: 10,
        minimumSampleSize: 50,
      }),
    ).not.toThrow();
  });

  it("rejects count and percentage set together", () => {
    expect(() =>
      assertCompletionConfig({
        toleratedFailureCount: 1,
        toleratedFailurePercentage: 10,
      } as unknown as DistributedMapCompletionConfig),
    ).toThrow(/mutually exclusive/);
  });

  it("rejects minimumSampleSize without a percentage", () => {
    expect(() =>
      assertCompletionConfig({
        minimumSampleSize: 50,
      } as unknown as DistributedMapCompletionConfig),
    ).toThrow(/minimumSampleSize/);
  });

  it("rejects a negative count", () => {
    expect(() => assertCompletionConfig({ toleratedFailureCount: -1 })).toThrow(
      /non-negative integer/,
    );
  });

  it("rejects a percentage outside 0 to 100", () => {
    expect(() =>
      assertCompletionConfig({ toleratedFailurePercentage: 101 }),
    ).toThrow(/Invalid toleratedFailurePercentage: 101/);
  });

  it("rejects a minimumSampleSize below 1", () => {
    expect(() =>
      assertCompletionConfig({
        toleratedFailurePercentage: 10,
        minimumSampleSize: 0,
      }),
    ).toThrow(/at least 1/);
  });
});
