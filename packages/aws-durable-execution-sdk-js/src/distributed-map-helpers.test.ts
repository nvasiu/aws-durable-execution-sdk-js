import {
  createDistributedMapItemHandler,
  createDistributedMapBatchHandler,
  createDistributedMapReader,
  createDistributedMapItemHandlerWithDurableExecution,
  createDistributedMapBatchHandlerWithDurableExecution,
} from "./distributed-map-helpers";
import { BatchItemStatus } from "./types";
import { Serdes } from "./utils/serdes/serdes";
import { Context } from "aws-lambda";

// The durable variants delegate to context.map and withDurableExecution. Mock the
// wrapper to the identity so the test drives the inner handler directly, and supply
// a context whose map runs the mapper per item and reports terminal outcomes.
jest.mock("./with-durable-execution", () => ({
  withDurableExecution: (fn: unknown) => fn,
}));

// Distinguishable serdes: input decode adds 1000, output encode adds 1, so a test
// can prove the item serdes ran on the input and the result serdes ran on the output.
const inputSerdes: Serdes<number> = {
  serialize: async (v) => JSON.stringify(v),
  deserialize: async (d) => (JSON.parse(d as string) as number) + 1000,
};
const outputSerdes: Serdes<number> = {
  serialize: async (v) => JSON.stringify((v as number) + 1),
  deserialize: async (d) => JSON.parse(d as string) as number,
};

const event = (
  records: Array<{ itemId: string; body: unknown }>,
): {
  distributedMapRunArn: string;
  records: Array<{ itemId: string; body: unknown }>;
} => ({
  distributedMapRunArn: "arn:test",
  records,
});

// The wrappers ignore context; a minimal typed mock satisfies the required param.
const ctx = {} as Context;

describe("createDistributedMapItemHandler concurrency", () => {
  // Records the number of handlers in flight, so the default can be proven serial.
  const trackingHandler = (opts?: { concurrency?: number }) => {
    let inFlight = 0;
    let peak = 0;
    const handler = createDistributedMapItemHandler<number, number>(
      async (item) => {
        inFlight++;
        peak = Math.max(peak, inFlight);
        await new Promise((resolve) => setTimeout(resolve, 1));
        inFlight--;
        return item;
      },
      opts,
    );
    return { handler, peak: () => peak };
  };

  const fourRecords = event([
    { itemId: "0", body: "0" },
    { itemId: "1", body: "1" },
    { itemId: "2", body: "2" },
    { itemId: "3", body: "3" },
  ]);

  it("processes one item at a time by default", async () => {
    const { handler, peak } = trackingHandler();

    await handler(fourRecords, ctx);

    expect(peak()).toBe(1);
  });

  it("overlaps up to the configured concurrency", async () => {
    const { handler, peak } = trackingHandler({ concurrency: 3 });

    await handler(fourRecords, ctx);

    expect(peak()).toBe(3);
  });

  it("still accounts for every item when running serially", async () => {
    const { handler } = trackingHandler();

    const result = await handler(fourRecords, ctx);

    expect(result.batchItemResults).toHaveLength(4);
    expect(result.batchItemFailures).toEqual([]);
  });

  it.each([0, -1, 2.5])(
    "rejects concurrency %p at construction",
    (concurrency) => {
      expect(() =>
        createDistributedMapItemHandler<number, number>(async (i) => i, {
          concurrency,
        }),
      ).toThrow("concurrency must be an integer of 1 or more");
    },
  );
});

describe("createDistributedMapItemHandler serdes split", () => {
  it("decodes the input with itemSerdes and encodes the output with resultSerdes", async () => {
    const handler = createDistributedMapItemHandler<number, number>(
      (item) => item * 2,
      {
        itemSerdes: inputSerdes,
        resultSerdes: outputSerdes,
      },
    );

    const res = await handler(event([{ itemId: "0", body: 2 }]), ctx);

    // 2 -> (+1000) 1002 -> (*2) 2004 -> (+1) 2005
    expect(res.batchItemResults).toEqual([
      { itemIdentifier: "0", output: "2005" },
    ]);
    expect(res.batchItemFailures).toEqual([]);
  });

  it("round-trips through the default serdes when none is supplied", async () => {
    const handler = createDistributedMapItemHandler<number, number>(
      (item) => item * 2,
    );
    const res = await handler(event([{ itemId: "0", body: 21 }]), ctx);
    expect(res.batchItemResults).toEqual([
      { itemIdentifier: "0", output: "42" },
    ]);
  });

  it("sends a non-JSON resultSerdes output unchanged", async () => {
    const handler = createDistributedMapItemHandler<number, number>(
      (item) => item,
      {
        resultSerdes: {
          serialize: async () => "not json {",
          deserialize: async () => 0,
        },
      },
    );
    const res = await handler(event([{ itemId: "0", body: 1 }]), ctx);
    expect(res.batchItemResults).toEqual([
      { itemIdentifier: "0", output: "not json {" },
    ]);
    expect(res.batchItemFailures).toEqual([]);
  });

  it("records a thrown item as a failure", async () => {
    const handler = createDistributedMapItemHandler<number, number>(() => {
      throw new Error("boom");
    });
    const res = await handler(event([{ itemId: "7", body: 1 }]), ctx);
    expect(res.batchItemResults).toEqual([]);
    expect(res.batchItemFailures).toEqual([
      {
        itemIdentifier: "7",
        error: { errorType: "Error", errorMessage: "boom" },
      },
    ]);
  });

  it("reports only failures when report is 'failures'", async () => {
    const handler = createDistributedMapItemHandler<number, number>(
      (item) => item,
      {
        report: "failures",
      },
    );
    const res = await handler(event([{ itemId: "0", body: 1 }]), ctx);
    expect(res).toEqual({ batchItemFailures: [] });
  });

  it("rejects an invalid report value at construction", () => {
    expect(() =>
      createDistributedMapItemHandler<number, number>((i) => i, {
        report: "bogus" as any,
      }),
    ).toThrow(/results.*failures/);
  });

  it("rejects an event that is not a processor payload", async () => {
    const handler = createDistributedMapItemHandler<number, number>((i) => i);
    await expect(handler({} as any, ctx)).rejects.toThrow(/records/);
    await expect(handler(undefined as any, ctx)).rejects.toThrow(/records/);
  });
});

describe("createDistributedMapItemHandler thrown non-error and empty output", () => {
  it("records an item that throws undefined as a failure, not a success", async () => {
    const handler = createDistributedMapItemHandler<number, number>(() => {
      throw undefined;
    });
    const res = await handler(event([{ itemId: "3", body: 1 }]), ctx);
    expect(res.batchItemResults).toEqual([]);
    expect(res.batchItemFailures).toEqual([
      {
        itemIdentifier: "3",
        error: { errorType: "Error", errorMessage: "undefined" },
      },
    ]);
  });

  it("produces output null when the fn returns undefined and does not throw", async () => {
    const handler = createDistributedMapItemHandler<number, number | undefined>(
      () => undefined,
    );
    const res = await handler(event([{ itemId: "0", body: 1 }]), ctx);
    expect(res.batchItemResults).toEqual([
      { itemIdentifier: "0", output: null },
    ]);
    expect(res.batchItemFailures).toEqual([]);
  });

  it('serializes a null return as the string "null"', async () => {
    const handler = createDistributedMapItemHandler<number, number | null>(
      () => null,
    );
    const res = await handler(event([{ itemId: "0", body: 1 }]), ctx);
    expect(res.batchItemResults).toEqual([
      { itemIdentifier: "0", output: "null" },
    ]);
    expect(res.batchItemFailures).toEqual([]);
  });
});

describe("createDistributedMapBatchHandler", () => {
  it("decodes every record body with the input item serdes", async () => {
    const seen: number[] = [];
    const handler = createDistributedMapBatchHandler<number>(
      (items) => {
        seen.push(...items);
      },
      { itemSerdes: inputSerdes },
    );
    await handler(
      event([
        { itemId: "0", body: 2 },
        { itemId: "1", body: 3 },
      ]),
      ctx,
    );
    expect(seen).toEqual([1002, 1003]);
  });

  it("rejects an event that is not a processor payload", async () => {
    const handler = createDistributedMapBatchHandler<number>(() => {});
    await expect(handler({} as any, ctx)).rejects.toThrow(/records/);
  });
});

describe("createDistributedMapReader", () => {
  it("returns items and a serialized nextState", async () => {
    const reader = createDistributedMapReader<number, number>((state) => ({
      items: [1, 2],
      nextState: (state ?? 0) + 1,
    }));
    const res = await reader(
      { maxItemsPerPage: 10, distributedMapRunArn: "arn:test" },
      ctx,
    );
    expect(res.items).toEqual(["1", "2"]);
    expect(res.nextState).toBeDefined();
  });

  it("serializes each item with a non-JSON itemSerdes", async () => {
    const reader = createDistributedMapReader<number, number>(
      () => ({ items: [1, 2] }),
      {
        itemSerdes: {
          serialize: async (v) => `item-${v}`,
          deserialize: async () => 0,
        },
      },
    );
    const res = await reader({ maxItemsPerPage: 10 }, ctx);
    expect(res.items).toEqual(["item-1", "item-2"]);
  });

  it("rejects a page that exceeds maxItemsPerPage", async () => {
    const reader = createDistributedMapReader<number, number>(() => ({
      items: [1, 2, 3],
    }));
    await expect(reader({ maxItemsPerPage: 2 }, ctx)).rejects.toThrow(
      /exceeding maxItemsPerPage/,
    );
  });

  it("passes maxItemsPerPage to the reader function", async () => {
    const reader = createDistributedMapReader<number, number>(
      (_state, maxItemsPerPage) => ({
        items: Array.from({ length: maxItemsPerPage }, (_, i) => i),
      }),
    );
    const res = await reader({ maxItemsPerPage: 3 }, ctx);
    expect(res.items).toEqual(["0", "1", "2"]);
  });

  it("rejects a nextState over the 32 KB limit", async () => {
    const big = "x".repeat(33 * 1024);
    const reader = createDistributedMapReader<number, string>(() => ({
      items: [1],
      nextState: big,
    }));
    await expect(reader({ maxItemsPerPage: 10 }, ctx)).rejects.toThrow(/32 KB/);
  });

  it("rejects an event that is not a reader payload", async () => {
    const reader = createDistributedMapReader<number, number>(() => ({
      items: [],
    }));
    await expect(reader({} as any, ctx)).rejects.toThrow(/maxItemsPerPage/);
  });
});

describe("createDistributedMapItemHandler serialization failures", () => {
  it("fails only the item whose output cannot be serialized", async () => {
    const circular: Record<string, unknown> = {};
    circular.self = circular;
    const handler = createDistributedMapItemHandler<number, unknown>((item) =>
      item === 2 ? circular : item * 10,
    );
    const res = await handler(
      event([
        { itemId: "0", body: 1 },
        { itemId: "1", body: 2 },
        { itemId: "2", body: 3 },
      ]),
      ctx,
    );
    expect(res.batchItemResults).toEqual([
      { itemIdentifier: "0", output: "10" },
      { itemIdentifier: "2", output: "30" },
    ]);
    expect(res.batchItemFailures).toHaveLength(1);
    expect(res.batchItemFailures[0].itemIdentifier).toBe("1");
    expect(res.batchItemFailures[0].error.errorType).toBe("TypeError");
  });
});

describe("distributed map durable variants", () => {
  // A context whose map runs the mapper for each item and records a terminal
  // outcome, so the wrapper's result/failure assembly can be exercised directly.
  const durableCtx = {
    map: async (
      items: unknown[],
      mapper: (ctx: unknown, item: unknown, index: number) => unknown,
    ) => {
      const all: Array<{
        index: number;
        status: BatchItemStatus;
        result?: unknown;
        error?: unknown;
      }> = [];
      for (let i = 0; i < items.length; i++) {
        try {
          const result = await mapper(durableCtx, items[i], i);
          all.push({ index: i, status: BatchItemStatus.SUCCEEDED, result });
        } catch (error) {
          all.push({ index: i, status: BatchItemStatus.FAILED, error });
        }
      }
      return { all };
    },
  } as any;

  it("reports per-item results from the durable item handler", async () => {
    const handler = createDistributedMapItemHandlerWithDurableExecution<
      number,
      number
    >((_ctx, item) => item * 2);
    const res = (await handler(
      event([
        { itemId: "0", body: 2 },
        { itemId: "1", body: 3 },
      ]) as any,
      durableCtx,
    )) as any;
    expect(res.batchItemResults).toEqual([
      { itemIdentifier: "0", output: "4" },
      { itemIdentifier: "1", output: "6" },
    ]);
    expect(res.batchItemFailures).toEqual([]);
  });

  it("captures a failed item while succeeding the rest", async () => {
    const handler = createDistributedMapItemHandlerWithDurableExecution<
      number,
      number
    >((_ctx, item) => {
      if (item === 2) throw new Error("boom");
      return item * 2;
    });
    const res = (await handler(
      event([
        { itemId: "0", body: 1 },
        { itemId: "1", body: 2 },
      ]) as any,
      durableCtx,
    )) as any;
    expect(res.batchItemResults).toEqual([
      { itemIdentifier: "0", output: "2" },
    ]);
    expect(res.batchItemFailures).toEqual([
      {
        itemIdentifier: "1",
        error: { errorType: "Error", errorMessage: "boom" },
      },
    ]);
  });

  it("fails only the item whose output cannot be serialized in the durable item handler", async () => {
    const circular: Record<string, unknown> = {};
    circular.self = circular;
    const handler = createDistributedMapItemHandlerWithDurableExecution<
      number,
      unknown
    >((_ctx, item) => (item === 2 ? circular : item * 10));
    const res = (await handler(
      event([
        { itemId: "0", body: 1 },
        { itemId: "1", body: 2 },
        { itemId: "2", body: 3 },
      ]) as any,
      durableCtx,
    )) as any;
    expect(res.batchItemResults).toEqual([
      { itemIdentifier: "0", output: "10" },
      { itemIdentifier: "2", output: "30" },
    ]);
    expect(res.batchItemFailures).toHaveLength(1);
    expect(res.batchItemFailures[0].itemIdentifier).toBe("1");
  });

  it("reports only failures when the durable item handler uses report 'failures'", async () => {
    const handler = createDistributedMapItemHandlerWithDurableExecution<
      number,
      number
    >((_ctx, item) => item, { report: "failures" });
    const res = (await handler(
      event([{ itemId: "0", body: 1 }]) as any,
      durableCtx,
    )) as any;
    expect(res).toEqual({ batchItemFailures: [] });
  });

  it.each([
    [undefined, 1],
    [4, 4],
  ])(
    "passes concurrency %p to context.map as maxConcurrency %p",
    async (concurrency, expected) => {
      const map = jest.fn(async () => ({ all: [] }));
      const handler = createDistributedMapItemHandlerWithDurableExecution<
        number,
        number
      >((_ctx, item) => item, { concurrency });
      await handler(event([{ itemId: "0", body: 1 }]) as any, { map } as any);
      expect(map).toHaveBeenCalledWith(
        expect.anything(),
        expect.anything(),
        expect.objectContaining({ maxConcurrency: expected }),
      );
    },
  );

  it.each([0, -1, 1.5])(
    "rejects durable item handler concurrency %p",
    (concurrency) => {
      expect(() =>
        createDistributedMapItemHandlerWithDurableExecution(
          (_ctx, item) => item,
          { concurrency },
        ),
      ).toThrow("concurrency must be an integer of 1 or more");
    },
  );

  it("passes every decoded item to the durable batch handler", async () => {
    const seen: number[] = [];
    const handler =
      createDistributedMapBatchHandlerWithDurableExecution<number>(
        (_ctx, items) => {
          seen.push(...items);
        },
      );
    await handler(
      event([
        { itemId: "0", body: 5 },
        { itemId: "1", body: 6 },
      ]) as any,
      durableCtx,
    );
    expect(seen).toEqual([5, 6]);
  });
});
