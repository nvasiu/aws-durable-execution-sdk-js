import { Context } from "aws-lambda";
import {
  Serdes,
  AnySerdes,
  SerdesContext,
  defaultSerdes,
} from "./utils/serdes/serdes";
import { DurableContext, DurableLambdaHandler, BatchItemStatus } from "./types";
import { withDurableExecution } from "./with-durable-execution";

const READER_STATE_LIMIT = 32 * 1024;

/** A page returned by a reader function, its items and the next state. @public */
export interface ReaderPage<TItem, TState> {
  items: TItem[];
  nextState?: TState;
}

interface ProcessorRecord {
  itemId: string;
  /** The item as a serialized string, deserialized by the configured item serdes. */
  body: string;
}

interface ProcessorEvent {
  distributedMapRunArn?: string;
  records: ProcessorRecord[];
}

interface ReaderEvent {
  distributedMapRunArn?: string;
  state?: string | null;
  maxItemsPerPage: number;
}

/** Error details for one failed item in an item handler response. @public */
export interface ErrorDetail {
  errorType: string;
  errorMessage: string;
}

/** Output of one succeeded item in an item handler response. @public */
export interface ItemResult {
  itemIdentifier: string;
  /** The output as a serialized string, or null when the result serdes produced none. */
  output: string | null;
}

/** One failed item in an item handler response. @public */
export interface ItemFailure {
  itemIdentifier: string;
  error: ErrorDetail;
}

/** Response an item handler returns to the distributed map run. @public */
export interface ItemHandlerResponse {
  /** Present only when the handler reports results. */
  batchItemResults?: ItemResult[];
  batchItemFailures: ItemFailure[];
}

/** Response a reader returns to the distributed map run. @public */
export interface ReaderResponse {
  /** Each item as a serialized string, deserialized by the processor's item serdes. */
  items: string[];
  /** The next state as a serialized string, absent when the source is exhausted. */
  nextState?: string;
}

type ItemReport = "results" | "failures";

const VALID_REPORTS: readonly string[] = ["results", "failures"];

/** The wrapper cannot see the processor config, so this must match the mode the run started with. */
function assertReport(report: ItemReport | undefined): void {
  if (report !== undefined && !VALID_REPORTS.includes(report)) {
    throw new Error(
      'report must be "results" or "failures", matching the processor reference mode',
    );
  }
}

/** Reject a concurrency the pool would otherwise silently coerce (0 to unbounded, negatives to 1). */
function assertConcurrency(concurrency: number | undefined): void {
  if (
    concurrency !== undefined &&
    (!Number.isInteger(concurrency) || concurrency < 1)
  ) {
    throw new Error("concurrency must be an integer of 1 or more");
  }
}

function ctxFor(
  entityId: string,
  event: { distributedMapRunArn?: string },
): SerdesContext {
  return { entityId, durableExecutionArn: event.distributedMapRunArn ?? "" };
}

/** Reject an event that is not a distributed map processor payload. */
function assertProcessorEvent(event: unknown): asserts event is ProcessorEvent {
  const e = event as ProcessorEvent | undefined;
  if (!e || !Array.isArray(e.records)) {
    throw new Error(
      "expected a distributed map processor event with a 'records' array (this handler must be invoked by a distributed map run, not directly)",
    );
  }
}

/** Reject an event that is not a distributed map reader payload. */
function assertReaderEvent(event: unknown): asserts event is ReaderEvent {
  const e = event as ReaderEvent | undefined;
  if (!e || typeof e.maxItemsPerPage !== "number") {
    throw new Error(
      "expected a distributed map reader event with a numeric 'maxItemsPerPage' (this handler must be invoked by a distributed map run, not directly)",
    );
  }
}

async function toItem<T>(
  serdes: Serdes<T>,
  body: string,
  ctx: SerdesContext,
): Promise<T> {
  return (await serdes.deserialize(body, ctx)) as T;
}

async function toOutput<T>(
  serdes: Serdes<T>,
  value: T,
  ctx: SerdesContext,
): Promise<string | null> {
  return (await serdes.serialize(value, ctx)) ?? null;
}

function errorEntry(itemId: string, err: unknown): ItemFailure {
  const e = err as { name?: string; message?: string };
  return {
    itemIdentifier: itemId,
    error: {
      errorType: e?.name ?? "Error",
      errorMessage: e?.message ?? String(err),
    },
  };
}

async function runPool<T, R>(
  items: T[],
  limit: number,
  worker: (item: T, index: number) => Promise<R>,
): Promise<Array<{ ok: true; value: R } | { ok: false; error: unknown }>> {
  const results: Array<{ ok: true; value: R } | { ok: false; error: unknown }> =
    new Array(items.length);
  let next = 0;
  const size = Math.max(1, Math.min(limit, items.length || 1));
  const runners = Array.from({ length: size }, async () => {
    for (;;) {
      const i = next++;
      if (i >= items.length) break;
      try {
        results[i] = { ok: true, value: await worker(items[i], i) };
      } catch (error) {
        results[i] = { ok: false, error };
      }
    }
  });
  await Promise.all(runners);
  return results;
}

/**
 * Wrap a Lambda as an itemResults processor (pass report "failures" for itemFailures).
 *
 * Items are processed one at a time by default. Raising `concurrency` overlaps the handler with
 * itself, so `fn` must tolerate several calls being in flight at once.
 *
 * @public
 */
export function createDistributedMapItemHandler<TItem, TOutput>(
  fn: (item: TItem) => TOutput | Promise<TOutput>,
  opts?: {
    itemSerdes?: Serdes<TItem>;
    resultSerdes?: Serdes<TOutput>;
    /** Handlers in flight at once. Defaults to 1, and must be an integer of 1 or more. */
    concurrency?: number;
    report?: "results" | "failures";
  },
): (event: unknown, context: Context) => Promise<ItemHandlerResponse> {
  assertReport(opts?.report);
  assertConcurrency(opts?.concurrency);
  const itemSerdes = (opts?.itemSerdes ?? defaultSerdes) as AnySerdes;
  const resultSerdes = (opts?.resultSerdes ?? defaultSerdes) as AnySerdes;
  const report: ItemReport = opts?.report ?? "results";
  const concurrency = opts?.concurrency ?? 1;

  return async (event: unknown): Promise<ItemHandlerResponse> => {
    assertProcessorEvent(event);
    const records = event.records;
    // Serialize inside the worker, so a serialization failure fails only that item.
    const outcomes = await runPool(records, concurrency, async (record) => {
      const ctx = ctxFor(record.itemId, event);
      const value = await fn(
        (await toItem(itemSerdes, record.body, ctx)) as TItem,
      );
      return report === "results"
        ? await toOutput(resultSerdes, value, ctx)
        : null;
    });

    const results: ItemResult[] = [];
    const failures: ItemFailure[] = [];
    for (let i = 0; i < records.length; i++) {
      const record = records[i];
      const outcome = outcomes[i];
      if (!outcome.ok) {
        failures.push(errorEntry(record.itemId, outcome.error));
      } else if (report === "results") {
        results.push({ itemIdentifier: record.itemId, output: outcome.value });
      }
    }

    if (report === "failures") return { batchItemFailures: failures };
    return { batchItemResults: results, batchItemFailures: failures };
  };
}

/** Wrap a Lambda as a batch processor, where returning passes the batch and throwing fails it. @public */
export function createDistributedMapBatchHandler<TItem>(
  fn: (items: TItem[]) => void | Promise<void>,
  opts?: { itemSerdes?: Serdes<TItem> },
): (event: unknown, context: Context) => Promise<void> {
  const serdes = (opts?.itemSerdes ?? defaultSerdes) as AnySerdes;
  return async (event: unknown): Promise<void> => {
    assertProcessorEvent(event);
    const items = await Promise.all(
      event.records.map(
        (r) =>
          toItem(serdes, r.body, ctxFor(r.itemId, event)) as Promise<TItem>,
      ),
    );
    await fn(items);
  };
}

/** Wrap a Lambda as a reader source, a missing nextState means the source is exhausted. @public */
export function createDistributedMapReader<TItem, TState>(
  fn: (
    state: TState | undefined,
    maxItemsPerPage: number,
  ) => ReaderPage<TItem, TState> | Promise<ReaderPage<TItem, TState>>,
  opts?: {
    /** Must match the item serdes the processor uses to read each record body. */
    itemSerdes?: Serdes<TItem>;
    stateSerdes?: Serdes<TState>;
  },
): (event: unknown, context: Context) => Promise<ReaderResponse> {
  const itemSerdes = (opts?.itemSerdes ?? defaultSerdes) as AnySerdes;
  const serdes = (opts?.stateSerdes ?? defaultSerdes) as AnySerdes;
  return async (event: unknown): Promise<ReaderResponse> => {
    assertReaderEvent(event);
    const ctx = ctxFor("reader", event);
    const state =
      event.state !== undefined && event.state !== null
        ? ((await serdes.deserialize(event.state, ctx)) as TState)
        : undefined;

    const page = await fn(state, event.maxItemsPerPage);
    if (page.items.length > event.maxItemsPerPage) {
      throw new Error(
        `reader returned ${page.items.length} items, exceeding maxItemsPerPage ${event.maxItemsPerPage}`,
      );
    }

    const items: string[] = [];
    for (const item of page.items) {
      items.push((await itemSerdes.serialize(item, ctx)) as string);
    }
    const response: ReaderResponse = { items };
    if (page.nextState !== undefined && page.nextState !== null) {
      const nextState = (await serdes.serialize(page.nextState, ctx)) as string;
      if (Buffer.byteLength(nextState, "utf8") > READER_STATE_LIMIT) {
        throw new Error("reader nextState exceeds the 32 KB limit");
      }
      response.nextState = nextState;
    }
    return response;
  };
}

/** Durable variant of the item handler, fn receives (context, item). @public */
export function createDistributedMapItemHandlerWithDurableExecution<
  TItem,
  TOutput,
>(
  fn: (context: DurableContext, item: TItem) => TOutput | Promise<TOutput>,
  opts?: {
    itemSerdes?: Serdes<TItem>;
    resultSerdes?: Serdes<TOutput>;
    report?: "results" | "failures";
    /** Children in flight at once. Defaults to 1, and must be an integer of 1 or more. */
    concurrency?: number;
  },
): DurableLambdaHandler {
  assertReport(opts?.report);
  assertConcurrency(opts?.concurrency);
  const itemSerdes = (opts?.itemSerdes ?? defaultSerdes) as AnySerdes;
  const resultSerdes = (opts?.resultSerdes ?? defaultSerdes) as AnySerdes;
  const report: ItemReport = opts?.report ?? "results";
  const concurrency = opts?.concurrency ?? 1;

  return withDurableExecution(
    async (
      event: ProcessorEvent,
      context: DurableContext,
    ): Promise<ItemHandlerResponse> => {
      assertProcessorEvent(event);
      const records = event.records;
      // Run every item to completion, so one failing item never cancels another that would succeed.
      // Serialize inside the child, so a serialization failure fails only that item.
      const batch = await context.map(
        records.map((r) => r.body),
        async (ctx, body, index) => {
          const serdesCtx = ctxFor(records[index].itemId, event);
          const value = await fn(
            ctx,
            (await toItem(itemSerdes, body, serdesCtx)) as TItem,
          );
          return report === "results"
            ? await toOutput(resultSerdes, value, serdesCtx)
            : null;
        },
        {
          maxConcurrency: concurrency,
          completionConfig: { toleratedFailurePercentage: 100 },
        },
      );

      const results: ItemResult[] = [];
      const failures: ItemFailure[] = [];
      for (const item of batch.all) {
        const itemId = records[item.index].itemId;
        // Only SUCCEEDED yields an output. FAILED or non-terminal is a failure.
        if (item.status === BatchItemStatus.SUCCEEDED) {
          if (report === "results") {
            results.push({
              itemIdentifier: itemId,
              output: item.result ?? null,
            });
          }
        } else {
          failures.push(
            errorEntry(
              itemId,
              item.error ?? new Error(`item ${itemId} did not complete`),
            ),
          );
        }
      }

      if (report === "failures") return { batchItemFailures: failures };
      return { batchItemResults: results, batchItemFailures: failures };
    },
  );
}

/** Durable variant of the batch handler, fn receives (context, items). @public */
export function createDistributedMapBatchHandlerWithDurableExecution<TItem>(
  fn: (context: DurableContext, items: TItem[]) => void | Promise<void>,
  opts?: { itemSerdes?: Serdes<TItem> },
): DurableLambdaHandler {
  const serdes = (opts?.itemSerdes ?? defaultSerdes) as AnySerdes;
  return withDurableExecution(
    async (event: ProcessorEvent, context: DurableContext): Promise<void> => {
      assertProcessorEvent(event);
      const items = await Promise.all(
        event.records.map(
          (r) =>
            toItem(serdes, r.body, ctxFor(r.itemId, event)) as Promise<TItem>,
        ),
      );
      await fn(context, items);
    },
  );
}
