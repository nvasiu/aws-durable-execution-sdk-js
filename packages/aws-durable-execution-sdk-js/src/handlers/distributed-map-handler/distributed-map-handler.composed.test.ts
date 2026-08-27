/**
 * Runs the processor wrapper's response through to the orchestrator's result reader.
 *
 * The processor Lambda and the orchestrator are separate code that share only the wire format.
 * The wrapper and the reader each have unit tests, but only running them together shows that
 * the reader decodes what the wrapper writes.
 */

import { Context } from "aws-lambda";
import { createTestDurableContext } from "../../testing/create-test-durable-context";
import {
  createDistributedMapItemHandler,
  ItemHandlerResponse,
} from "../../distributed-map-helpers";
import {
  DistributedMapItemStatus,
  DistributedMapStatus,
  OperationSubType,
} from "../../types";
import {
  DistributedMapResultItemDetails,
  OperationStatus,
  OperationType,
} from "../../types/wire";
import { Serdes } from "../../utils/serdes/serdes";
import { hashId } from "../../utils/step-id-utils/step-id-utils";

interface Order {
  id: string;
  amount: number;
}

interface Receipt {
  id: string;
  charged: number;
}

// Prefixed JSON, so decoding with the wrong serdes fails visibly.
function prefixedSerdes<T>(prefix: string): Serdes<T> {
  return {
    serialize: async (value) =>
      value === undefined ? undefined : prefix + JSON.stringify(value),
    deserialize: async (data): Promise<T | undefined> => {
      if (data === undefined) return undefined;
      if (!data.startsWith(prefix))
        throw new Error(`expected ${prefix} payload, got ${data}`);
      return JSON.parse(data.slice(prefix.length)) as T;
    },
  };
}

// The conversion the service applies to a processor response before storing results.
function toStoredResults(
  itemIds: string[],
  response: ItemHandlerResponse,
): DistributedMapResultItemDetails[] {
  return itemIds.map((id) => {
    const failure = response.batchItemFailures.find(
      (f) => f.itemIdentifier === id,
    );
    if (failure) {
      return {
        ItemId: id,
        Status: "FAILED",
        Error: {
          ErrorType: failure.error.errorType,
          ErrorMessage: failure.error.errorMessage,
        },
      };
    }
    const result = response.batchItemResults?.find(
      (r) => r.itemIdentifier === id,
    );
    return {
      ItemId: id,
      Status: "SUCCEEDED",
      Output: result?.output ?? undefined,
    };
  });
}

describe("Distributed map processor-to-result composed tests", () => {
  it("decodes what the item handler wrapper reports", async () => {
    const itemSerdes = prefixedSerdes<Order>("item:");
    const resultSerdes = prefixedSerdes<Receipt>("result:");
    const orders: Order[] = [
      { id: "a", amount: 1 },
      { id: "b", amount: -1 },
      { id: "c", amount: 0 },
      { id: "d", amount: 3 },
    ];

    // Processor side: the wrapper as the service invokes it.
    const processor = createDistributedMapItemHandler<Order, Receipt>(
      (order) => {
        if (order.amount < 0)
          throw new RangeError(`negative amount for ${order.id}`);
        // A zero amount returns no receipt, which the result serdes serializes to no output.
        if (order.amount === 0) return undefined as unknown as Receipt;
        return { id: order.id, charged: order.amount * 2 };
      },
      { itemSerdes, resultSerdes },
    );
    const itemIds = orders.map((_, i) => `item-${i}`);
    const records = await Promise.all(
      orders.map(async (order, i) => ({
        itemId: itemIds[i],
        body: (await itemSerdes.serialize(order, {
          entityId: itemIds[i],
          durableExecutionArn: "",
        })) as string,
      })),
    );
    const response = await processor({ records }, {} as Context);

    // Orchestrator side: replay the completed run the service recorded from that response.
    const { context } = createTestDurableContext({
      existingOperations: [
        {
          Id: hashId("1"),
          Type: OperationType.DISTRIBUTED_MAP,
          SubType: OperationSubType.DISTRIBUTED_MAP,
          Name: "charge",
          StartTimestamp: new Date(),
          Status: OperationStatus.SUCCEEDED,
          DistributedMapDetails: {
            CompletionReason: "ALL_COMPLETED",
            SuccessCount: 3,
            FailureCount: 1,
            UnprocessedCount: 0,
            TotalCount: 4,
            Results: toStoredResults(itemIds, response),
          },
        },
      ],
    });

    const result = await context.distributedMap<Order, Receipt>(
      "charge",
      orders,
      { functionName: "processor", responseMode: "itemResults" },
      2,
      { collectResults: true, resultSerdes },
    );

    expect(result.status).toBe(DistributedMapStatus.SUCCEEDED);
    expect(result.all.map((i) => [i.itemId, i.status])).toEqual([
      ["item-0", DistributedMapItemStatus.SUCCEEDED],
      ["item-1", DistributedMapItemStatus.FAILED],
      ["item-2", DistributedMapItemStatus.SUCCEEDED],
      ["item-3", DistributedMapItemStatus.SUCCEEDED],
    ]);
    expect(result.getResults()).toEqual([
      { id: "a", charged: 2 },
      undefined,
      { id: "d", charged: 6 },
    ]);
    const [error] = result.getErrors();
    expect(error?.message).toBe("negative amount for b");
    expect(error?.cause?.name).toBe("RangeError");
  });
});
