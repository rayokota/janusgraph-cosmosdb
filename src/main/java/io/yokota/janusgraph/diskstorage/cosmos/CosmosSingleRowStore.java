/*
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */
package io.yokota.janusgraph.diskstorage.cosmos;

import static io.yokota.janusgraph.diskstorage.cosmos.Constants.BATCH_SIZE_LIMIT;
import static io.yokota.janusgraph.diskstorage.cosmos.Constants.PATCH_SIZE_LIMIT;
import static io.yokota.janusgraph.diskstorage.cosmos.builder.AbstractBuilder.encodeKey;
import static io.yokota.janusgraph.diskstorage.cosmos.builder.AbstractBuilder.encodeValue;

import com.azure.cosmos.models.CosmosBatch;
import com.azure.cosmos.models.CosmosBatchItemRequestOptions;
import com.azure.cosmos.models.CosmosBatchPatchItemRequestOptions;
import com.azure.cosmos.models.CosmosBatchRequestOptions;
import com.azure.cosmos.models.CosmosBatchResponse;
import com.azure.cosmos.models.CosmosItemOperation;
import com.azure.cosmos.models.CosmosItemRequestOptions;
import com.azure.cosmos.models.CosmosItemResponse;
import com.azure.cosmos.models.CosmosPatchOperations;
import com.azure.cosmos.models.CosmosQueryRequestOptions;
import com.azure.cosmos.models.PartitionKey;
import com.azure.cosmos.util.CosmosPagedIterable;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.yokota.janusgraph.diskstorage.cosmos.builder.EntryBuilder;
import io.yokota.janusgraph.diskstorage.cosmos.builder.ItemBuilder;
import io.yokota.janusgraph.diskstorage.cosmos.iterator.SingleRowStreamInterpreter;
import io.yokota.janusgraph.diskstorage.cosmos.iterator.StreamBackedKeyIterator;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.Entry;
import org.janusgraph.diskstorage.EntryList;
import org.janusgraph.diskstorage.StaticBuffer;
import org.janusgraph.diskstorage.keycolumnvalue.KCVMutation;
import org.janusgraph.diskstorage.keycolumnvalue.KeyIterator;
import org.janusgraph.diskstorage.keycolumnvalue.KeySliceQuery;
import org.janusgraph.diskstorage.keycolumnvalue.SliceQuery;
import org.janusgraph.diskstorage.keycolumnvalue.StoreTransaction;
import org.janusgraph.diskstorage.util.StaticArrayEntryList;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.util.function.Tuple2;
import reactor.util.function.Tuples;

/**
 * Acts as if Cosmos DB were a Column Oriented Database by using key as the hash key and each entry
 * has their own column. Note that if you are likely to go over the Cosmos DB 400kb per item limit
 * you should use CosmosStore.
 * <p>
 * See configuration storage.cosmos.stores.***store_name***.data-model=SINGLE
 * <p>
 * KCV Schema - actual table (Hash(S) only):
 * hk   |  0x02  |  0x04    <-Attribute Names
 * 0x01 |  0x03  |  0x05    <-Row Values
 */
@Slf4j
public class CosmosSingleRowStore extends AbstractCosmosStore {

  CosmosSingleRowStore(final CosmosStoreManager manager, final String prefix,
      final String storeName) {
    super(manager, prefix, storeName);
  }

  private EntryList extractEntriesFromGetItemResult(final ObjectNode item,
      final StaticBuffer sliceStart, final StaticBuffer sliceEnd, final int limit) {
    List<Entry> filteredEntries = Collections.emptyList();
    if (null != item) {
      item.remove(Constants.JANUSGRAPH_PARTITION_KEY);
      filteredEntries = new EntryBuilder(item)
          .slice(sliceStart, sliceEnd)
          .limit(limit)
          .buildAll();
    }
    return StaticArrayEntryList.of(filteredEntries);
  }

  @Override
  public KeyIterator getKeys(final SliceQuery query, final StoreTransaction txh)
      throws BackendException {
    try {
      log.debug("==> getKeys table:{} query:{} txh:{}", getContainerName(), encodeForLog(query),
          txh);

      String sql = "SELECT * FROM c ORDER BY c.id";
      CosmosPagedIterable<ObjectNode> iterable = new CosmosPagedIterable<>(
          getContainer().queryItems(sql,
              new CosmosQueryRequestOptions(), ObjectNode.class));
      // TODO make page size configurable?
      return new StreamBackedKeyIterator<>(iterable.stream(),
          new SingleRowStreamInterpreter(query));
    } finally {
      log.debug("<== getKeys table:{} query:{} txh:{}", getContainerName(), encodeForLog(query),
          txh);
    }
  }

  @Override
  public EntryList getSlice(final KeySliceQuery query, final StoreTransaction txh)
      throws BackendException {
    EntryList filteredEntries = null;
    try {
      log.debug("==> getSliceKeySliceQuery table:{} query:{} txh:{}", getContainerName(),
          encodeForLog(query), txh);
      String itemId = encodeKey(query.getKey());
      CosmosItemResponse<ObjectNode> response = getContainer()
          .readItem(itemId, new PartitionKey(itemId), new CosmosItemRequestOptions(),
              ObjectNode.class)
          .onErrorResume(exception -> Mono.empty())
          .block();

      filteredEntries = extractEntriesFromGetItemResult(
          response != null ? response.getItem() : null,
          query.getSliceStart(), query.getSliceEnd(), query.getLimit());
      return filteredEntries;
    } finally {
      log.debug("<== getSliceKeySliceQuery table:{} query:{} txh:{} returning:{}",
          getContainerName(),
          encodeForLog(query), txh,
          filteredEntries != null ? filteredEntries.size() : 0);

    }
  }

  @Override
  public Map<StaticBuffer, EntryList> getSlice(final List<StaticBuffer> keys,
      final SliceQuery query, final StoreTransaction txh) throws BackendException {
    try {
      log.debug("==> getSliceMultiSliceQuery table:{} keys:{} query:{} txh:{}", getContainerName(),
          encodeForLog(keys), encodeForLog(query),
          txh);
      return keys.stream()
          .parallel()
          .map(key -> {
                String itemId = encodeKey(key);
                CosmosItemResponse<ObjectNode> response = getContainer()
                    .readItem(itemId, new PartitionKey(itemId), new CosmosItemRequestOptions(),
                        ObjectNode.class)
                    .onErrorResume(exception -> Mono.empty())
                    .block();
                EntryList entryList = extractEntriesFromGetItemResult(
                    response != null ? response.getItem() : null,
                    query.getSliceStart(), query.getSliceEnd(), query.getLimit());
                return Tuples.of(key, entryList);
              }
          )
          .collect(Collectors.toMap(Tuple2::getT1, Tuple2::getT2));
    } finally {
      log.debug("<== getSliceMultiSliceQuery table:{} keys:{} query:{} txh:{}", getContainerName(),
          encodeForLog(keys), encodeForLog(query),
          txh);
    }
  }

  @Override
  public void mutate(final StaticBuffer hashKey, final List<Entry> additions,
      final List<StaticBuffer> deletions, final StoreTransaction txh) throws BackendException {
    try {
      log.debug("==> mutate table:{} keys:{} additions:{} deletions:{} txh:{}",
          getContainerName(),
          encodeKeyForLog(hashKey),
          encodeForLog(additions),
          encodeForLog(deletions),
          txh);
      super.mutateOneKey(hashKey, new KCVMutation(additions, deletions), txh);
    } finally {
      log.debug("<== mutate table:{} keys:{} additions:{} deletions:{} txh:{} returning:void",
          getContainerName(),
          encodeKeyForLog(hashKey),
          encodeForLog(additions),
          encodeForLog(deletions),
          txh);
    }
  }

  @Override
  public void mutateMany(
      final Map<StaticBuffer, KCVMutation> mutations, final StoreTransaction txh)
      throws BackendException {
    Flux.fromIterable(mutations.entrySet())
        .parallel()
        .runOn(Schedulers.boundedElastic())
        .flatMap(entry -> executeCreateAndBatch(entry.getKey(), entry.getValue()))
        .map(response -> {
          // Examining if the batch of operations is successful
          if (response.isSuccessStatusCode()) {
            log.debug("The batch of operations succeeded.");
          } else {
            // Iterating over the operation results to find out the error code
            response.getResults().forEach(result -> {
              // Failed operation will have a status code of the corresponding error.
              // All other operations will have a 424 (Failed Dependency) status code.
              if (result.getStatusCode() != HttpResponseStatus.FAILED_DEPENDENCY.code()) {
                CosmosItemOperation itemOperation = result.getOperation();
                log.warn("Operation for Item with ID [{}] and Partition Key Value [{}]" +
                        " failed with a status code [{}], resulting in batch failure.",
                    itemOperation.getId(),
                    itemOperation.getPartitionKeyValue(),
                    result.getStatusCode());
              }
            });
          }
          return response;
        })
        .sequential()
        .blockLast();
  }

  protected Flux<CosmosBatchResponse> executeCreateAndBatch(StaticBuffer key, KCVMutation mutation) {
    ObjectNode item = new ItemBuilder()
        .partitionKey(key)
        .columnKey(key)
        .build();
    PartitionKey partitionKey = new PartitionKey(encodeKey(key));

    // Ensure the items already exist, as patch operations will not create the item
    // (a patch operation is not a "patch-sert" in the same manner as upsert).
    // If the item already exists, the create operation will fail, which we ignore.
    return getContainer().createItem(item, partitionKey, new CosmosItemRequestOptions())
        .onErrorResume(exception -> Mono.empty())
        .thenMany(executeBatch(key, mutation));
  }

  protected Flux<CosmosBatchResponse> executeBatch(StaticBuffer key, KCVMutation mutation) {
    List<CosmosBatch> batches = convertToBatches(encodeKey(key), mutation);
    return Flux.fromIterable(batches)
        .flatMap(batch ->
            getContainer().executeCosmosBatch(batch, new CosmosBatchRequestOptions()));
  }

  protected List<CosmosBatch> convertToBatches(String key, KCVMutation mutation) {
    PartitionKey partitionKey = new PartitionKey(key);
    List<CosmosBatch> result = new ArrayList<>();
    CosmosBatch batch = CosmosBatch.createCosmosBatch(partitionKey);
    CosmosPatchOperations patch = CosmosPatchOperations.create();
    int batchSize = 0;
    int patchSize = 0;

    if (mutation.hasDeletions()) {
      for (StaticBuffer b : mutation.getDeletions()) {
        patch.remove("/" + encodeKey(b));
        if (++patchSize == PATCH_SIZE_LIMIT) {
          batch.patchItemOperation(key, patch, new CosmosBatchPatchItemRequestOptions());
          patch = CosmosPatchOperations.create();
          patchSize = 0;
          if (++batchSize == BATCH_SIZE_LIMIT) {
            result.add(batch);
            batch = CosmosBatch.createCosmosBatch(partitionKey);
            batchSize = 0;
          }
        }
      }
    }

    if (mutation.hasAdditions()) {
      for (Entry e : mutation.getAdditions()) {
        patch.add("/" + encodeKey(e.getColumn()), encodeValue(e.getValue()));
        if (++patchSize == PATCH_SIZE_LIMIT) {
          batch.patchItemOperation(key, patch, new CosmosBatchPatchItemRequestOptions());
          patch = CosmosPatchOperations.create();
          patchSize = 0;
          if (++batchSize == BATCH_SIZE_LIMIT) {
            result.add(batch);
            batch = CosmosBatch.createCosmosBatch(partitionKey);
            batchSize = 0;
          }
        }
      }
    }

    if (patchSize > 0) {
      batch.patchItemOperation(key, patch, new CosmosBatchPatchItemRequestOptions());
      ++batchSize;
    }
    if (batchSize > 0) {
      result.add(batch);
    }
    return result;
  }
}
