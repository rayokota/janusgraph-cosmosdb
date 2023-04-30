/*
 * Copyright 2014-2015 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *  http://aws.amazon.com/apache2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */
package io.yokota.janusgraph.diskstorage.cosmos;

import com.azure.cosmos.models.CosmosItemRequestOptions;
import com.azure.cosmos.models.CosmosItemResponse;
import com.azure.cosmos.models.CosmosPatchItemRequestOptions;
import com.azure.cosmos.models.CosmosPatchOperations;
import com.azure.cosmos.models.CosmosQueryRequestOptions;
import com.azure.cosmos.models.PartitionKey;
import com.azure.cosmos.util.CosmosPagedIterable;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.yokota.janusgraph.diskstorage.cosmos.builder.EntryBuilder;
import io.yokota.janusgraph.diskstorage.cosmos.iterator.StreamBackedKeyIterator;
import io.yokota.janusgraph.diskstorage.cosmos.iterator.SingleRowStreamInterpreter;
import io.yokota.janusgraph.diskstorage.cosmos.builder.AbstractBuilder;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.extern.slf4j.Slf4j;
import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.Entry;
import org.janusgraph.diskstorage.EntryList;
import org.janusgraph.diskstorage.StaticBuffer;
import org.janusgraph.diskstorage.keycolumnvalue.KCVMutation;
import org.janusgraph.diskstorage.keycolumnvalue.KeyIterator;
import org.janusgraph.diskstorage.keycolumnvalue.KeyRangeQuery;
import org.janusgraph.diskstorage.keycolumnvalue.KeySliceQuery;
import org.janusgraph.diskstorage.keycolumnvalue.SliceQuery;
import org.janusgraph.diskstorage.keycolumnvalue.StoreTransaction;
import org.janusgraph.diskstorage.util.StaticArrayEntryList;
import reactor.util.function.Tuple2;
import reactor.util.function.Tuples;

/**
 * Acts as if DynamoDB were a Column Oriented Database by using key as the hash key and each entry
 * has their own column. Note that if you are likely to go over the DynamoDB 400kb per item limit
 * you should use DynamoDbStore.
 * <p>
 * See configuration
 * storage.dynamodb.stores.***store_name***.data-model=SINGLE
 * <p>
 * KCV Schema - actual table (Hash(S) only):
 * hk   |  0x02  |  0x04    <-Attribute Names
 * 0x01 |  0x03  |  0x05    <-Row Values
 *
 * @author Matthew Sowders
 * @author Alexander Patrikalakis
 */
@Slf4j
public class CosmosSingleRowStore extends AbstractCosmosStore {

  CosmosSingleRowStore(final CosmosStoreManager manager, final String prefix,
      final String storeName) {
    super(manager, prefix, storeName);
  }

  @Override
  public KeyIterator getKeys(final KeyRangeQuery query, final StoreTransaction txh)
      throws BackendException {
    throw new UnsupportedOperationException("Keys are not byte ordered.");
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
    log.debug("==> getKeys table:{} query:{} txh:{}", getContainerName(), encodeForLog(query), txh);

    String sql = "SELECT * FROM c";
    CosmosPagedIterable<ObjectNode> iterable = getContainer().queryItems(sql, new CosmosQueryRequestOptions(), ObjectNode.class);
    // TODO make page size configurable?
    log.debug("<== getKeys table:{} query:{} txh:{}", getContainerName(), encodeForLog(query), txh);
    return new StreamBackedKeyIterator<>(iterable.stream(), new SingleRowStreamInterpreter(query));
  }

  @Override
  public EntryList getSlice(final KeySliceQuery query, final StoreTransaction txh)
      throws BackendException {
    log.debug("==> getSliceKeySliceQuery table:{} query:{} txh:{}", getContainerName(),
        encodeForLog(query), txh);
    String itemId = AbstractBuilder.encodeKey(query.getKey());
    CosmosItemResponse<ObjectNode> response = getContainer()
        .readItem(itemId, new PartitionKey(itemId), new CosmosItemRequestOptions(), ObjectNode.class);

    EntryList filteredEntries = extractEntriesFromGetItemResult(
        response != null ? response.getItem() : null,
        query.getSliceStart(), query.getSliceEnd(), query.getLimit());
    log.debug("<== getSliceKeySliceQuery table:{} query:{} txh:{} returning:{}", getContainerName(),
        encodeForLog(query), txh,
        filteredEntries.size());
    return filteredEntries;
  }

  @Override
  public Map<StaticBuffer, EntryList> getSlice(final List<StaticBuffer> keys,
      final SliceQuery query, final StoreTransaction txh) throws BackendException {
    log.debug("==> getSliceMultiSliceQuery table:{} keys:{} query:{} txh:{}", getContainerName(),
        encodeForLog(keys), encodeForLog(query),
        txh);
    return keys.stream()
        .parallel()
        .map(key -> {
          String itemId = AbstractBuilder.encodeKey(key);
          CosmosItemResponse<ObjectNode> response = getContainer()
              .readItem(itemId, new PartitionKey(itemId), new CosmosItemRequestOptions(),
                  ObjectNode.class);
          EntryList entryList = extractEntriesFromGetItemResult(
            response.getItem(),
            query.getSliceStart(), query.getSliceEnd(), query.getLimit());
          return Tuples.of(key, entryList);
            }
        )
        .collect(Collectors.toMap(Tuple2::getT1, Tuple2::getT2));
  }

  @Override
  public void mutate(final StaticBuffer hashKey, final List<Entry> additions,
      final List<StaticBuffer> deletions, final StoreTransaction txh) throws BackendException {
    log.debug("==> mutate table:{} keys:{} additions:{} deletions:{} txh:{}",
        getContainerName(),
        encodeKeyForLog(hashKey),
        encodeForLog(additions),
        encodeForLog(deletions),
        txh);
    super.mutateOneKey(hashKey, new KCVMutation(additions, deletions), txh);

    log.debug("<== mutate table:{} keys:{} additions:{} deletions:{} txh:{} returning:void",
        getContainerName(),
        encodeKeyForLog(hashKey),
        encodeForLog(additions),
        encodeForLog(deletions),
        txh);
  }

  @Override
  public void mutateMany(
      final Map<StaticBuffer, KCVMutation> mutations, final StoreTransaction txh)
      throws BackendException {
    mutations.forEach((k, v) -> {
          String key = AbstractBuilder.encodeKey(k);
          CosmosPatchOperations ops = convertToPatch(v);
          CosmosItemResponse<ObjectNode> response =
              getContainer().patchItem(key, new PartitionKey(key), ops, new CosmosPatchItemRequestOptions(), ObjectNode.class);
        });
  }

  protected CosmosPatchOperations convertToPatch(KCVMutation mutation) {
    CosmosPatchOperations patch = CosmosPatchOperations.create();

    if (mutation.hasDeletions()) {
      for (StaticBuffer b : mutation.getDeletions()) {
        patch.remove(AbstractBuilder.encodeKey(b));
      }
    }

    if (mutation.hasAdditions()) {
      for (Entry e : mutation.getAdditions()) {
        patch.add(AbstractBuilder.encodeKey(e.getColumn()), AbstractBuilder.encodeValue(e.getValue()));
      }
    }
    return patch;
  }
}
