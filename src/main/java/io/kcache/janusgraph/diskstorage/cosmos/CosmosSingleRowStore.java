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
package io.kcache.janusgraph.diskstorage.cosmos;

import static io.kcache.janusgraph.diskstorage.cosmos.builder.AbstractBuilder.*;

import com.azure.cosmos.models.CosmosItemRequestOptions;
import com.azure.cosmos.models.CosmosItemResponse;
import com.azure.cosmos.models.CosmosPatchOperations;
import com.azure.cosmos.models.CosmosQueryRequestOptions;
import com.azure.cosmos.models.PartitionKey;
import com.azure.cosmos.util.CosmosPagedFlux;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.kcache.janusgraph.diskstorage.cosmos.builder.AbstractBuilder;
import io.kcache.janusgraph.diskstorage.cosmos.builder.EntryBuilder;
import io.kcache.janusgraph.diskstorage.cosmos.iterator.FluxBasedKeyIterator;
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
import org.janusgraph.diskstorage.keycolumnvalue.KeyRangeQuery;
import org.janusgraph.diskstorage.keycolumnvalue.KeySliceQuery;
import org.janusgraph.diskstorage.keycolumnvalue.SliceQuery;
import org.janusgraph.diskstorage.keycolumnvalue.StoreTransaction;
import org.janusgraph.diskstorage.util.StaticArrayEntryList;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * Acts as if DynamoDB were a Column Oriented Database by using key as the hash key and each entry
 * has their own column. Note that if you are likely to go over the DynamoDB 400kb per item limit
 * you should use DynamoDbStore.
 * <p>
 * See configuration storage.dynamodb.stores.***store_name***.data-model=SINGLE
 * <p>
 * KCV Schema - actual table (Hash(S) only): hk   |  0x02  |  0x04    <-Attribute Names 0x01 |  0x03
 * |  0x05    <-Row Values
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
    log.debug("Entering getKeys table:{} query:{} txh:{}", getTableName(), encodeForLog(query),
        txh);

    String sql = "SELECT * FROM c";
    CosmosPagedFlux<ObjectNode> pagedFlux = getContainer().queryItems(sql, new CosmosQueryRequestOptions(), ObjectNode.class);
    // TODO make page size configurable?
    Flux<ObjectNode> flux = pagedFlux.byPage(100).flatMap(response -> Flux.fromIterable(response.getResults()));
    return new FluxBasedKeyIterator(flux);
  }

  @Override
  public EntryList getSlice(final KeySliceQuery query, final StoreTransaction txh)
      throws BackendException {
    log.debug("Entering getSliceKeySliceQuery table:{} query:{} txh:{}", getTableName(),
        encodeForLog(query), txh);
    String itemId = AbstractBuilder.encodeKey(query.getKey());
    CosmosItemResponse<ObjectNode> result = getContainer().readItem(itemId, new PartitionKey(itemId), new CosmosItemRequestOptions(), ObjectNode.class).block();

    final List<Entry> filteredEntries = extractEntriesFromGetItemResult(
        result != null ? result.getItem() : null,
        query.getSliceStart(), query.getSliceEnd(), query.getLimit());
    log.debug("Exiting getSliceKeySliceQuery table:{} query:{} txh:{} returning:{}", getTableName(),
        encodeForLog(query), txh,
        filteredEntries.size());
    return StaticArrayEntryList.of(filteredEntries);
  }

  @Override
  public Map<StaticBuffer, EntryList> getSlice(final List<StaticBuffer> keys,
      final SliceQuery query, final StoreTransaction txh) throws BackendException {
    log.debug("Entering getSliceMultiSliceQuery table:{} keys:{} query:{} txh:{}", getTableName(),
        encodeForLog(keys), encodeForLog(query),
        txh);
    final Map<StaticBuffer, EntryList> entries =
        //convert keys to get item workers and get the items
        client.getDelegate().parallelGetItem(
                keys.stream().map(this::createGetItemWorker).collect(Collectors.toList()))
            .entrySet()
            .stream()
            .collect(Collectors.toMap(Map.Entry::getKey,
                entry -> extractEntriesFromGetItemResult(entry.getValue(),
                    query.getSliceStart(), query.getSliceEnd(), query.getLimit())));

    log.debug("Exiting getSliceMultiSliceQuery table:{} keys:{} query:{} txh:{} returning:{}",
        getTableName(),
        encodeForLog(keys),
        encodeForLog(query),
        txh,
        entries.size());
    return entries;
  }

  @Override
  public void mutate(final StaticBuffer hashKey, final List<Entry> additions,
      final List<StaticBuffer> deletions, final StoreTransaction txh) throws BackendException {
    log.debug("Entering mutate table:{} keys:{} additions:{} deletions:{} txh:{}",
        getTableName(),
        encodeKeyForLog(hashKey),
        encodeForLog(additions),
        encodeForLog(deletions),
        txh);
    super.mutateOneKey(hashKey, new KCVMutation(additions, deletions), txh);

    log.debug("Exiting mutate table:{} keys:{} additions:{} deletions:{} txh:{} returning:void",
        getTableName(),
        encodeKeyForLog(hashKey),
        encodeForLog(additions),
        encodeForLog(deletions),
        txh);
  }


  @Override
  public List<Mono<CosmosItemResponse<ObjectNode>>> mutateMany(
      final Map<StaticBuffer, KCVMutation> mutations, final StoreTransaction txh)
      throws BackendException {
    List<Mono<CosmosItemResponse<ObjectNode>>> monos = new ArrayList<>();
    for (Map.Entry<StaticBuffer, KCVMutation> entry : mutations.entrySet()) {
      String key = encodeKey(entry.getKey());
      CosmosPatchOperations ops = convertToPatch(entry.getValue());
      Mono<CosmosItemResponse<ObjectNode>> mono =
          getContainer().patchItem(key, new PartitionKey(key), ops, ObjectNode.class);
      monos.add(mono);
    }
    return monos;
  }
}
