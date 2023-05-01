/*
 * Copyright 2014-2018 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

import static io.yokota.janusgraph.diskstorage.cosmos.builder.AbstractBuilder.encodeKey;

import com.azure.cosmos.models.CosmosBulkExecutionOptions;
import com.azure.cosmos.models.CosmosBulkOperations;
import com.azure.cosmos.models.CosmosItemOperation;
import com.azure.cosmos.models.CosmosItemRequestOptions;
import com.azure.cosmos.models.CosmosItemResponse;
import com.azure.cosmos.models.CosmosQueryRequestOptions;
import com.azure.cosmos.models.PartitionKey;
import com.azure.cosmos.util.CosmosPagedIterable;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.yokota.janusgraph.diskstorage.cosmos.builder.EntryBuilder;
import io.yokota.janusgraph.diskstorage.cosmos.builder.ItemBuilder;
import io.yokota.janusgraph.diskstorage.cosmos.iterator.MultiRowStreamInterpreter;
import io.yokota.janusgraph.diskstorage.cosmos.iterator.StreamBackedKeyIterator;
import io.yokota.janusgraph.diskstorage.cosmos.mutation.BulkWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.extern.slf4j.Slf4j;
import one.util.streamex.StreamEx;
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
 * Acts as if DynamoDB were a Column Oriented Database by using range query when required.
 * <p>
 * See configuration storage.dynamodb.stores.***table_name***.data-model=MULTI
 * <p>
 * KCV Schema - actual table (Hash(S) + Range(S)): hk(S)  |  rk(S)  |  v(B)  <-Attribute Names 0x01
 * |  0x02   |  0x03  <-Row Values 0x01   |  0x04   |  0x05  <-Row Values
 *
 * @author Matthew Sowders
 * @author Alexander Patrikalakis
 * @author Michael Rodaitis
 */
@Slf4j
public class CosmosStore extends AbstractCosmosStore {

  public CosmosStore(final CosmosStoreManager manager, final String prefix,
      final String storeName) {
    super(manager, prefix, storeName);
  }

  @Override
  public KeyIterator getKeys(final SliceQuery query, final StoreTransaction txh)
      throws BackendException {
    try {
      log.debug("==> getKeys table:{} query:{} txh:{}", getContainerName(),
          encodeForLog(query), txh);

      String sql = "SELECT * FROM c where c.id >= '" + encodeKey(query.getSliceStart())
          + "' and c.id < '" + encodeKey(query.getSliceEnd())
          + "'";
      CosmosPagedIterable<ObjectNode> iterable = new CosmosPagedIterable<>(getContainer().queryItems(sql,
          new CosmosQueryRequestOptions(), ObjectNode.class));
      // TODO make page size configurable?
      Stream<List<ObjectNode>> grouped = StreamEx.of(iterable.stream())
          .groupRuns((item1, item2) -> item1.get(Constants.JANUSGRAPH_PARTITION_KEY).textValue()
              .equals(item2.get(Constants.JANUSGRAPH_PARTITION_KEY).textValue()));

      return new StreamBackedKeyIterator<>(grouped, new MultiRowStreamInterpreter(this, query));
    } finally {
      log.debug("<== getKeys table:{} query:{} txh:{}", getContainerName(),
          encodeForLog(query), txh);
    }
  }

  @Override
  public EntryList getSlice(final KeySliceQuery query, final StoreTransaction txh)
      throws BackendException {
    try {
      log.debug("==> getSliceKeySliceQuery table:{} query:{} txh:{}", getContainerName(),
          encodeForLog(query), txh);

      SliceQuery sliceQuery = new SliceQuery(query.getSliceStart(), query.getSliceEnd());
      if (query.hasLimit()) {
        sliceQuery.setLimit(query.getLimit());
      }
      Stream<Entry> entries = query(query.getKey(), sliceQuery, txh);
      return StaticArrayEntryList.of(entries.collect(Collectors.toList()));
    } finally {
      log.debug("<== getSliceKeySliceQuery table:{} query:{} txh:{}", getContainerName(),
          encodeForLog(query), txh);
    }
  }

  @Override
  public Map<StaticBuffer, EntryList> getSlice(final List<StaticBuffer> keys,
      final SliceQuery query, final StoreTransaction txh) throws BackendException {
    try {
      log.debug("==> getSliceMultiSliceQuery table:{} keys:{} query:{} txh:{}",
          getContainerName(),
          encodeForLog(keys),
          encodeForLog(query),
          txh);

      return keys.stream()
          .parallel()
          .map(key -> Tuples.of(key, query(key, query, txh)))
          .collect(Collectors.toMap(
              Tuple2::getT1,
              tuple -> StaticArrayEntryList.of(tuple.getT2().collect(Collectors.toList())))
          );
    } finally {
      log.debug("<== getSliceMultiSliceQuery table:{} keys:{} query:{} txh:{}",
          getContainerName(),
          encodeForLog(keys),
          encodeForLog(query),
          txh);
    }
  }

  private Stream<Entry> query(final StaticBuffer key, SliceQuery query,
      final StoreTransaction txh) {
    String itemId = encodeKey(key);
    String sql = "SELECT * FROM c where c.pk = '" + itemId
        + "' and c.id >= '" + encodeKey(query.getSliceStart())
        + "' and c.id < '" + encodeKey(query.getSliceEnd())
        + "'";

    CosmosPagedIterable<ObjectNode> iterable = new CosmosPagedIterable<>(getContainer().queryItems(sql,
        new CosmosQueryRequestOptions(), ObjectNode.class));
    // TODO make page size configurable?
    return iterable.stream()
        .flatMap(item -> {
          final Entry entry = new EntryBuilder(item)
              .slice(query.getSliceStart(), query.getSliceEnd())
              .build();
          return entry != null ? Stream.of(entry) : Stream.empty();
        })
        .limit(query.getLimit());
  }

  @Override
  public void mutate(final StaticBuffer key, final List<Entry> additions,
      final List<StaticBuffer> deletions, final StoreTransaction txh) throws BackendException {
    try {
      log.debug("==> mutate table:{} keys:{} additions:{} deletions:{} txh:{}",
          getContainerName(),
          encodeKeyForLog(key),
          encodeForLog(additions),
          encodeForLog(deletions),
          txh);
      // this method also filters out deletions that are also added
      super.mutateOneKey(key, new KCVMutation(additions, deletions), txh);
    } finally {
      log.debug("<== mutate table:{} keys:{} additions:{} deletions:{} txh:{} returning:void",
          getContainerName(),
          encodeKeyForLog(key),
          encodeForLog(additions),
          encodeForLog(deletions),
          txh);
    }
  }

  @Override
  public void mutateMany(
      final Map<StaticBuffer, KCVMutation> mutations, final StoreTransaction txh)
      throws BackendException {
    BulkWriter bulkWriter = new BulkWriter(getContainer());
    List<CosmosItemOperation> ops = mutations.entrySet().stream()
        .flatMap(entry -> convertToOps(entry.getKey(), entry.getValue()).stream())
        .collect(Collectors.toList());
    ops.forEach(bulkWriter::scheduleWrites);
    bulkWriter.execute(new CosmosBulkExecutionOptions())
        .take(ops.size())
        .blockLast();
  }

  protected List<CosmosItemOperation> convertToOps(StaticBuffer partitionKey, KCVMutation mutation) {
    List<CosmosItemOperation> result = new ArrayList<>();
    if (mutation.hasDeletions()) {
      for (StaticBuffer b : mutation.getDeletions()) {
        result.add(CosmosBulkOperations.getDeleteItemOperation(encodeKey(b), new PartitionKey(encodeKey(partitionKey))));
      }
    }

    if (mutation.hasAdditions()) {
      for (Entry e : mutation.getAdditions()) {
        ObjectNode item = new ItemBuilder()
            .partitionKey(partitionKey)
            .columnKey(e.getColumn())
            .value(e.getValue())
            .build();
        result.add(CosmosBulkOperations.getUpsertItemOperation(item, new PartitionKey(encodeKey(partitionKey))));
      }
    }
    return result;
  }

  @Override
  public String toString() {
    return "DynamoDBKeyColumnValueStore:" + getContainerName();
  }
}
