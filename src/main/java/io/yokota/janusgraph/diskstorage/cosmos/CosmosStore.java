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

import com.azure.cosmos.models.CosmosItemRequestOptions;
import com.azure.cosmos.models.CosmosItemResponse;
import com.azure.cosmos.models.CosmosQueryRequestOptions;
import com.azure.cosmos.models.PartitionKey;
import com.azure.cosmos.util.CosmosPagedFlux;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.yokota.janusgraph.diskstorage.cosmos.builder.EntryBuilder;
import io.yokota.janusgraph.diskstorage.cosmos.builder.ItemBuilder;
import io.yokota.janusgraph.diskstorage.cosmos.iterator.FluxBackedKeyIterator;
import io.yokota.janusgraph.diskstorage.cosmos.iterator.MultiRowFluxInterpreter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
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
import reactor.core.publisher.Flux;
import reactor.core.publisher.GroupedFlux;
import reactor.core.publisher.Mono;
import reactor.util.function.Tuple2;

/**
 * Acts as if DynamoDB were a Column Oriented Database by using range query when
 * required.
 *
 * See configuration
 * storage.dynamodb.stores.***table_name***.data-model=MULTI
 *
 * KCV Schema - actual table (Hash(S) + Range(S)):
 * hk(S)  |  rk(S)  |  v(B)  <-Attribute Names
 * 0x01   |  0x02   |  0x03  <-Row Values
 * 0x01   |  0x04   |  0x05  <-Row Values
 *
 * @author Matthew Sowders
 * @author Alexander Patrikalakis
 * @author Michael Rodaitis
 *
 */
@Slf4j
public class CosmosStore extends AbstractCosmosStore {

    public CosmosStore(final CosmosStoreManager manager, final String prefix, final String storeName) {
        super(manager, prefix, storeName);
    }

    @Override
    public KeyIterator getKeys(final KeyRangeQuery query, final StoreTransaction txh) throws BackendException {
        throw new UnsupportedOperationException("Byteorder is not maintained.");
    }

    @Override
    public KeyIterator getKeys(final SliceQuery query, final StoreTransaction txh) throws BackendException {
        log.debug("Entering getKeys table:{} query:{} txh:{}", getContainerName(), encodeForLog(query), txh);

        String sql = "SELECT * FROM c";
        CosmosPagedFlux<ObjectNode> pagedFlux = getContainer().queryItems(sql, new CosmosQueryRequestOptions(), ObjectNode.class);
        // TODO make page size configurable?
        Flux<GroupedFlux<String, ObjectNode>> flux = pagedFlux
            .byPage(100)
            .flatMap(response -> Flux.fromIterable(response.getResults()))
            .groupBy(item -> item.get(Constants.JANUSGRAPH_PARTITION_KEY).textValue());

        log.debug("Exiting getKeys table:{} query:{} txh:{}", getContainerName(), encodeForLog(query), txh);
        return new FluxBackedKeyIterator<>(flux, new MultiRowFluxInterpreter(this, query));
    }

    @Override
    public EntryList getSlice(final KeySliceQuery query, final StoreTransaction txh)
            throws BackendException {
        log.debug("Entering getSliceKeySliceQuery table:{} query:{} txh:{}", getContainerName(), encodeForLog(query), txh);

        SliceQuery sliceQuery = new SliceQuery(query.getSliceStart(), query.getSliceEnd());
        if (query.hasLimit()) {
            sliceQuery.setLimit(query.getLimit());
        }
        Flux<Entry> flux = query(query.getKey(), sliceQuery, txh);
        log.debug("Exiting getSliceKeySliceQuery table:{} query:{} txh:{}", getContainerName(), encodeForLog(query), txh);
        // TODO make page size configurable?
        /*
        List<Entry> entries = flux.collectList().block();
        return StaticArrayEntryList.of(entries);
         */
        return StaticArrayEntryList.of(flux.toIterable());
    }

    private Flux<Entry> query(final StaticBuffer key, SliceQuery query, final StoreTransaction txh) {
        String itemId = encodeKey(key);
        String sql = "SELECT * FROM c where c.pk = '" + itemId
            + "' and c.id >= '" + encodeKey(query.getSliceStart())
            + "' and c.id < '" + encodeKey(query.getSliceEnd())
            + "'";

        CosmosPagedFlux<ObjectNode> pagedFlux = getContainer().queryItems(sql, new CosmosQueryRequestOptions(), ObjectNode.class);
        log.debug("Exiting getSliceKeySliceQuery table:{} query:{} txh:{}", getContainerName(), encodeForLog(
            query), txh);
        // TODO make page size configurable?
        return pagedFlux
            .byPage(100)
            .flatMap(response -> Flux.fromIterable(response.getResults()))
            .flatMap(item -> {
                final Entry entry = new EntryBuilder(item)
                    .slice(query.getSliceStart(), query.getSliceEnd())
                    .build();
                return entry != null ? Flux.just(entry) : Flux.empty();
            })
            .take(query.getLimit());
    }

    @Override
    public Map<StaticBuffer, EntryList> getSlice(final List<StaticBuffer> keys, final SliceQuery query, final StoreTransaction txh) throws BackendException {
        log.debug("Entering getSliceMultiSliceQuery table:{} keys:{} query:{} txh:{}",
                  getContainerName(),
                  encodeForLog(keys),
                  encodeForLog(query),
                  txh);

        return Flux.fromIterable(keys)
            .parallel()
            .flatMap(key -> Mono.zip(Mono.just(key), query(key, query, txh).collectList()))
            .sequential()
            .collectMap(Tuple2::getT1, tuple -> StaticArrayEntryList.of(tuple.getT2()))
            .block();
    }

    @Override
    public void mutate(final StaticBuffer key, final List<Entry> additions, final List<StaticBuffer> deletions, final StoreTransaction txh) throws BackendException {
        log.debug("Entering mutate table:{} keys:{} additions:{} deletions:{} txh:{}",
                  getContainerName(),
                  encodeKeyForLog(key),
                  encodeForLog(additions),
                  encodeForLog(deletions),
                  txh);
        // this method also filters out deletions that are also added
        super.mutateOneKey(key, new KCVMutation(additions, deletions), txh);

        log.debug("Exiting mutate table:{} keys:{} additions:{} deletions:{} txh:{} returning:void",
                  getContainerName(),
                  encodeKeyForLog(key),
                  encodeForLog(additions),
                  encodeForLog(deletions),
                  txh);
    }

    @Override
    public Stream<Mono<Void>> mutateMany(
        final Map<StaticBuffer, KCVMutation> mutations, final StoreTransaction txh)
        throws BackendException {
        return mutations.entrySet().stream()
            .flatMap(entry -> mutate(entry.getKey(), entry.getValue())
                .stream()
                .map(Mono::then)
            );
    }

    protected List<Mono<CosmosItemResponse<Object>>> mutate(StaticBuffer partitionKey, KCVMutation mutation) {
        List<Mono<CosmosItemResponse<Object>>> responses = new ArrayList<>();

        if (mutation.hasDeletions()) {
            for (StaticBuffer b : mutation.getDeletions()) {
                Mono<CosmosItemResponse<Object>> response = getContainer().deleteItem(encodeKey(b), new PartitionKey(encodeKey(partitionKey)), new CosmosItemRequestOptions());
                responses.add(response);
            }
        }

        if (mutation.hasAdditions()) {
            for (Entry e : mutation.getAdditions()) {
                ObjectNode item = new ItemBuilder()
                    .partitionKey(partitionKey)
                    .columnKey(e.getColumn())
                    .value(e.getValue())
                    .build();
                Mono<CosmosItemResponse<Object>> response = getContainer().upsertItem(item, new PartitionKey(encodeKey(partitionKey)), new CosmosItemRequestOptions());
                responses.add(response);
            }
        }
        return responses;
    }

    @Override
    public String toString() {
        return "DynamoDBKeyColumnValueStore:" + getContainerName();
    }
}
