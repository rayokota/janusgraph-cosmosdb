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
package io.kcache.janusgraph.diskstorage.cosmos.iterator;

import static io.kcache.janusgraph.diskstorage.cosmos.builder.AbstractBuilder.decodeKey;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.Lists;
import io.kcache.janusgraph.diskstorage.cosmos.Constants;
import io.kcache.janusgraph.diskstorage.cosmos.CosmosStore;
import io.kcache.janusgraph.diskstorage.cosmos.builder.AbstractBuilder;
import io.kcache.janusgraph.diskstorage.cosmos.builder.EntryBuilder;
import io.kcache.janusgraph.diskstorage.cosmos.builder.KeyBuilder;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.NonNull;
import org.janusgraph.diskstorage.Entry;
import org.janusgraph.diskstorage.StaticBuffer;
import org.janusgraph.diskstorage.keycolumnvalue.SliceQuery;
import org.janusgraph.diskstorage.util.RecordIterator;
import reactor.core.publisher.Flux;
import reactor.core.publisher.GroupedFlux;

/**
 * Interprets Scan results for MULTI stores and assumes that results are SEQUENTIAL. This means that the scan is assumed to be non-segmented.
 * We need this assumption because it makes it so we don't need to keep track of where segment boundaries lie in order to avoid returning duplicate
 * hash keys.
 */
public class MultiRowFluxInterpreter implements FluxContextInterpreter<GroupedFlux<String, ObjectNode>> {

    @NonNull
    private final CosmosStore store;
    @NonNull
    private final SliceQuery sliceQuery;

    public MultiRowFluxInterpreter(CosmosStore store, SliceQuery sliceQuery) {
        this.store = store;
        this.sliceQuery = sliceQuery;
    }

    @Override
    public List<SingleKeyRecordIterator> buildRecordIterators(final Flux<GroupedFlux<String, ObjectNode>> flux) {
        return flux.flatMap(item -> {
            final StaticBuffer key = decodeKey(item.key());
            final RecordIterator<Entry> recordIterator = createRecordIterator(item);
            if (recordIterator.hasNext()) {
                return Flux.just(new SingleKeyRecordIterator(key, recordIterator));
            } else {
                return Flux.empty();
            }
        }).collectList().block();
    }

    private RecordIterator<Entry> createRecordIterator(final GroupedFlux<String, ObjectNode> flux) {
        return new StaticRecordIterator(flux.collectList()
            .block()
            // TODO fix NPE
            .stream()
            .flatMap(item -> {
                // DynamoDB's between includes the end of the range, but Titan's slice queries expect the end key to be exclusive
                final Entry entry = new EntryBuilder(item)
                    .slice(sliceQuery.getSliceStart(), sliceQuery.getSliceEnd())
                    .build();
                return entry != null ? Stream.of(entry) : Stream.empty();
            }).collect(Collectors.toList()));
    }
}
