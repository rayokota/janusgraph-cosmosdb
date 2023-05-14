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
package io.yokota.janusgraph.diskstorage.cosmos.iterator;

import static io.yokota.janusgraph.diskstorage.cosmos.builder.AbstractBuilder.decodeKey;

import com.fasterxml.jackson.databind.node.ObjectNode;
import io.yokota.janusgraph.diskstorage.cosmos.Constants;
import io.yokota.janusgraph.diskstorage.cosmos.CosmosStore;
import io.yokota.janusgraph.diskstorage.cosmos.builder.EntryBuilder;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.NonNull;
import org.janusgraph.diskstorage.Entry;
import org.janusgraph.diskstorage.StaticBuffer;
import org.janusgraph.diskstorage.keycolumnvalue.SliceQuery;

/**
 * Interprets Scan results for MULTI stores and assumes that results are SEQUENTIAL. This means that
 * the scan is assumed to be non-segmented. We need this assumption because it makes it so we don't
 * need to keep track of where segment boundaries lie in order to avoid returning duplicate hash
 * keys.
 */
public class MultiRowStreamInterpreter implements StreamContextInterpreter<List<ObjectNode>> {

  @NonNull
  private final SliceQuery sliceQuery;

  public MultiRowStreamInterpreter(SliceQuery sliceQuery) {
    this.sliceQuery = sliceQuery;
  }

  @Override
  public Iterator<SingleKeyRecordIterator> buildRecordIterators(
      final Stream<List<ObjectNode>> stream) {
    return stream.flatMap(items -> {
      if (items.isEmpty()) {
        return Stream.empty();
      }
      final String partitionKey = items.get(0).get(Constants.JANUSGRAPH_PARTITION_KEY).textValue();
      final StaticBuffer key = decodeKey(partitionKey);
      final StaticRecordIterator recordIterator = createRecordIterator(items);
      return recordIterator.hasNext()
          ? Stream.of(new SingleKeyRecordIterator(key, recordIterator))
          : Stream.empty();
    }).iterator();
  }

  private StaticRecordIterator createRecordIterator(final List<ObjectNode> items) {
    return new StaticRecordIterator(items.stream()
        .flatMap(item -> {
          final Entry entry = new EntryBuilder(item)
              .slice(sliceQuery.getSliceStart(), sliceQuery.getSliceEnd())
              .build();
          return entry != null ? Stream.of(entry) : Stream.empty();
        })
        .limit(sliceQuery.getLimit())
        .collect(Collectors.toList()));
  }
}
