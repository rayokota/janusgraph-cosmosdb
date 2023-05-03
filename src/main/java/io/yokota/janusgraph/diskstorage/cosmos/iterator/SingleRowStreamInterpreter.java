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

import com.fasterxml.jackson.databind.node.ObjectNode;
import io.yokota.janusgraph.diskstorage.cosmos.Constants;
import io.yokota.janusgraph.diskstorage.cosmos.builder.EntryBuilder;
import io.yokota.janusgraph.diskstorage.cosmos.builder.KeyBuilder;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Stream;
import org.janusgraph.diskstorage.Entry;
import org.janusgraph.diskstorage.StaticBuffer;
import org.janusgraph.diskstorage.keycolumnvalue.SliceQuery;
import org.janusgraph.diskstorage.util.RecordIterator;

/**
 * Turns Scan results into RecordIterators for stores using the SINGLE data model. This interpreter
 * doesn't need to consider whether or not a scan is segmented, because each item in a ScanResult
 * represents ALL columns for a given key. It is impossible for keys to be split across multiple
 * ScanResults (or segments for that matter) when using the SINGLE data model.
 */
public class SingleRowStreamInterpreter implements StreamContextInterpreter<ObjectNode> {

  private final SliceQuery sliceQuery;

  public SingleRowStreamInterpreter(final SliceQuery sliceQuery) {
    this.sliceQuery = sliceQuery;
  }

  @Override
  public Iterator<SingleKeyRecordIterator> buildRecordIterators(final Stream<ObjectNode> stream) {
    return stream.flatMap(item -> {
      final StaticBuffer key = new KeyBuilder(item).build(Constants.JANUSGRAPH_PARTITION_KEY);
      final RecordIterator<Entry> recordIterator = createRecordIterator(item);
      return recordIterator.hasNext()
          ? Stream.of(new SingleKeyRecordIterator(key, recordIterator))
          : Stream.empty();
    }).iterator();
  }

  private RecordIterator<Entry> createRecordIterator(final ObjectNode item) {
    item.remove(Constants.JANUSGRAPH_PARTITION_KEY);
    final List<Entry> entries = new EntryBuilder(item)
        .slice(sliceQuery.getSliceStart(), sliceQuery.getSliceEnd())
        .limit(sliceQuery.getLimit())
        .buildAll();
    return new StaticRecordIterator(entries);
  }
}
