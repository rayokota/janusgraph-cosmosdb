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
package io.yokota.janusgraph.diskstorage.cosmos.builder;

import com.fasterxml.jackson.databind.node.ObjectNode;
import io.yokota.janusgraph.diskstorage.cosmos.Constants;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.janusgraph.diskstorage.Entry;
import org.janusgraph.diskstorage.StaticBuffer;
import org.janusgraph.diskstorage.util.BufferUtil;
import org.janusgraph.diskstorage.util.StaticArrayEntry;

/**
 * EntryBuilder is responsible for translating from Cosmos DB item maps to Entry objects.
 */
public class EntryBuilder extends AbstractBuilder {

  private final ObjectNode item;
  private StaticBuffer start;
  private StaticBuffer end;
  private boolean slice;
  @Setter
  @Accessors(fluent = true)
  private int limit = Integer.MAX_VALUE;

  public EntryBuilder(final ObjectNode item) {
    this.item = item;
  }

  public List<Entry> buildAll() {
    if (null == item) {
      return Collections.emptyList();
    }
    final Entry sliceStartEntry;
    final Entry sliceEndEntry;
    if (slice) {
      sliceStartEntry = StaticArrayEntry.of(start, BufferUtil.emptyBuffer());
      sliceEndEntry = StaticArrayEntry.of(end, BufferUtil.emptyBuffer());
    } else {
      sliceStartEntry = null;
      sliceEndEntry = null;
    }
    return item.properties().stream()
        .filter(entry -> !ignoreAttribute(entry.getKey()))
        .map(entry -> {
          final StaticBuffer columnKey = decodeKey(entry.getKey());
          final StaticBuffer value = decodeValue(entry.getValue().textValue());
          return StaticArrayEntry.of(columnKey, value);
        })
        .filter(entry -> !slice
            || (entry.compareTo(sliceStartEntry) >= 0 && entry.compareTo(sliceEndEntry) < 0))
        .sorted()
        .limit(limit)
        .collect(Collectors.toList());
  }

  private boolean ignoreAttribute(String name) {
    return name.equals(Constants.JANUSGRAPH_PARTITION_KEY)
        || name.equals((Constants.JANUSGRAPH_COLUMN_KEY))
        || name.startsWith("_");  // ignore internal Cosmos DB attributes
  }

  public Entry build() {
    if (null == item) {
      return null;
    }

    final StaticBuffer rangeKey = decodeKey(item, Constants.JANUSGRAPH_COLUMN_KEY);
    return build(rangeKey);
  }

  public Entry build(final StaticBuffer column) {
    if (null == item || null == column) {
      return null;
    }

    if (slice && (column.compareTo(start) < 0 || column.compareTo(end) >= 0)) {
      return null;
    }

    final String valueValue = item.get(Constants.JANUSGRAPH_VALUE).textValue();
    final StaticBuffer value = decodeValue(valueValue);

    return StaticArrayEntry.of(column, value);
  }

  public EntryBuilder slice(final StaticBuffer sliceStart, final StaticBuffer sliceEnd) {
    this.start = sliceStart;
    this.end = sliceEnd;
    this.slice = true;
    return this;
  }
}
