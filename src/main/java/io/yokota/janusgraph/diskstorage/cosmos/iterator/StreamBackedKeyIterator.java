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

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.stream.Stream;
import org.janusgraph.diskstorage.Entry;
import org.janusgraph.diskstorage.StaticBuffer;
import org.janusgraph.diskstorage.keycolumnvalue.KeyIterator;
import org.janusgraph.diskstorage.util.RecordIterator;

/**
 * KeyIterator that is backed by a DynamoDB scan. This class is ignorant to the fact that its
 * backing scan might be running in parallel. The ScanContextInterpreter is expected to be
 * compatible with whatever scan order the scanner is using.
 */
public class StreamBackedKeyIterator<T> implements KeyIterator {

  private final Stream<T> stream;
  private final StreamContextInterpreter<T> interpreter;

  private SingleKeyRecordIterator current;
  private Iterator<SingleKeyRecordIterator> recordIterators = Collections.emptyIterator();

  public StreamBackedKeyIterator(final Stream<T> stream,
      final StreamContextInterpreter<T> interpreter) {
    this.stream = stream;
    this.interpreter = interpreter;
    this.recordIterators = interpreter.buildRecordIterators(stream);
  }

  @Override
  public RecordIterator<Entry> getEntries() {
    return current.getRecordIterator();
  }

  @Override
  public void close() throws IOException {
    recordIterators = Collections.emptyIterator();
  }

  @Override
  public boolean hasNext() {
    return recordIterators.hasNext();
  }

  @Override
  public StaticBuffer next() {
    current = recordIterators.next();
    return current.getKey();
  }

  @Override
  public void remove() {
    throw new UnsupportedOperationException("remove");
  }
}
