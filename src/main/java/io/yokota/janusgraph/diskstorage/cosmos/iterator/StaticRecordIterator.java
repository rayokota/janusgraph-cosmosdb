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
import java.util.Iterator;
import org.janusgraph.diskstorage.Entry;
import org.janusgraph.diskstorage.util.RecordIterator;

/**
 * Iterator for entries. This implementation is eagerly loaded. A further improvement might be to
 * add a lazily loaded implementation.
 */
public class StaticRecordIterator implements RecordIterator<Entry> {

  private Iterator<Entry> delegate;

  public StaticRecordIterator(final Iterable<Entry> entries) {
    this.delegate = entries.iterator();
  }

  @Override
  public boolean hasNext() {
    if (null == delegate) {
      return false;
    }
    return delegate.hasNext();
  }

  @Override
  public Entry next() {
    if (null == delegate) {
      throw new IllegalStateException();
    }
    return delegate.next();
  }

  @Override
  public void remove() {
    throw new UnsupportedOperationException();
  }

  @Override
  public void close() throws IOException {
    delegate = null;
  }
}
