//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package io.yokota.janusgraph.diskstorage.cosmos;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.yokota.janusgraph.CosmosStorageSetup;
import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.Entry;
import org.janusgraph.diskstorage.KeyValueStoreUtil;
import org.janusgraph.diskstorage.StaticBuffer;
import org.janusgraph.diskstorage.keycolumnvalue.KCVSUtil;
import org.janusgraph.diskstorage.keycolumnvalue.KeyColumnValueStoreManager;
import org.janusgraph.diskstorage.keycolumnvalue.KeyIterator;
import org.janusgraph.diskstorage.util.RecordIterator;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

public class DummyCosmosStoreTest extends DummyKeyColumnValueStoreTest {

  @Override
  public KeyColumnValueStoreManager openStorageManager() throws BackendException {
    return new CosmosStoreManager(CosmosStorageSetup.getCosmosConfiguration(BackendDataModel.SINGLE));
  }

  int nKeys = 500;
  int nColumns = 50;

  public String[][] generateValues() {
    return KeyValueStoreUtil.generateData(nKeys, nColumns);
  }

  @Test
  public void scanTest() throws BackendException {
    String[][] values = generateValues();
    loadValues(values);
    KeyIterator iterator0 = KCVSUtil.getKeys(store, storeFeatures(), 8, 4, tx);
    verifyIterator(iterator0, nKeys);
    clopen();
    KeyIterator iterator1 = KCVSUtil.getKeys(store, storeFeatures(), 8, 4, tx);
    KeyIterator iterator2 = KCVSUtil.getKeys(store, storeFeatures(), 8, 4, tx);
    // The idea is to open an iterator without using it
    // to make sure that closing a transaction will clean it up.
    // (important for BerkeleyJE where leaving cursors open causes exceptions)
    @SuppressWarnings("unused")
    KeyIterator iterator3 = KCVSUtil.getKeys(store, storeFeatures(), 8, 4, tx);
    verifyIterator(iterator1, nKeys);
    verifyIterator(iterator2, nKeys);
  }

  private void verifyIterator(KeyIterator iterator, int expectedKeys) {
    int keys = 0;
    while (iterator.hasNext()) {
      StaticBuffer b = iterator.next();
      assertTrue(b != null && b.length() > 0);
      keys++;
      RecordIterator<Entry> entryRecordIterator = iterator.getEntries();
      int cols = 0;
      while (entryRecordIterator.hasNext()) {
        Entry e = entryRecordIterator.next();
        assertTrue(e != null && e.length() > 0);
        cols++;
      }
      assertEquals(1, cols);
    }
    assertEquals(expectedKeys, keys);
  }

  @AfterEach
  public void tearDown() throws Exception {
    if (null != this.manager) {
      this.manager.clearStorage();
    }
    super.tearDown();
  }
}
