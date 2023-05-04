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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.yokota.janusgraph.CosmosStorageSetup;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;
import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.Entry;
import org.janusgraph.diskstorage.KeyColumn;
import org.janusgraph.diskstorage.KeyValueStoreUtil;
import org.janusgraph.diskstorage.StaticBuffer;
import org.janusgraph.diskstorage.keycolumnvalue.KCVSUtil;
import org.janusgraph.diskstorage.keycolumnvalue.KeyColumnValueStore;
import org.janusgraph.diskstorage.keycolumnvalue.KeyColumnValueStoreManager;
import org.janusgraph.diskstorage.keycolumnvalue.KeyIterator;
import org.janusgraph.diskstorage.keycolumnvalue.StoreTransaction;
import org.janusgraph.diskstorage.util.RecordIterator;
import org.janusgraph.diskstorage.util.StaticArrayBuffer;
import org.janusgraph.diskstorage.util.StaticArrayEntry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

@Slf4j
public class DummyCosmosStoreTest extends DummyKeyColumnValueStoreTest {

  @Override
  public KeyColumnValueStoreManager openStorageManager() throws BackendException {
    return new CosmosStoreManager(
        CosmosStorageSetup.getCosmosConfiguration(BackendDataModel.SINGLE));
  }

  static int nKeys = 10;
  static int nColumns = 2;

  public String[][] generateValues() {
    return KeyValueStoreUtil.generateData(nKeys, nColumns);
  }

  public static void loadAndCheck(KeyColumnValueStore store, StoreTransaction tx, String[][] values,
      int shiftEveryNthRow,
      int shiftSliceLength) throws BackendException {
    for (int i = 0; i < values.length; i++) {

      final List<Entry> entries = new ArrayList<>();
      for (int j = 0; j < values[i].length; j++) {
        StaticBuffer col;
        if (0 < shiftEveryNthRow && 0 == i/* +1 */ % shiftEveryNthRow) {
          ByteBuffer bb = ByteBuffer.allocate(shiftSliceLength + 9);
          for (int s = 0; s < shiftSliceLength; s++) {
            bb.put((byte) -1);
          }
          bb.put(KeyValueStoreUtil.getBuffer(j + 1).asByteBuffer());
          bb.flip();
          col = StaticArrayBuffer.of(bb);

          // col = KeyValueStoreUtil.getBuffer(j + values[i].length +
          // 100);
        } else {
          col = KeyValueStoreUtil.getBuffer(j);
        }
        entries.add(StaticArrayEntry.of(col, KeyValueStoreUtil
            .getBuffer(values[i][j])));
      }
      if (!entries.isEmpty()) {
        store.mutate(KeyValueStoreUtil.getBuffer(i), entries,
            KeyColumnValueStore.NO_DELETIONS, tx);
        for (int j = 0; j < nColumns; j++) {
          boolean result = KCVSUtil.containsKeyColumn(store, KeyValueStoreUtil.getBuffer(i),
              KeyValueStoreUtil.getBuffer(j), tx);
          if (!result) {
            log.error("Could not find i " + i + ", j " + j);
          }
          assertTrue(result);
        }
      }
    }
  }

  public void checkValueExistence(String[][] values) throws BackendException {
    checkValueExistence(values, new HashSet<>());
  }

  public void checkValueExistence(String[][] values, Set<KeyColumn> removed)
      throws BackendException {
    for (int i = 0; i < nKeys; i++) {
      for (int j = 0; j < nColumns; j++) {
        boolean result = KCVSUtil.containsKeyColumn(store, KeyValueStoreUtil.getBuffer(i),
            KeyValueStoreUtil.getBuffer(j), tx);
        if (removed.contains(new KeyColumn(i, j))) {
          assertFalse(result);
        } else {
          assertTrue(result);
        }
      }
    }
  }

  public void checkValues(String[][] values) throws BackendException {
    checkValues(values, new HashSet<>());
  }

  public void checkValues(String[][] values, Set<KeyColumn> removed) throws BackendException {
    for (int i = 0; i < nKeys; i++) {
      for (int j = 0; j < nColumns; j++) {
        StaticBuffer result = KCVSUtil.get(store, KeyValueStoreUtil.getBuffer(i),
            KeyValueStoreUtil.getBuffer(j), tx);
        if (removed.contains(new KeyColumn(i, j))) {
          assertNull(result);
        } else {
          assertEquals(values[i][j], KeyValueStoreUtil.getString(result));
        }
      }
    }

  }

  @Test
  public void testClearStorage() throws Exception {
    final String[][] values = generateValues();
    loadValues(values);
    close();

    manager = openStorageManagerForClearStorageTest();
    assertTrue(manager.exists(), "storage should exist before clearing");
    manager.clearStorage();
    try {
      assertFalse(manager.exists(), "storage should not exist after clearing");
    } catch (Exception e) {
      // Retry to accommodate backends (e.g. BerkeleyDB) which may require a clean manager after clearing storage
      manager.close();
      manager = openStorageManager();
      assertFalse(manager.exists(), "storage should not exist after clearing");
    }
  }
  //@Test
  public void storeAndRetrieveWithClosing() throws BackendException {
    String[][] values = generateValues();
    loadValues(values);
    clopen();
    checkValueExistence(values);
    checkValues(values);
  }

  //@Test
  public void storeAndRetrieve() throws BackendException {
    String[][] values = generateValues();
    //loadValues(values);
    loadAndCheck(store, tx, values, -1, -1);
    //print(values);
    checkValueExistence(values);
    checkValues(values);
  }

  //@Test
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
