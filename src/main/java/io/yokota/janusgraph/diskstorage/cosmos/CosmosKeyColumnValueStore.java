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
package io.yokota.janusgraph.diskstorage.cosmos;

import java.util.Map;
import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.StaticBuffer;
import org.janusgraph.diskstorage.keycolumnvalue.KCVMutation;
import org.janusgraph.diskstorage.keycolumnvalue.KeyColumnValueStore;
import org.janusgraph.diskstorage.keycolumnvalue.StoreTransaction;

/**
 * Responsible for communicating with a single AWS backing store table.
 */
public interface CosmosKeyColumnValueStore extends KeyColumnValueStore {

  /**
   * Creates the KCV store and underlying DynamoDB tables.
   *
   * @throws BackendException if unable to ensure the underlying store
   */
  void ensureStore() throws BackendException;

  /**
   * Deletes the KCV store and underlying DynamoDB tables.
   *
   * @throws BackendException if unable to delete the underlying store
   */
  void deleteStore() throws BackendException;

  /**
   * Titan relies on static store names to be used, but we want the ability to have multiple graphs
   * in a single region, so prepend a configurable prefix to the underlying container names of each
   * graph and get the Cosmos DB container name with this method
   *
   * @return the container name corresponding to the KCVStore
   */
  String getContainerName();

  void mutateMany(
      final Map<StaticBuffer, KCVMutation> mutations, final StoreTransaction txh)
      throws BackendException;
}
