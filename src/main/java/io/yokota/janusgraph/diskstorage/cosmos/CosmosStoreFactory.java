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

import org.janusgraph.diskstorage.BackendException;

/**
 * Creates a backend store for a given container name.
 */
public interface CosmosStoreFactory {

  /**
   * Creates a backend store for a given container name.
   *
   * @param prefix the prefix of the container name. For example if prefix was foo and name was bar, the
   *               full container name would be foo_bar. The prefix is shared by all stores created by a
   *               factory.
   * @param name   the name of the KCVStore, without the prefix.
   * @return a KCVStore with the given name and container prefix
   * @throws BackendException if unable to create the store
   */
  CosmosKeyColumnValueStore create(CosmosStoreManager manager, String prefix, String name)
      throws BackendException;

  /**
   * Gets all stores created by this factory.
   *
   * @return an Iterable of all the stores interned in this factory
   */
  Iterable<CosmosKeyColumnValueStore> getAllStores();

  /**
   * Gets backend store for store name.
   *
   * @param store the name of the store to get
   * @return the KCVStore that corresponds to the store name provided
   */
  CosmosKeyColumnValueStore getStore(String store);

}
