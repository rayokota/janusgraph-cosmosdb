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

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import lombok.extern.slf4j.Slf4j;
import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.configuration.Configuration;

/**
 * Creates backend store based on table name.
 */
@Slf4j
public class ContainerNameCosmosStoreFactory implements CosmosStoreFactory {


  private final Configuration config;
  private final ConcurrentMap<String, CosmosKeyColumnValueStore> stores = new ConcurrentHashMap<>();

  public ContainerNameCosmosStoreFactory(Configuration config) {
    this.config = config;
  }

  @Override
  public CosmosKeyColumnValueStore create(final CosmosStoreManager manager, final String prefix,
      final String name) throws BackendException {
    log.debug("==> TableNameDynamoDbStoreFactory.create prefix:{} name:{}", prefix, name);
    // ensure there is only one instance used per table name.

    BackendDataModel model = BackendDataModel.valueOf(
        config.get(Constants.STORES_DATA_MODEL, name));
    if (model == BackendDataModel.UNKNOWN) {
      model = BackendDataModel.valueOf(config.get(Constants.STORES_DATA_MODEL_DEFAULT));
    }
    log.debug("=== TableNameDynamoDbStoreFactory.create model:{}", model);
    final CosmosKeyColumnValueStore storeBackend = model.createStoreBackend(manager, prefix, name);
    final CosmosKeyColumnValueStore previous = stores.putIfAbsent(name, storeBackend);
    if (null == previous) {
      try {
        storeBackend.ensureStore();
      } catch (BackendException e) {
        throw e;
      }
    }
    final CosmosKeyColumnValueStore store = stores.get(name);
    log.debug("<== TableNameDynamoDbStoreFactory.create prefix:{} name:{} returning:{}", prefix,
        name, store);
    return store;
  }

  @Override
  public Iterable<CosmosKeyColumnValueStore> getAllStores() {
    return stores.values();
  }

  @Override
  public CosmosKeyColumnValueStore getStore(final String store) {
    return stores.get(store);
  }

}
