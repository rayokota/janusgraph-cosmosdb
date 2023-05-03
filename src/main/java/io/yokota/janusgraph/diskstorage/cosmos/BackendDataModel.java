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

import lombok.Getter;
import lombok.RequiredArgsConstructor;

/**
 * Creates a store backend based on configuration.
 */
@RequiredArgsConstructor
public enum BackendDataModel {
  UNKNOWN("Unknown") {
    @Override
    public CosmosKeyColumnValueStore createStoreBackend(final CosmosStoreManager manager,
        final String prefix, final String name) {
      throw new UnsupportedOperationException();
    }

  },
  SINGLE("Single") {
    @Override
    public CosmosKeyColumnValueStore createStoreBackend(final CosmosStoreManager manager,
        final String prefix, final String name) {
      return new CosmosSingleRowStore(manager, prefix, name);
    }
  },
  MULTI("Multiple") {
    @Override
    public CosmosKeyColumnValueStore createStoreBackend(final CosmosStoreManager manager,
        final String prefix, final String name) {
      return new CosmosStore(manager, prefix, name);
    }
  };

  @Getter
  private final String camelCaseName;

  public abstract CosmosKeyColumnValueStore createStoreBackend(CosmosStoreManager manager,
      String prefix, String name);

}
