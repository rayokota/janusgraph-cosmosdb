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

package io.yokota.janusgraph;

import static io.yokota.janusgraph.diskstorage.cosmos.Constants.COSMOS_CLIENT_ENDPOINT;
import static io.yokota.janusgraph.diskstorage.cosmos.Constants.COSMOS_CLIENT_KEY;
import static io.yokota.janusgraph.diskstorage.cosmos.Constants.STORES_DATA_MODEL;
import static io.yokota.janusgraph.diskstorage.cosmos.Constants.STORES_DATA_MODEL_DEFAULT;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.DROP_ON_CLEAR;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.STORAGE_BACKEND;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.buildGraphConfiguration;

import io.yokota.janusgraph.diskstorage.cosmos.BackendDataModel;
import org.janusgraph.StorageSetup;
import org.janusgraph.diskstorage.configuration.ModifiableConfiguration;
import org.janusgraph.diskstorage.configuration.WriteConfiguration;

public class CosmosStorageSetup extends StorageSetup {


  public static ModifiableConfiguration getCosmosConfiguration(final String graphName) {
    return getCosmosConfiguration(graphName, BackendDataModel.MULTI);
  }

  public static ModifiableConfiguration getCosmosConfiguration(final BackendDataModel model) {
    return getCosmosConfiguration(null, model);
  }

  public static ModifiableConfiguration getCosmosConfiguration(final String graphName,
      final BackendDataModel model) {
    ModifiableConfiguration config = buildGraphConfiguration()
        .set(STORAGE_BACKEND, "io.yokota.janusgraph.diskstorage.cosmos.CosmosStoreManager")
        .set(STORES_DATA_MODEL_DEFAULT, model.name())
        .set(COSMOS_CLIENT_ENDPOINT, "https://localhost:8081")
        .set(COSMOS_CLIENT_KEY,
            "C2y6yDjf5/R+ob0N8A7Cgv30VRDJIWEHLM+4QDU5DE2nQ9nDuVTqobD4b8mGGyPMbIZnqyMsEcaGQy67XIw/Jw==")
        .set(DROP_ON_CLEAR, false);

    if (graphName != null) {
      config.set(STORES_DATA_MODEL, model.name(), graphName);
    }
    return config;
  }

  public static WriteConfiguration getCosmosGraphConfiguration(final BackendDataModel model) {
    return getCosmosConfiguration(model).getConfiguration();
  }
}
