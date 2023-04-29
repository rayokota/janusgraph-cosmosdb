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

import io.yokota.janusgraph.diskstorage.cosmos.BackendDataModel;
import io.yokota.janusgraph.diskstorage.cosmos.Constants;
import org.janusgraph.StorageSetup;
import org.janusgraph.diskstorage.configuration.ModifiableConfiguration;
import org.janusgraph.diskstorage.configuration.WriteConfiguration;

import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.*;

public class CosmosStorageSetup extends StorageSetup {


    public static ModifiableConfiguration getCosmosConfiguration() {
        return getCosmosConfiguration("janusgraph-test-cosmos");
    }

    public static ModifiableConfiguration getCosmosConfiguration(final String graphName) {
        return buildGraphConfiguration()
            .set(STORAGE_BACKEND,"io.yokota.janusgraph.diskstorage.cosmos.CosmosStoreManager")
            .set(Constants.COSMOS_CLIENT_ENDPOINT, "https://localhost:8081")
            .set(Constants.COSMOS_CLIENT_KEY, "C2y6yDjf5/R+ob0N8A7Cgv30VRDJIWEHLM+4QDU5DE2nQ9nDuVTqobD4b8mGGyPMbIZnqyMsEcaGQy67XIw/Jw==")
            .set(Constants.STORES_DATA_MODEL, BackendDataModel.MULTI.name(), graphName)
            .set(DROP_ON_CLEAR, false);
    }

    public static WriteConfiguration getCosmosGraphConfiguration() {
        return getCosmosConfiguration().getConfiguration();
    }
}
