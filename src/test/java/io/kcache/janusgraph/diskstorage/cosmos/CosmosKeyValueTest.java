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

package io.kcache.janusgraph.diskstorage.cosmos;

import io.kcache.janusgraph.CosmosStorageSetup;
import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.KeyValueStoreTest;
import org.janusgraph.diskstorage.keycolumnvalue.keyvalue.OrderedKeyValueStoreManager;
import org.junit.jupiter.api.AfterEach;

public class CosmosKeyValueTest extends KeyValueStoreTest {

    @Override
    public OrderedKeyValueStoreManager openStorageManager() throws BackendException {
        return new CosmosStoreManager(CosmosStorageSetup.getKafkaConfiguration());
    }

    @AfterEach
    public void tearDown() throws Exception {
        if (null != this.manager) {
            this.manager.clearStorage();
        }
        super.tearDown();
    }
}
