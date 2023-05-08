// Copyright 2017 JanusGraph Authors
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

import io.yokota.janusgraph.CosmosStorageSetup;
import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.LockKeyColumnValueStoreTest;
import org.janusgraph.diskstorage.configuration.Configuration;
import org.janusgraph.diskstorage.keycolumnvalue.KeyColumnValueStoreManager;

public class AbstractCosmosLockStoreTest extends LockKeyColumnValueStoreTest {

    protected final BackendDataModel model;

    protected AbstractCosmosLockStoreTest(final BackendDataModel model) {
        this.model = model;
    }

    public KeyColumnValueStoreManager openStorageManager(int idx, Configuration configuration) throws BackendException {
        return new CosmosStoreManager(CosmosStorageSetup.getCosmosConfiguration(model));
    }
}
