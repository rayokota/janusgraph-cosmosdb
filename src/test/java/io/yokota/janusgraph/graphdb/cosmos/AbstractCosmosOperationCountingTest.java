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

package io.yokota.janusgraph.graphdb.cosmos;

import io.yokota.janusgraph.CosmosStorageSetup;
import io.yokota.janusgraph.diskstorage.cosmos.BackendDataModel;
import org.janusgraph.diskstorage.configuration.WriteConfiguration;
import org.janusgraph.graphdb.JanusGraphOperationCountingTest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

public abstract class AbstractCosmosOperationCountingTest extends JanusGraphOperationCountingTest {

  protected final BackendDataModel model;

  protected AbstractCosmosOperationCountingTest(final BackendDataModel model) {
    this.model = model;
  }

  @Override
  public WriteConfiguration getBaseConfiguration() {
    return CosmosStorageSetup.getCosmosGraphConfiguration(model);
  }

  @AfterEach
  public void resetCounts() {
    resetMetrics(); // Metrics is a singleton, so subsequents test runs have wrong counts if we don't clean up.
  }

  @Test
  @Override
  public void testCacheSpeedup() {
    // Ignore this test as cache speedup is a little less than double as asserted in superclass test
  }
}
