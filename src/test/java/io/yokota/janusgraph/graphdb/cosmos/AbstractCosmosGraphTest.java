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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import io.yokota.janusgraph.CosmosStorageSetup;
import io.yokota.janusgraph.diskstorage.cosmos.BackendDataModel;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import org.janusgraph.core.JanusGraphException;
import org.janusgraph.core.JanusGraphFactory;
import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.configuration.WriteConfiguration;
import org.janusgraph.graphdb.JanusGraphTest;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractCosmosGraphTest extends JanusGraphTest {

  private static final Logger log =
      LoggerFactory.getLogger(AbstractCosmosGraphTest.class);

  protected final BackendDataModel model;

  protected AbstractCosmosGraphTest(final BackendDataModel model) {
    this.model = model;
  }

  @Override
  public WriteConfiguration getConfiguration() {
    return CosmosStorageSetup.getCosmosGraphConfiguration(model);
  }

  @Test
  @Override
  public void testClearStorage() {

  }

  @Test
  @Override
  public void testConsistencyEnforcement() {
  }


  @Test
  @Override
  public void testConcurrentConsistencyEnforcement() {
  }

  @Test
  public void testIDBlockAllocationTimeout() throws BackendException {
    config.set("ids.authority.wait-time", Duration.of(0L, ChronoUnit.NANOS));
    config.set("ids.renew-timeout", Duration.of(1L, ChronoUnit.MILLIS));
    close();
    JanusGraphFactory.drop(graph);
    open(config);
    try {
      graph.addVertex();
      fail();
    } catch (JanusGraphException ignored) {

    }

    assertTrue(graph.isOpen());

    close(); // must be able to close cleanly

    // Must be able to reopen
    open(config);

    assertEquals(0L, (long) graph.traversal().V().count().next());
  }
}
