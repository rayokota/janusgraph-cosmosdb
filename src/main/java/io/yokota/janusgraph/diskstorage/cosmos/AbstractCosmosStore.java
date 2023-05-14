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

import com.azure.cosmos.CosmosAsyncContainer;
import com.azure.cosmos.CosmosAsyncDatabase;
import com.azure.cosmos.models.CosmosContainerProperties;
import com.azure.cosmos.models.CosmosContainerRequestOptions;
import com.azure.cosmos.models.ThroughputProperties;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.lang.builder.EqualsBuilder;
import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.Entry;
import org.janusgraph.diskstorage.StaticBuffer;
import org.janusgraph.diskstorage.keycolumnvalue.KCVMutation;
import org.janusgraph.diskstorage.keycolumnvalue.KeyIterator;
import org.janusgraph.diskstorage.keycolumnvalue.KeyRangeQuery;
import org.janusgraph.diskstorage.keycolumnvalue.KeySliceQuery;
import org.janusgraph.diskstorage.keycolumnvalue.KeySlicesIterator;
import org.janusgraph.diskstorage.keycolumnvalue.MultiSlicesQuery;
import org.janusgraph.diskstorage.keycolumnvalue.SliceQuery;
import org.janusgraph.diskstorage.keycolumnvalue.StoreTransaction;
import reactor.core.publisher.Mono;

/**
 * The base class for the SINGLE and MULTI implementations of the Cosmos DB Storage Backend
 * for JanusGraph distributed store type.
 */
@Slf4j
public abstract class AbstractCosmosStore implements CosmosKeyColumnValueStore {

  private final CosmosStoreManager manager;
  private final String name;
  private final String containerName;
  private CosmosAsyncContainer container;

  AbstractCosmosStore(final CosmosStoreManager manager, final String prefix,
      final String storeName) {
    this.manager = manager;
    this.name = storeName;
    this.containerName = prefix + "_" + storeName;
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public String getContainerName() {
    return containerName;
  }

  public CosmosAsyncContainer getContainer() {
    return container;
  }

  public int getBatchSize() {
    return manager.getBatchSize();
  }

  public int getPatchSize() {
    return manager.getPatchSize();
  }

  @Override
  public KeyIterator getKeys(final KeyRangeQuery query, final StoreTransaction txh)
      throws BackendException {
    throw new UnsupportedOperationException("Keys are not byte ordered.");
  }

  @Override
  public KeySlicesIterator getKeys(final MultiSlicesQuery queries, final StoreTransaction txh)
      throws BackendException {
    throw new UnsupportedOperationException();
  }

  protected void mutateOneKey(final StaticBuffer key, final KCVMutation mutation,
      final StoreTransaction txh) throws BackendException {
    manager.mutateMany(Collections.singletonMap(name, Collections.singletonMap(key, mutation)),
        txh);
  }

  @Override
  public final void ensureStore() throws BackendException {
    log.debug("==> ensureStore table:{}", containerName);
    createContainerIfNotExists();
    while (!exists()) {
      createContainerIfNotExists();
    }
  }

  @Override
  public final void deleteStore() throws BackendException {
    log.debug("==> deleteStore name:{}", containerName);
    if (container != null) {
      container.delete(new CosmosContainerRequestOptions())
          .onErrorResume(exception -> {
            log.warn("Could not delete container", exception);
            return Mono.empty();
          })
          .block();
    }
  }

  private void createContainerIfNotExists() {
    log.info("Create container {} if not exists.", containerName);

    //  Create container if not exists
    CosmosContainerProperties containerProperties =
        new CosmosContainerProperties(containerName, "/" + Constants.JANUSGRAPH_PARTITION_KEY);

    // Provision throughput, default 10000 RU/s
    ThroughputProperties throughputProperties =
        ThroughputProperties.createManualThroughput(
            manager.getStorageConfig().get(Constants.COSMOS_STORES_THROUGHPUT));

    //  Create container
    CosmosAsyncDatabase database = manager.getDatabase();
    database.createContainerIfNotExists(containerProperties, throughputProperties)
        .onErrorResume(exception -> {
          log.warn("Could not create container", exception);
          return Mono.empty();
        })
        .block();
    container = database.getContainer(containerName);
  }

  private boolean exists() throws BackendException {
    CosmosAsyncDatabase database = manager.getDatabase();
    List<CosmosContainerProperties> result = database.queryContainers(
            String.format("SELECT * FROM root r where r.id = '%s'", containerName) , null)
        .collectList()
        .onErrorResume(exception -> {
          log.warn("Could not query container:{}", containerName, exception);
          return Mono.empty();
        })
        .block();
    return result != null && !result.isEmpty();
  }

  @Override
  public void acquireLock(final StaticBuffer key, final StaticBuffer column,
      final StaticBuffer expectedValue, final StoreTransaction txh) throws BackendException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void close() throws BackendException {
    log.debug("Closing table:{}", containerName);
  }

  @Override
  public int hashCode() {
    return containerName.hashCode();
  }

  @Override
  public boolean equals(final Object obj) {
    if (obj == null) {
      return false;
    }
    if (obj == this) {
      return true;
    }
    if (obj.getClass() != getClass()) {
      return false;
    }
    final AbstractCosmosStore rhs = (AbstractCosmosStore) obj;
    return new EqualsBuilder().append(containerName, rhs.containerName).isEquals();
  }

  @Override
  public String toString() {
    return this.getClass().getName() + ":" + getContainerName();
  }

  String encodeKeyForLog(final StaticBuffer key) {
    if (null == key) {
      return "";
    }
    return Constants.HEX_PREFIX + Hex.encodeHexString(key.asByteBuffer().array());
  }

  String encodeForLog(final List<?> columns) {
    return columns.stream()
        .map(obj -> {
          if (obj instanceof StaticBuffer) {
            return (StaticBuffer) obj;
          } else if (obj instanceof Entry) {
            return ((Entry) obj).getColumn();
          } else {
            return null;
          }
        })
        .map(this::encodeKeyForLog)
        .collect(Collectors.joining(",", "[", "]"));
  }

  protected String encodeForLog(final SliceQuery query) {
    return "slice[rk:" + encodeKeyForLog(query.getSliceStart()) + " -> " + encodeKeyForLog(
        query.getSliceEnd()) + " limit:" + query.getLimit() + "]";
  }

  protected String encodeForLog(final KeySliceQuery query) {
    return "keyslice[pk:" + encodeKeyForLog(query.getKey()) + " " + "rk:" + encodeKeyForLog(
        query.getSliceStart()) + " -> " + encodeKeyForLog(query.getSliceEnd()) + " limit:"
        + query.getLimit() + "]";
  }
}
