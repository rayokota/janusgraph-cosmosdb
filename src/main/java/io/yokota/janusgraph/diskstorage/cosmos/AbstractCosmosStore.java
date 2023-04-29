/*
 * Copyright 2014-2018 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *  http://aws.amazon.com/apache2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */
package io.yokota.janusgraph.diskstorage.cosmos;

import com.azure.cosmos.CosmosAsyncClient;
import com.azure.cosmos.CosmosAsyncContainer;
import com.azure.cosmos.CosmosAsyncDatabase;
import com.azure.cosmos.models.CosmosContainerProperties;
import com.azure.cosmos.models.CosmosContainerRequestOptions;
import com.azure.cosmos.models.CosmosContainerResponse;
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
import org.janusgraph.diskstorage.keycolumnvalue.KeySliceQuery;
import org.janusgraph.diskstorage.keycolumnvalue.KeySlicesIterator;
import org.janusgraph.diskstorage.keycolumnvalue.MultiSlicesQuery;
import org.janusgraph.diskstorage.keycolumnvalue.SliceQuery;
import org.janusgraph.diskstorage.keycolumnvalue.StoreTransaction;
import reactor.core.publisher.Mono;

/**
 * The base class for the SINGLE and MULTI implementations of the Amazon DynamoDB Storage Backend
 * for JanusGraph distributed store type.
 *
 * @author Matthew Sowders
 * @author Alexander Patrikalakis
 */
@Slf4j
public abstract class AbstractCosmosStore implements CosmosKeyColumnValueStore {

  protected final CosmosAsyncClient client;
  private final String containerName;
  private final CosmosStoreManager manager;
  private final String name;
  // TODO
  private final boolean forceConsistentRead = false;
  private CosmosAsyncContainer container;

  AbstractCosmosStore(final CosmosStoreManager manager, final String prefix,
      final String storeName) {
    this.manager = manager;
    this.client = this.manager.getClient();
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

  @Override
  public KeySlicesIterator getKeys(MultiSlicesQuery queries, StoreTransaction txh) throws BackendException {
    throw new UnsupportedOperationException();
  }

  protected void mutateOneKey(final StaticBuffer key, final KCVMutation mutation, final StoreTransaction txh) throws BackendException {
    manager.mutateMany(Collections.singletonMap(name, Collections.singletonMap(key, mutation)), txh);
  }

  @Override
  public final void ensureStore() throws BackendException {
    log.debug("Entering ensureStore table:{}", containerName);
    createContainerIfNotExists();
  }

  @Override
  public final void deleteStore() throws BackendException {
    log.debug("Entering deleteStore name:{}", name);
    container.delete(new CosmosContainerRequestOptions()).block();
  }

  private void createContainerIfNotExists() {
    log.info("Create container " + containerName + " if not exists.");

    CosmosAsyncDatabase database = manager.getDatabase();
    // Create container if not exists
    CosmosContainerProperties containerProperties =
        new CosmosContainerProperties(containerName, "/" + Constants.JANUSGRAPH_PARTITION_KEY);
    ThroughputProperties throughputProperties = ThroughputProperties.createManualThroughput(400);
    Mono<CosmosContainerResponse> containerIfNotExists =
        database.createContainerIfNotExists(containerProperties, throughputProperties);

    // Create container with 400 RU/s
    CosmosContainerResponse cosmosContainerResponse = containerIfNotExists.block();
    container = database.getContainer(cosmosContainerResponse.getProperties().getId());

    // Modify existing container
    containerProperties = cosmosContainerResponse.getProperties();
    Mono<CosmosContainerResponse> propertiesReplace = container.replace(containerProperties,
        new CosmosContainerRequestOptions());
    propertiesReplace.flatMap(containerResponse -> {
      log.info("setupContainer(): Container " + container.getId() + " in " + database.getId() +
          "has been updated with it's new properties.");
      return Mono.empty();
    }).onErrorResume(exception -> {
      log.error("setupContainer(): Unable to update properties for container " + container.getId() +
          " in database " + database.getId() +
          ". e: " + exception.getLocalizedMessage());
      return Mono.empty();
    }).block();
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

  protected String encodeForLog(final SliceQuery query) {
    return "slice[rk:" + encodeKeyForLog(query.getSliceStart()) + " -> " + encodeKeyForLog(
        query.getSliceEnd()) + " limit:" + query.getLimit() + "]";
  }

  protected String encodeForLog(final KeySliceQuery query) {
    return "keyslice[hk:" + encodeKeyForLog(query.getKey()) + " " + "rk:" + encodeKeyForLog(
        query.getSliceStart()) + " -> " + encodeKeyForLog(query.getSliceEnd()) + " limit:"
        + query.getLimit() + "]";
  }
}
