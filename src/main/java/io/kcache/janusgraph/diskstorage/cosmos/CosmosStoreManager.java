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
package io.kcache.janusgraph.diskstorage.cosmos;

import com.azure.cosmos.ConsistencyLevel;
import com.azure.cosmos.CosmosAsyncClient;
import com.azure.cosmos.CosmosAsyncDatabase;
import com.azure.cosmos.CosmosClientBuilder;
import com.azure.cosmos.models.CosmosDatabaseRequestOptions;
import com.azure.cosmos.models.CosmosDatabaseResponse;
import com.azure.cosmos.models.CosmosItemResponse;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.BaseTransactionConfig;
import org.janusgraph.diskstorage.PermanentBackendException;
import org.janusgraph.diskstorage.StaticBuffer;
import org.janusgraph.diskstorage.StoreMetaData.Container;
import org.janusgraph.diskstorage.common.DistributedStoreManager;
import org.janusgraph.diskstorage.configuration.Configuration;
import org.janusgraph.diskstorage.keycolumnvalue.KCVMutation;
import org.janusgraph.diskstorage.keycolumnvalue.KeyColumnValueStoreManager;
import org.janusgraph.diskstorage.keycolumnvalue.KeyRange;
import org.janusgraph.diskstorage.keycolumnvalue.StandardStoreFeatures;
import org.janusgraph.diskstorage.keycolumnvalue.StandardStoreFeatures.Builder;
import org.janusgraph.diskstorage.keycolumnvalue.StoreFeatures;
import org.janusgraph.diskstorage.keycolumnvalue.StoreTransaction;
import org.janusgraph.diskstorage.util.time.TimestampProviders;
import reactor.core.publisher.Mono;

/**
 * The JanusGraph manager for the Azure Cosmos DB Storage Backend for JanusGraph.
 */
@Slf4j
public class CosmosStoreManager extends DistributedStoreManager implements
    KeyColumnValueStoreManager {

  private static final int DEFAULT_PORT = 8081;
  CosmosAsyncClient client;
  private final CosmosStoreFactory factory;
  private final StoreFeatures features;
  private final String databaseName;
  private final String prefix;
  private CosmosAsyncDatabase database;

  private static int getPort(final Configuration config) throws BackendException {
    final String endpoint = JanusGraphConfigUtil.getNullableConfigValue(config,
        Constants.COSMOS_CLIENT_ENDPOINT);

    int port = DEFAULT_PORT;
    if (endpoint != null && !endpoint.equals(Constants.COSMOS_CLIENT_ENDPOINT.getDefaultValue())) {
      final URL url;
      try {
        url = new URL(endpoint);
      } catch (IOException e) {
        throw new PermanentBackendException("Unable to determine port from endpoint: " + endpoint);
      }
      port = url.getPort();
    }

    return port;
  }

  public CosmosStoreManager(final Configuration backendConfig) throws BackendException {
    super(backendConfig, getPort(backendConfig));
    try {
      client = new CosmosClientBuilder()
          .endpoint(backendConfig.get(Constants.COSMOS_CLIENT_ENDPOINT))
          // TODO
          .key(backendConfig.get(Constants.COSMOS_CLIENT_ENDPOINT))
          //.preferredRegions(preferredRegions)
          .contentResponseOnWriteEnabled(true)
          .consistencyLevel(ConsistencyLevel.SESSION)
          .buildAsyncClient();
    } catch (IllegalArgumentException e) {
      throw new PermanentBackendException("Bad configuration used: " + backendConfig, e);
    }
    databaseName = backendConfig.get(Constants.COSMOS_DATABASE);
    prefix = backendConfig.get(Constants.COSMOS_TABLE_PREFIX);
    factory = new TableNameCosmosStoreFactory();
    features = initializeFeatures(backendConfig);
    createDatabaseIfNotExists();
  }

  public CosmosAsyncDatabase getDatabase() {
    return database;
  }

  private void createDatabaseIfNotExists() {
    log.info("Create database " + databaseName + " if not exists.");

    // Create database if not exists
    Mono<CosmosDatabaseResponse> databaseIfNotExists = client.createDatabaseIfNotExists(
        databaseName);
    databaseIfNotExists.flatMap(databaseResponse -> {
      database = client.getDatabase(databaseResponse.getProperties().getId());
      log.info("Checking database " + database.getId() + " completed!\n");
      return Mono.empty();
    }).block();
  }

  public CosmosAsyncClient getClient() {
    return client;
  }

  @Override
  public StoreTransaction beginTransaction(final BaseTransactionConfig config)
      throws BackendException {
    final CosmosStoreTransaction txh = new CosmosStoreTransaction(config);
    return txh;
  }

  @Override
  public void clearStorage() throws BackendException {
    log.debug("Entering clearStorage");
    for (CosmosSingleRowStore store : factory.getAllStores()) {
      store.deleteStore();
    }
    client.getDatabase(databaseName).delete(new CosmosDatabaseRequestOptions()).block();
    log.debug("Exiting clearStorage returning:void");
  }

  @Override
  public boolean exists() throws BackendException {
    // TODO
    //return client.getDelegate().listTables(new ListTablesRequest()) != null;
    return false;
  }

  @Override
  public void close() throws BackendException {
    log.debug("Entering close");
    for (CosmosSingleRowStore store : factory.getAllStores()) {
      store.close();
    }
    client.close();
    log.debug("Exiting close returning:void");
  }

  @Override
  public String getName() {
    log.debug("Entering getName");
    final String name = getClass().getSimpleName() + prefix;
    log.debug("Exiting getName returning:{}", name);
    return name;
  }

  @Override
  public StoreFeatures getFeatures() {
    return features;
  }

  private StandardStoreFeatures initializeFeatures(final Configuration config) {
    final Builder builder = new Builder();
    return builder.batchMutation(true)
        .cellTTL(false)
        .distributed(true)
        .keyConsistent(config)
        .keyOrdered(false)
        .localKeyPartition(false)
        .locking(false)
        .multiQuery(true)
        .orderedScan(false)
        .preferredTimestamps(TimestampProviders.MILLI) //ignored because timestamps is false
        .storeTTL(false)
        .timestamps(false)
        .transactional(false)
        .supportsInterruption(false)
        .optimisticLocking(true)
        .unorderedScan(true)
        .visibility(false).build();
  }

  @Override
  public void mutateMany(final Map<String, Map<StaticBuffer, KCVMutation>> mutations,
      final StoreTransaction txh) throws BackendException {
    Mono.when(mutations.entrySet().stream()
        .flatMap(entry -> {
          try {
            final CosmosSingleRowStore store = openDatabase(entry.getKey());
            return store.mutateMany(entry.getValue(), txh);
          } catch (BackendException e) {
            throw new RuntimeException(e);
          }
        })
        .collect(Collectors.toList()))
      .block();
  }

  @Override
  public CosmosSingleRowStore openDatabase(@NonNull final String name) throws BackendException {
    Preconditions.checkArgument(!Strings.isNullOrEmpty(name),
        "database name may not be null or empty");
    return factory.create(this /*manager*/, prefix, name);
  }

  @Override
  public CosmosSingleRowStore openDatabase(@NonNull final String name, Container metaData)
      throws BackendException {
    Preconditions.checkArgument(!Strings.isNullOrEmpty(name),
        "database name may not be null or empty");
    return factory.create(this /*manager*/, prefix, name);
  }

  @Override
  public List<KeyRange> getLocalKeyPartition() throws BackendException {
    throw new UnsupportedOperationException();
  }

  @Override
  public Deployment getDeployment() {
    return Deployment.REMOTE;
  }
}
