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

import com.azure.cosmos.ConsistencyLevel;
import com.azure.cosmos.CosmosAsyncClient;
import com.azure.cosmos.CosmosAsyncDatabase;
import com.azure.cosmos.CosmosClientBuilder;
import com.azure.cosmos.models.CosmosDatabaseRequestOptions;
import com.azure.cosmos.models.CosmosDatabaseResponse;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import java.io.IOException;
import java.net.URL;
import java.util.List;
import java.util.Map;
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
          .key(backendConfig.get(Constants.COSMOS_CLIENT_KEY))
          //.preferredRegions(preferredRegions)
          .contentResponseOnWriteEnabled(true)
          .consistencyLevel(ConsistencyLevel.SESSION)
          .buildAsyncClient();
    } catch (IllegalArgumentException e) {
      throw new PermanentBackendException("Bad configuration used: " + backendConfig, e);
    }
    databaseName = backendConfig.get(Constants.COSMOS_DATABASE);
    prefix = backendConfig.get(Constants.COSMOS_TABLE_PREFIX);
    factory = new ContainerNameCosmosStoreFactory(backendConfig);
    features = initializeFeatures(backendConfig);
    createDatabaseIfNotExists();
  }

  public CosmosAsyncDatabase getDatabase() {
    return database;
  }

  private void createDatabaseIfNotExists() {
    log.info("Create database {} if not exists.", databaseName);

    //  Create database if not exists
    CosmosDatabaseResponse databaseResponse = client.createDatabaseIfNotExists(databaseName).block();
    database = client.getDatabase(databaseResponse.getProperties().getId());
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
    log.debug("==> clearStorage");
    for (CosmosKeyColumnValueStore store : factory.getAllStores()) {
      store.deleteStore();
    }
    client.getDatabase(databaseName).delete(new CosmosDatabaseRequestOptions());
    log.debug("<== clearStorage returning:void");
  }

  @Override
  public boolean exists() throws BackendException {
    return client.readAllDatabases() != null;
  }

  @Override
  public void close() throws BackendException {
    log.debug("==> close");
    for (CosmosKeyColumnValueStore store : factory.getAllStores()) {
      store.close();
    }
    client.close();
    log.debug("<== close returning:void");
  }

  @Override
  public String getName() {
    log.debug("==> getName");
    final String name = getClass().getSimpleName() + prefix;
    log.debug("<== getName returning:{}", name);
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
    mutations.forEach((k, v) -> {
      try {
        final CosmosKeyColumnValueStore store = openDatabase(k);
        store.mutateMany(v, txh);
      } catch (BackendException e) {
        throw new RuntimeException(e);
      }
    });
  }

  @Override
  public CosmosKeyColumnValueStore openDatabase(@NonNull final String name)
      throws BackendException {
    Preconditions.checkArgument(!Strings.isNullOrEmpty(name),
        "database name may not be null or empty");
    return factory.create(this /*manager*/, prefix, name);
  }

  @Override
  public CosmosKeyColumnValueStore openDatabase(@NonNull final String name, Container metaData)
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
