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

import static io.yokota.janusgraph.diskstorage.cosmos.Constants.COSMOS_CLIENT_CONN_MODE;
import static io.yokota.janusgraph.diskstorage.cosmos.Constants.COSMOS_CLIENT_CONN_TIMEOUT;
import static io.yokota.janusgraph.diskstorage.cosmos.Constants.COSMOS_CLIENT_ENDPOINT;
import static io.yokota.janusgraph.diskstorage.cosmos.Constants.COSMOS_CLIENT_KEY;
import static io.yokota.janusgraph.diskstorage.cosmos.Constants.COSMOS_CLIENT_MAX_CONN;
import static io.yokota.janusgraph.diskstorage.cosmos.Constants.COSMOS_CLIENT_MAX_REQUESTS;
import static io.yokota.janusgraph.diskstorage.cosmos.Constants.COSMOS_CLIENT_PROXY_HOST;
import static io.yokota.janusgraph.diskstorage.cosmos.Constants.COSMOS_CLIENT_PROXY_MAX_POOL_SIZE;
import static io.yokota.janusgraph.diskstorage.cosmos.Constants.COSMOS_CLIENT_PROXY_PASSWORD;
import static io.yokota.janusgraph.diskstorage.cosmos.Constants.COSMOS_CLIENT_PROXY_PORT;
import static io.yokota.janusgraph.diskstorage.cosmos.Constants.COSMOS_CLIENT_PROXY_USERNAME;
import static io.yokota.janusgraph.diskstorage.cosmos.Constants.COSMOS_CLIENT_REQUEST_TIMEOUT;
import static io.yokota.janusgraph.diskstorage.cosmos.Constants.COSMOS_CONN_SHARING_ACROSS_CLIENTS;
import static io.yokota.janusgraph.diskstorage.cosmos.Constants.COSMOS_CONSISTENCY_LEVEL;
import static io.yokota.janusgraph.diskstorage.cosmos.Constants.COSMOS_CONTENT_RESPONSE_ON_WRITE;
import static io.yokota.janusgraph.diskstorage.cosmos.Constants.COSMOS_DATABASE;
import static io.yokota.janusgraph.diskstorage.cosmos.Constants.COSMOS_PREFERRED_REGIONS;
import static io.yokota.janusgraph.diskstorage.cosmos.Constants.COSMOS_TABLE_PREFIX;
import static io.yokota.janusgraph.diskstorage.cosmos.Constants.COSMOS_USER_AGENT_SUFFIX;

import com.azure.core.http.ProxyOptions;
import com.azure.core.http.ProxyOptions.Type;
import com.azure.cosmos.ConnectionMode;
import com.azure.cosmos.ConsistencyLevel;
import com.azure.cosmos.CosmosAsyncClient;
import com.azure.cosmos.CosmosAsyncDatabase;
import com.azure.cosmos.CosmosClientBuilder;
import com.azure.cosmos.DirectConnectionConfig;
import com.azure.cosmos.GatewayConnectionConfig;
import com.azure.cosmos.models.CosmosDatabaseProperties;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URL;
import java.util.Arrays;
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
        COSMOS_CLIENT_ENDPOINT);

    int port = DEFAULT_PORT;
    if (endpoint != null && !endpoint.equals(COSMOS_CLIENT_ENDPOINT.getDefaultValue())) {
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

  public CosmosStoreManager(final Configuration config) throws BackendException {
    super(config, getPort(config));
    try {
      ConsistencyLevel consistencyLevel = ConsistencyLevel.valueOf(
          config.get(COSMOS_CONSISTENCY_LEVEL));
      String userAgentSuffix = config.get(COSMOS_USER_AGENT_SUFFIX);
      String[] preferredRegions = config.get(COSMOS_PREFERRED_REGIONS);
      CosmosClientBuilder builder = new CosmosClientBuilder()
          .endpoint(config.get(COSMOS_CLIENT_ENDPOINT))
          .key(config.get(COSMOS_CLIENT_KEY))
          .consistencyLevel(consistencyLevel)
          .connectionSharingAcrossClientsEnabled(config.get(COSMOS_CONN_SHARING_ACROSS_CLIENTS))
          .contentResponseOnWriteEnabled(config.get(COSMOS_CONTENT_RESPONSE_ON_WRITE));
      ConnectionMode connectionMode = ConnectionMode.valueOf(
          config.get(COSMOS_CLIENT_CONN_MODE));
      switch (connectionMode) {
        case DIRECT:
          DirectConnectionConfig directConfig = DirectConnectionConfig.getDefaultConfig();
          directConfig.setConnectTimeout(config.get(COSMOS_CLIENT_CONN_TIMEOUT));
          directConfig.setMaxConnectionsPerEndpoint(config.get(COSMOS_CLIENT_MAX_CONN));
          directConfig.setMaxRequestsPerConnection(config.get(COSMOS_CLIENT_MAX_REQUESTS));
          directConfig.setNetworkRequestTimeout(config.get(COSMOS_CLIENT_REQUEST_TIMEOUT));
          builder.directMode(directConfig);
          break;
        case GATEWAY:
          GatewayConnectionConfig gatewayConfig = GatewayConnectionConfig.getDefaultConfig();
          InetSocketAddress address = new InetSocketAddress(
              config.get(COSMOS_CLIENT_PROXY_HOST), config.get(COSMOS_CLIENT_PROXY_PORT));
          ProxyOptions proxyOptions = new ProxyOptions(Type.HTTP, address);
          proxyOptions.setCredentials(
              config.get(COSMOS_CLIENT_PROXY_USERNAME), config.get(COSMOS_CLIENT_PROXY_PASSWORD));
          gatewayConfig.setProxy(proxyOptions);
          gatewayConfig.setMaxConnectionPoolSize(config.get(COSMOS_CLIENT_PROXY_MAX_POOL_SIZE));
          builder.gatewayMode(gatewayConfig);
          break;
        default:
          throw new IllegalArgumentException();
      }
      if (userAgentSuffix != null) {
        builder.userAgentSuffix(userAgentSuffix);
      }
      if (preferredRegions.length > 0) {
        builder.preferredRegions(Arrays.asList(preferredRegions));
      }
      client= builder.buildAsyncClient();
    } catch (IllegalArgumentException e) {
      throw new PermanentBackendException("Bad configuration used: " + config, e);
    }
    databaseName = config.get(COSMOS_DATABASE);
    prefix = config.get(COSMOS_TABLE_PREFIX);
    factory = new ContainerNameCosmosStoreFactory(config);
    features = initializeFeatures(config);
    createDatabaseIfNotExists();
    while (!exists()) {
      createDatabaseIfNotExists();
    }
  }

  public CosmosAsyncDatabase getDatabase() {
    return database;
  }

  private void createDatabaseIfNotExists() {
    log.info("Create database {} if not exists.", databaseName);

    //  Create database if not exists
    client.createDatabaseIfNotExists(databaseName)
        .onErrorResume(exception -> {
          log.warn("Could not create database", exception);
          return Mono.empty();
        })
        .block();
    database = client.getDatabase(databaseName);
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
    // TODO remove
    /*
    client.getDatabase(databaseName).delete(new CosmosDatabaseRequestOptions())
        .onErrorResume(exception -> {
          log.warn("Could not delete database:{}", databaseName, exception);
          return Mono.empty();
        })
        .block();
     */
    log.debug("<== clearStorage returning:void");
  }

  @Override
  public boolean exists() throws BackendException {
    List<CosmosDatabaseProperties> result = client.queryDatabases(
        String.format("SELECT * FROM root r where r.id = '%s'", databaseName) , null)
        .collectList()
        .onErrorResume(exception -> {
          log.warn("Could not query database:{}", databaseName, exception);
          return Mono.empty();
        })
        .block();
    return result != null && !result.isEmpty();
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
