/*
 * Copyright 2014-2018 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * Portions copyright Titan: Distributed Graph Database - Copyright 2012 and onwards Aurelius.
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

import static org.janusgraph.diskstorage.configuration.ConfigOption.Type.FIXED;
import static org.janusgraph.diskstorage.configuration.ConfigOption.Type.LOCAL;

import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableList;
import java.time.Duration;
import java.util.List;
import org.janusgraph.diskstorage.Backend;
import org.janusgraph.diskstorage.configuration.ConfigNamespace;
import org.janusgraph.diskstorage.configuration.ConfigOption;
import org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration;

/**
 * Constants for the Cosmos DB backend.
 *
 * @author Matthew Sowders
 * @author Alexander Patrikalakis
 */
public final class Constants {

  private Constants() {
  }

  public static final String JANUSGRAPH_PARTITION_KEY = "pk";
  public static final String JANUSGRAPH_COLUMN_KEY = "id";
  public static final String JANUSGRAPH_VALUE = "v";
  public static final String HEX_PREFIX = "0x";

  public static final List<String> REQUIRED_BACKEND_STORES = ImmutableList.of(
      Backend.EDGESTORE_NAME,
      Backend.INDEXSTORE_NAME,
      Backend.SYSTEM_TX_LOG_NAME,
      Backend.SYSTEM_MGMT_LOG_NAME,
      GraphDatabaseConfiguration.SYSTEM_PROPERTIES_STORE_NAME);

  // Copied from com.azure.cosmos.DirectConnnectionConfig
  private static final Boolean DEFAULT_CONNECTION_ENDPOINT_REDISCOVERY_ENABLED = true;
  private static final Duration DEFAULT_IDLE_ENDPOINT_TIMEOUT = Duration.ofHours(1l);
  private static final Duration DEFAULT_CONNECT_TIMEOUT = Duration.ofSeconds(5L);
  private static final Duration DEFAULT_NETWORK_REQUEST_TIMEOUT = Duration.ofSeconds(5L);
  private static final Duration MIN_NETWORK_REQUEST_TIMEOUT = Duration.ofSeconds(1L);
  private static final Duration MAX_NETWORK_REQUEST_TIMEOUT = Duration.ofSeconds(10L);
  private static final int DEFAULT_MAX_CONNECTIONS_PER_ENDPOINT = 130;
  private static final int DEFAULT_MAX_REQUESTS_PER_CONNECTION = 30;
  private static final int DEFAULT_IO_THREAD_COUNT_PER_CORE_FACTOR = 2;
  private static final int DEFAULT_IO_THREAD_PRIORITY = Thread.NORM_PRIORITY;

  public static final ConfigNamespace COSMOS_CONFIGURATION_NAMESPACE =
      new ConfigNamespace(GraphDatabaseConfiguration.STORAGE_NS, "cosmos",
          "Cosmos DB storage options", false /*isUmbrella*/);
  public static final ConfigNamespace COSMOS_STORES_NAMESPACE =
      new ConfigNamespace(COSMOS_CONFIGURATION_NAMESPACE, "stores",
          "Cosmos DB KCV store options", true /*isUmbrella*/);
  public static final ConfigNamespace COSMOS_CLIENT_NAMESPACE =
      new ConfigNamespace(COSMOS_CONFIGURATION_NAMESPACE,
          "client", "Cosmos DB client options", false /*isUmbrella*/);
  public static final ConfigNamespace COSMOS_CLIENT_PROXY_NAMESPACE =
      new ConfigNamespace(COSMOS_CLIENT_NAMESPACE,
          "proxy", "Cosmos DB client proxy options", false /*isUmbrella*/);
  public static final ConfigNamespace COSMOS_CLIENT_SOCKET_NAMESPACE =
      new ConfigNamespace(COSMOS_CLIENT_NAMESPACE, "socket", "Cosmos DB client socket options",
          false /*isUmbrella*/);
  public static final ConfigNamespace COSMOS_CLIENT_EXECUTOR_NAMESPACE =
      new ConfigNamespace(COSMOS_CLIENT_NAMESPACE,
          "executor", "Cosmos DB client executor options", false /*isUmbrella*/);
  public static final ConfigNamespace COSMOS_CLIENT_CREDENTIALS_NAMESPACE =
      new ConfigNamespace(COSMOS_CLIENT_NAMESPACE, "credentials",
          "Cosmos DB client credentials options",
          false /*isUmbrella*/);

  public static final ConfigOption<String> COSMOS_DATABASE = new ConfigOption<>(
      COSMOS_CONFIGURATION_NAMESPACE,
      "database", "The name of the Cosmos DB database.  It will be created if it does not exist.",
      LOCAL, "janusgraph");
  public static final ConfigOption<String> COSMOS_TABLE_PREFIX = new ConfigOption<>(
      COSMOS_CONFIGURATION_NAMESPACE,
      "prefix", "A prefix to put before the JanusGraph table name. "
      + "This allows clients to have multiple graphs on the same AWS Cosmos DB account.",
      LOCAL, "jg");
  public static final ConfigOption<Boolean> COSMOS_ENABLE_PARALLEL_SCAN =
      new ConfigOption<>(COSMOS_CONFIGURATION_NAMESPACE, "enable-parallel-scans",
          "This feature enables scans to run in parallel, which should decrease the total blocking time "
              + "spent when iterating over large sets of vertices. "
              + "WARNING: while this feature is enabled JanusGraph's OLAP libraries are NOT supported."
              + "The JanusGraph-Hadoop implementations of OLAP rely on consistent scan orders across multiple scans, "
              + "which cannot be guaranteed when scans are run in parallel",
          LOCAL, false);
  public static final ConfigOption<String> STORES_DATA_MODEL =
      new ConfigOption<>(Constants.COSMOS_STORES_NAMESPACE, "data-model",
          "SINGLE Means that all the values for a given key are put into a single DynamoDB item. "
              + "A SINGLE is efficient because all the updates for a single key can be done atomically. "
              + "However, the trade-off is that DynamoDB has a 400k limit per item so it cannot hold much data. "
              + "MULTI Means that each 'column' is used as a range key in DynamoDB so a key can span multiple items. "
              + "A MULTI implementation is slightly less efficient than SINGLE because it must use DynamoDB Query "
              + "rather than a direct lookup. It is HIGHLY recommended to use MULTI for edgestore unless your graph has "
              + "very low max degree.",
          FIXED, BackendDataModel.MULTI.name());
  public static final ConfigOption<Boolean> COSMOS_USE_NATIVE_LOCKING = new ConfigOption<>(
      COSMOS_CONFIGURATION_NAMESPACE,
      "native-locking",
      "Set this to false if you need to use JanusGraph's locking mechanism for remote lock expiry.",
      FIXED, true);

  public static final ConfigOption<Boolean> COSMOS_FORCE_CONSISTENT_READ =
      new ConfigOption<>(COSMOS_CONFIGURATION_NAMESPACE, "force-consistent-read",
          "This feature sets the force consistent read property on Cosmos DB calls.",
          LOCAL, true);
  public static final ConfigOption<Long> STORES_INITIAL_CAPACITY_READ =
      new ConfigOption<>(Constants.COSMOS_STORES_NAMESPACE, "initial-capacity-read",
          "Define the initial read capacity for a given Cosmos DB table.",
          LOCAL, 4L);
  public static final ConfigOption<Long> STORES_INITIAL_CAPACITY_WRITE =
      new ConfigOption<>(Constants.COSMOS_STORES_NAMESPACE, "initial-capacity-write",
          "Define the initial write capacity for a given Cosmos DB table.",
          LOCAL, 4L);
  public static final ConfigOption<Double> STORES_READ_RATE_LIMIT =
      new ConfigOption<>(Constants.COSMOS_STORES_NAMESPACE, "read-rate",
          "The max number of reads per second.",
          LOCAL, 4.0);
  public static final ConfigOption<Double> STORES_WRITE_RATE_LIMIT =
      new ConfigOption<>(Constants.COSMOS_STORES_NAMESPACE, "write-rate",
          "Used to throttle write rate of given table. The max number of writes per second.",
          LOCAL, 4.0);
  public static final ConfigOption<Duration> COSMOS_CLIENT_CONN_TIMEOUT =
      new ConfigOption<>(COSMOS_CLIENT_NAMESPACE, "connection-timeout",
          "The amount of time to wait (in milliseconds) when initially establishing a connection before giving up and timing out.",
          //
          LOCAL, DEFAULT_CONNECT_TIMEOUT);
  public static final ConfigOption<Integer> COSMOS_CLIENT_MAX_CONN =
      new ConfigOption<>(COSMOS_CLIENT_NAMESPACE, "connection-max",
          "The maximum number of allowed open HTTP connections.",
          LOCAL, DEFAULT_MAX_CONNECTIONS_PER_ENDPOINT);
  public static final ConfigOption<Integer> COSMOS_CLIENT_MAX_ERROR_RETRY =
      new ConfigOption<>(COSMOS_CLIENT_NAMESPACE, "retry-error-max",
          "The maximum number of retry attempts for failed retryable "
              + "requests (ex: 5xx error responses from services).",
          LOCAL, 0);
  public static final ConfigOption<String> COSMOS_CLIENT_ENDPOINT =
      new ConfigOption<>(COSMOS_CLIENT_NAMESPACE, "endpoint",
          "Sets the service endpoint to use for connecting to Cosmos DB.",
          LOCAL, String.class);
  public static final ConfigOption<String> COSMOS_CLIENT_KEY =
      new ConfigOption<>(COSMOS_CLIENT_NAMESPACE, "key",
          "Sets the authentication key for connecting to Cosmos DB.",
          LOCAL, String.class);
  public static final ConfigOption<String> COSMOS_CLIENT_SIGNING_REGION =
      new ConfigOption<>(COSMOS_CLIENT_NAMESPACE, "signing-region",
          "Sets the signing region to use for signing requests to Cosmos DB. Required.",
          LOCAL, String.class);

  public static final ConfigOption<String> COSMOS_CLIENT_PROXY_DOMAIN =
      new ConfigOption<>(COSMOS_CLIENT_PROXY_NAMESPACE, "domain",
          "The optional Windows domain name for configuration an NTLM proxy.",
          LOCAL, "", Predicates.alwaysTrue());
  public static final ConfigOption<String> COSMOS_CLIENT_PROXY_WORKSTATION =
      new ConfigOption<>(COSMOS_CLIENT_PROXY_NAMESPACE, "workstation",
          "The optional Windows workstation name for configuring NTLM proxy support.",
          LOCAL, "", Predicates.alwaysTrue());
  public static final ConfigOption<String> COSMOS_CLIENT_PROXY_HOST =
      new ConfigOption<>(COSMOS_CLIENT_PROXY_NAMESPACE, "host",
          "The optional proxy host the client will connect through.",
          LOCAL, "", Predicates.alwaysTrue());
  public static final ConfigOption<Integer> COSMOS_CLIENT_PROXY_PORT =
      new ConfigOption<>(COSMOS_CLIENT_PROXY_NAMESPACE, "port",
          "The optional proxy port the client will connect through.",
          LOCAL, 0, Predicates.alwaysTrue());
  public static final ConfigOption<String> COSMOS_CLIENT_PROXY_USERNAME =
      new ConfigOption<>(COSMOS_CLIENT_PROXY_NAMESPACE, "username",
          "The optional proxy user name to use if connecting through a proxy.",
          LOCAL, "", Predicates.alwaysTrue());
  public static final ConfigOption<String> COSMOS_CLIENT_PROXY_PASSWORD =
      new ConfigOption<>(COSMOS_CLIENT_PROXY_NAMESPACE, "password",
          "The optional proxy password to use when connecting through a proxy.",
          LOCAL, "", Predicates.alwaysTrue());

  public static final ConfigOption<Integer> COSMOS_CLIENT_SOCKET_BUFFER_SEND_HINT =
      new ConfigOption<>(COSMOS_CLIENT_SOCKET_NAMESPACE, "buffer-send-hint",
          "The optional size hint (in bytes) for the low level TCP send buffer.",
          LOCAL, 1048576);
  public static final ConfigOption<Integer> COSMOS_CLIENT_SOCKET_BUFFER_RECV_HINT =
      new ConfigOption<>(COSMOS_CLIENT_SOCKET_NAMESPACE, "buffer-recv-hint",
          "The optional size hints (in bytes) for the low level TCP receive buffer.",
          LOCAL, 1048576);

  public static final ConfigOption<Integer> COSMOS_CLIENT_EXECUTOR_CORE_POOL_SIZE =
      new ConfigOption<>(COSMOS_CLIENT_EXECUTOR_NAMESPACE, "core-pool-size",
          "The core number of threads for the Cosmos DB client.",
          LOCAL, 25);
  public static final ConfigOption<Integer> COSMOS_CLIENT_EXECUTOR_MAX_POOL_SIZE =
      new ConfigOption<>(COSMOS_CLIENT_EXECUTOR_NAMESPACE, "max-pool-size",
          "The maximum allowed number of threads for the Cosmos DB client.",
          LOCAL, 50);
  public static final ConfigOption<Long> COSMOS_CLIENT_EXECUTOR_KEEP_ALIVE =
      new ConfigOption<>(COSMOS_CLIENT_EXECUTOR_NAMESPACE, "keep-alive",
          "The time limit for which threads may remain idle before being terminated for the Cosmos DB client.",
          LOCAL, 60000L);

  public static final ConfigOption<Integer> COSMOS_CLIENT_EXECUTOR_QUEUE_MAX_LENGTH =
      new ConfigOption<>(COSMOS_CLIENT_EXECUTOR_NAMESPACE, "max-queue-length",
          "The maximum size of the executor queue before requests start getting run in the caller.",
          LOCAL, 1024);
  public static final ConfigOption<Long> COSMOS_MAX_SELF_THROTTLED_RETRIES =
      new ConfigOption<>(COSMOS_CONFIGURATION_NAMESPACE, "max-self-throttled-retries",
          "The max number of retries to use when Cosmos DB throws temporary failure exceptions",
          LOCAL, 60L);
  public static final ConfigOption<Long> COSMOS_INITIAL_RETRY_MILLIS =
      new ConfigOption<>(COSMOS_CONFIGURATION_NAMESPACE, "initial-retry-millis",
          "The initial retry time (in milliseconds) to use during exponential backoff between Cosmos DB requests",
          LOCAL, 25L);
  public static final ConfigOption<Double> COSMOS_CONTROL_PLANE_RATE =
      new ConfigOption<>(COSMOS_CONFIGURATION_NAMESPACE, "control-plane-rate",
          "The maximum rate at which control plane requests (CreateTable, UpdateTable, DeleteTable, ListTables, "
              + "DescribeTable) are issued.",
          LOCAL, 10.0);

  public static final ConfigOption<Integer> COSMOS_CLIENT_EXECUTOR_MAX_CONCURRENT_OPERATIONS =
      new ConfigOption<>(COSMOS_CLIENT_EXECUTOR_NAMESPACE, "max-concurrent-operations",
          "The expected number of threads expected to be using a single TitanGraph instance. "
              + "Used to allocate threads to batch operations", //
          LOCAL, 1);

  public static final ConfigOption<String> COSMOS_CREDENTIALS_CLASS_NAME =
      new ConfigOption<>(COSMOS_CLIENT_CREDENTIALS_NAMESPACE, "class-name",
          "Specify the fully qualified class that implements AWSCredentialsProvider or AWSCredentials.",
          LOCAL, "com.amazonaws.auth.BasicAWSCredentials");
  public static final ConfigOption<String[]> COSMOS_CREDENTIALS_CONSTRUCTOR_ARGS =
      new ConfigOption<>(COSMOS_CLIENT_CREDENTIALS_NAMESPACE, "constructor-args",
          "Comma separated list of strings to pass to the credentials constructor.",
          LOCAL, new String[]{
          "accessKey", "secretKey"
      });
}
