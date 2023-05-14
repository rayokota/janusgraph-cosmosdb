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

import static org.janusgraph.diskstorage.configuration.ConfigOption.Type.FIXED;
import static org.janusgraph.diskstorage.configuration.ConfigOption.Type.LOCAL;

import com.azure.cosmos.ConnectionMode;
import com.azure.cosmos.ConsistencyLevel;
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
 */
public final class Constants {

  private Constants() {
  }

  public static final String JANUSGRAPH_PARTITION_KEY = "pk";
  public static final String JANUSGRAPH_COLUMN_KEY = "id";
  public static final String JANUSGRAPH_VALUE = "v";
  public static final String HEX_PREFIX = "0x";
  // TODO make configurable
  // The maximum size of a batch, per the Cosmos DB docs
  public static final int BATCH_SIZE_LIMIT = 10;
  // The maximum size of a patch, per the Cosmos DB docs
  public static final int PATCH_SIZE_LIMIT = 10;

  // Copied from com.azure.cosmos.DirectConnnectionConfig
  private static final Boolean DEFAULT_CONNECTION_ENDPOINT_REDISCOVERY_ENABLED = true;
  private static final Duration DEFAULT_IDLE_ENDPOINT_TIMEOUT = Duration.ofHours(1L);
  private static final Duration DEFAULT_CONNECT_TIMEOUT = Duration.ofSeconds(5L);
  private static final Duration DEFAULT_NETWORK_REQUEST_TIMEOUT = Duration.ofSeconds(5L);
  private static final Duration MIN_NETWORK_REQUEST_TIMEOUT = Duration.ofSeconds(1L);
  private static final Duration MAX_NETWORK_REQUEST_TIMEOUT = Duration.ofSeconds(10L);
  private static final int DEFAULT_MAX_CONNECTIONS_PER_ENDPOINT = 130;
  private static final int DEFAULT_MAX_REQUESTS_PER_CONNECTION = 30;
  private static final int DEFAULT_IO_THREAD_COUNT_PER_CORE_FACTOR = 2;
  private static final int DEFAULT_IO_THREAD_PRIORITY = Thread.NORM_PRIORITY;

  public static final ConfigNamespace COSMOS_CONFIGURATION_NAMESPACE =
      new ConfigNamespace(GraphDatabaseConfiguration.STORAGE_NS,
          "cosmos", "Cosmos DB storage options", false /*isUmbrella*/);
  public static final ConfigNamespace COSMOS_STORES_NAMESPACE =
      new ConfigNamespace(COSMOS_CONFIGURATION_NAMESPACE,
          "stores", "Cosmos DB KCV store options", true /*isUmbrella*/);
  public static final ConfigNamespace COSMOS_CLIENT_NAMESPACE =
      new ConfigNamespace(COSMOS_CONFIGURATION_NAMESPACE,
          "client", "Cosmos DB client options", false /*isUmbrella*/);
  public static final ConfigNamespace COSMOS_CLIENT_PROXY_NAMESPACE =
      new ConfigNamespace(COSMOS_CLIENT_NAMESPACE,
          "proxy", "Cosmos DB client proxy options", false /*isUmbrella*/);

  public static final ConfigOption<String> COSMOS_DATABASE = new ConfigOption<>(
      COSMOS_CONFIGURATION_NAMESPACE,
      "database", "The name of the Cosmos DB database.  It will be created if it does not exist.",
      LOCAL, "janusgraph");
  public static final ConfigOption<String> COSMOS_TABLE_PREFIX = new ConfigOption<>(
      COSMOS_CONFIGURATION_NAMESPACE,
      "prefix", "A prefix to put before the JanusGraph table name. "
      + "This allows clients to have multiple graphs on the same AWS Cosmos DB account.",
      LOCAL, "jg");
  public static final ConfigOption<String> STORES_DATA_MODEL_DEFAULT = new ConfigOption<>(
      COSMOS_CONFIGURATION_NAMESPACE,
      "data-model-default", "The default data model.",
      FIXED, BackendDataModel.MULTI.name());
  public static final ConfigOption<String> STORES_DATA_MODEL =
      new ConfigOption<>(COSMOS_STORES_NAMESPACE, "data-model",
          "SINGLE Means that all the values for a given key are put into a single Cosmos DB item. "
              + "A SINGLE is efficient because all the updates for a single key can be done atomically. "
              + "However, the trade-off is that Cosmos DB has a 400k limit per item so it cannot hold much data. "
              + "MULTI Means that each 'column' is used as a range key in Cosmos DB so a key can span multiple items. "
              + "A MULTI implementation is slightly less efficient than SINGLE because it must use Cosmos DB Query "
              + "rather than a direct lookup. It is HIGHLY recommended to use MULTI for edgestore unless your graph has "
              + "very low max degree.",
          FIXED, BackendDataModel.UNKNOWN.name());

  public static final ConfigOption<String> COSMOS_CLIENT_ENDPOINT =
      new ConfigOption<>(COSMOS_CLIENT_NAMESPACE, "endpoint",
          "Sets the service endpoint to use for connecting to Cosmos DB.",
          LOCAL, String.class);
  public static final ConfigOption<String> COSMOS_CLIENT_KEY =
      new ConfigOption<>(COSMOS_CLIENT_NAMESPACE, "key",
          "Sets the authentication key for connecting to Cosmos DB.",
          LOCAL, String.class);
  public static final ConfigOption<String> COSMOS_CLIENT_CONN_MODE =
      new ConfigOption<>(COSMOS_CLIENT_NAMESPACE, "connection-mode",
          "The connection mode, either DIRECT or GATEWAY.",
          LOCAL, ConnectionMode.DIRECT.name());
  public static final ConfigOption<Duration> COSMOS_CLIENT_CONN_TIMEOUT =
      new ConfigOption<>(COSMOS_CLIENT_NAMESPACE, "connection-timeout",
          "The amount of time to wait when initially establishing a connection before giving up and timing out.",
          LOCAL, DEFAULT_CONNECT_TIMEOUT);
  public static final ConfigOption<Integer> COSMOS_CLIENT_MAX_CONN =
      new ConfigOption<>(COSMOS_CLIENT_NAMESPACE, "connection-max",
          "The maximum number connections per endpoint.",
          LOCAL, DEFAULT_MAX_CONNECTIONS_PER_ENDPOINT);
  public static final ConfigOption<Integer> COSMOS_CLIENT_MAX_REQUESTS =
      new ConfigOption<>(COSMOS_CLIENT_NAMESPACE, "request-max",
          "The maximum number requests per connection.",
          LOCAL, DEFAULT_MAX_REQUESTS_PER_CONNECTION);
  public static final ConfigOption<Duration> COSMOS_CLIENT_REQUEST_TIMEOUT =
      new ConfigOption<>(COSMOS_CLIENT_NAMESPACE, "request-timeout",
          "The amount of time to wait for a response.",
          LOCAL, DEFAULT_NETWORK_REQUEST_TIMEOUT);
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
  public static final ConfigOption<Integer> COSMOS_CLIENT_PROXY_MAX_POOL_SIZE =
      new ConfigOption<>(COSMOS_CLIENT_PROXY_NAMESPACE, "max-pool-size",
          "The maximum allowed number of threads for the Cosmos DB client.",
          LOCAL, 1000);
  public static final ConfigOption<String> COSMOS_CONSISTENCY_LEVEL =
      new ConfigOption<>(COSMOS_CLIENT_NAMESPACE, "consistency-level",
          "This feature sets the consistency level on Cosmos DB calls.",
          LOCAL, ConsistencyLevel.SESSION.name());
  public static final ConfigOption<Boolean> COSMOS_CONN_SHARING_ACROSS_CLIENTS =
      new ConfigOption<>(COSMOS_CLIENT_NAMESPACE, "connection-sharing-across-clients",
          "Whether connections are shared across clients.",
          LOCAL, false);
  public static final ConfigOption<Boolean> COSMOS_CONTENT_RESPONSE_ON_WRITE =
      new ConfigOption<>(COSMOS_CLIENT_NAMESPACE, "content-response-on-write",
          "Whether to only return header and status code for writes.",
          LOCAL, false);
  public static final ConfigOption<String> COSMOS_USER_AGENT_SUFFIX =
      new ConfigOption<>(COSMOS_CLIENT_NAMESPACE, "user-agent-suffix",
          "The value to be appended to the user-agent header.",
          LOCAL, null);
  public static final ConfigOption<String[]> COSMOS_PREFERRED_REGIONS =
      new ConfigOption<>(COSMOS_CLIENT_NAMESPACE, "preferred-regions",
          "The preferred regions for geo-replicated accounts.",
          LOCAL, new String[]{});
}
