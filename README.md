# Azure Cosmos DB Storage Adapter for JanusGraph

[![Build Status][github-actions-shield]][github-actions-link]
[![Maven][maven-shield]][maven-link]
[![Javadoc][javadoc-shield]][javadoc-link]

[github-actions-shield]: https://github.com/rayokota/janusgraph-cosmosdb/workflows/build/badge.svg?branch=master
[github-actions-link]: https://github.com/rayokota/janusgraph-cosmosdb/actions
[maven-shield]: https://img.shields.io/maven-central/v/io.yokota/janusgraph-cosmosdb.svg
[maven-link]: https://search.maven.org/#search%7Cga%7C1%7Cio.yokota
[javadoc-shield]: https://javadoc.io/badge/io.yokota/janusgraph-cosmosdb.svg?color=blue
[javadoc-link]: https://javadoc.io/doc/io.yokota/janusgraph-cosmosdb

[JanusGraph](http://janusgraph.org) is an [Apache TinkerPop](http://tinkerpop.apache.org) enabled graph database that supports a variety of storage and indexing backends. This project adds [Azure Cosmos DB](https://azure.microsoft.com/en-us/products/cosmos-db) to the supported list of backends.

## Getting started

### Installing the adapter from a binary release
Binary releases can be found on [GitHub](http://github.com/rayokota/janusgraph-cosmosdb/releases).

This installation procedure will copy the necessary libraries, properties, and Gremlin Server configuration files into your JanusGraph installation.

1. Download the JanusGraph [release](https://github.com/JanusGraph/janusgraph/releases).
2. Download the Cosmos DB Storage Backend for JanusGraph release.
3. Unzip the storage adapter zip file and run `./install.sh $YOUR_JANUSGRAPH_INSTALL_DIRECTORY`

You can connect from the Gremlin console by running:

`gremlin> graph = JanusGraphFactory.open('conf/janusgraph-cosmosdb.properties')`

To start Gremlin Server run `gremlin-server.sh` directly or `bin/janusgraph.sh start` which will also start a local Elasticsearch instance.

### Installing from source

Follow these steps if you'd like to use the latest version built from source.
1. Clone the repository.
    `git clone http://github.com/rayokota/janusgraph-cosmosdb`
2. Build the distribution package.
    `mvn package -DskipTests`
3. Follow the binary installation steps starting at step 3.

## Data Model
The Azure Cosmos DB Storage Backend for JanusGraph has a flexible data model
(adapted from the 
[Amazon DynamoDB Storage Backend for JanusGraph](https://github.com/amazon-archives/dynamodb-janusgraph-storage-backend))
that allows clients to select the data model for each JanusGraph backend table. Clients
can configure tables to use either a single-item model or a multiple-item model.

### Single-Item Model
The single-item model uses a single Cosmos DB item to store all values for a
single key.  In terms of JanusGraph backend implementations, the key becomes the
Cosmos DB partition key, and each column becomes a property name and the column
value is stored in the respective property value.

This is definitely the most efficient implementation, but beware of the 2mb
limit Cosmos DB imposes on items. It is best to only use this on tables you are
sure will not surpass the item size limit. Graphs with low vertex degree and
low number of items per index can take advantage of this implementation.

### Multiple-Item Model
The multiple-item model uses multiple Cosmos DB items to store all values for a
single key.  In terms of JanusGraph backend implementations, the key becomes the
Cosmos DB partition key, and each column becomes the id in its own item.
The column values are stored in its own property.

The multiple-item model is less efficient than the single-item during initial
graph loads, but it gets around the 2mb limit. The multiple-item model
uses QueryItems calls instead of ReadItem calls to get the necessary column
values.

## Cosmos DB Specific Configuration
Each configuration option has a certain mutability level that governs whether
and how it can be modified after the database is opened for the first time. The
following listing describes the mutability levels.

1. **FIXED** - Once the database has been opened, these configuration options
   cannot be changed for the entire life of the database
2. **GLOBAL_OFFLINE** - These options can only be changed for the entire
   database cluster at once when all instances are shut down
3. **GLOBAL** - These options can only be changed globally across the entire
   database cluster
4. **MASKABLE** - These options are global but can be overwritten by a local
   configuration file
5. **LOCAL** - These options can only be provided through a local configuration
   file

Leading namespace names are shortened and sometimes spaces were inserted in long
strings to make sure the tables below are formatted correctly.

### General Cosmos DB Configuration Parameters
All of the following parameters are in the `storage` (`s`) namespace, and most
are in the `storage.cosmos` (`s.c`) namespace subset.

| Name                             | Description                                                                                                                                                                                                                                                   | Datatype | Default Value | Mutability |
|----------------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|----------|---------------|------------|
| `s.backend`                      | The primary persistence provider used by JanusGraph. To use Cosmos DB you must set this to `io.yokota.janusgraph.diskstorage.cosmos.CosmosStoreManager`                                                                                                       | String   |               | LOCAL      |
| `s.c.database`                   | The name of the Cosmos DB database.  It will be created if it does not exist.                                                                                                                                                                                 | String   | janusgraph    | LOCAL      |
| `s.c.prefix`                     | A prefix to put before the JanusGraph table name. This allows clients to have multiple graphs in the same AWS Cosmos DB account in the same region.                                                                                                           | String   | jg            | LOCAL      |
| `s.c.data-model-default`         | The default data model.                                                                                                                                                                                                                                       | String   | MULTIPLE      | FIXED      |

### Cosmos DB KeyColumnValue Store Configuration Parameters
Some configurations require specifications for each of the JanusGraph backend
Key-Column-Value stores. Here is a list of the default JanusGraph backend
Key-Column-Value stores:
* edgestore
* graphindex
* janusgraph_ids
* system_properties
* systemlog
* txlog

In addition, the three following stores are used to support locking:
* edgestore_lock_
* graphindex_lock_
* system_properties_lock_

All of these configuration parameters are in the `storage.cosmos.stores`
(`s.c.s`) umbrella namespace subset. In the tables below these configurations
have the text `t` where the JanusGraph store name should go.

| Name                             | Description                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 | Datatype | Default Value | Mutability |
|----------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|----------|---------------|------------|
| `s.c.s.t.data-model`             | SINGLE means that all the values for a given key are put into a single Cosmos DB item.  A SINGLE is efficient because all the updates for a single key can be done atomically. However, the tradeoff is that Cosmos DB has a 2mb limit per item so it cannot hold much data. MULTI means that each 'column' is used as a range key in Cosmos DB so a key can span multiple items. A MULTI implementation is slightly less efficient than SINGLE because it must use Cosmos DB Query rather than a direct lookup. It is HIGHLY recommended to use MULTI for edgestore and graphindex unless your graph has very low max degree. UNKNOWN means to use the default data model. | String   | UNKNOWN       | FIXED |
| `s.c.s.t.throughput`             | Sets the request units for the Cosmos DB table/container.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   | Integer  | 10000         | LOCAL |

### Cosmos DB Client Configuration Parameters
All of these configuration parameters are in the `storage.cosmos.client`
(`s.c.c`) namespace subset, and are related to the Cosmos DB SDK client
configuration.

| Name                                      | Description                                                                                          | Datatype | Default Value | Mutability |
|-------------------------------------------|------------------------------------------------------------------------------------------------------|----------|---------------|------------|
| `s.c.c.endpoint`                          | Sets the service endpoint to use for connecting to Cosmos DB.                                        | String   |               | LOCAL |
| `s.c.c.key`                               | Sets the authentication key for connecting to Cosmos DB.                                             | String   |               | LOCAL |
| `s.c.c.connection-mode`                   | The connection mode, either DIRECT or GATEWAY.                                                       | String   | DIRECT        | LOCAL |
| `s.c.c.connection-timeout`                | The amount of time to wait when initially establishing a connection before giving up and timing out. | Duration | 5 seconds     | LOCAL |
| `s.c.c.connection-max`                    | The maximum number of connections per endpoint.                                                      | Integer  | 130           | LOCAL |
| `s.c.c.request-max`                       | The maximum number of requests per connection.                                                       | Integer  | 30            | LOCAL |
| `s.c.c.request-timeout`                   | The amount of time to wait for a response.                                                           | Duration | 5 seconds     | LOCAL |
| `s.c.c.consistency-level`                 | This feature sets the consistency level on Cosmos DB calls.                                          | String   | SESSION       | LOCAL |
| `s.c.c.connection-sharing-across-clients` | Whether connections are shared across clients.                                                       | Boolean  | false         | LOCAL |
| `s.c.c.content-response-on-write`         | Whether to only return header and status codes for writes.                                           | Boolean  | false         | LOCAL |
| `s.c.c.user-agent-suffix`                 | The value to be appended to the user-agent header.                                                   | String   | false         | LOCAL |
| `s.c.c.preferred-regions`                 | The preferred regions for geo-replicated accounts.                                                   | String[] |               | LOCAL |
| `s.c.c.batch-size`                        | The size for batch requests.  Must be between 1 and 100.                                             | Integer  | 100           | LOCAL |
| `s.c.c.patch-size`                        | The size for patch requests.  Must be between 1 and 10.                                              | Integer  | 10            | LOCAL |

### Cosmos DB Client Proxy Configuration Parameters
All of these configuration parameters are in the
`storage.cosmos.client.proxy` (`s.c.c.p`) namespace subset, and are related to
the Cosmos DB SDK client proxy configuration.

| Name                    | Description                                                         | Datatype | Default Value | Mutability |
|-------------------------|---------------------------------------------------------------------|----------|---------------|------------|
| `s.c.c.p.host`          | The optional proxy host the client will connect through.            | String   |               | LOCAL |
| `s.c.c.p.port`          | The optional proxy port the client will connect through.            | String   |               | LOCAL |
| `s.c.c.p.username`      | The optional proxy user name to use if connecting through a proxy.  | String   |               | LOCAL |
| `s.c.c.p.password`      | The optional proxy password to use when connecting through a proxy. | String   |               | LOCAL |
| `s.c.c.p.max-pool-size` | The maximum allowed number of threads for the Cosmos DB client.     | Integer  | 1000          | LOCAL |
