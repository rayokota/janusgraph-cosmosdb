package io.kcache.janusgraph.diskstorage.cosmos;

import org.janusgraph.diskstorage.BaseTransactionConfig;
import org.janusgraph.diskstorage.common.AbstractStoreTransaction;

public class CosmosStoreTransaction extends AbstractStoreTransaction {

  public CosmosStoreTransaction(BaseTransactionConfig config) {
    super(config);
  }
}
