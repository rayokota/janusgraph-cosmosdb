package io.kcache.janusgraph.diskstorage.cosmos;

import org.janusgraph.diskstorage.BaseTransactionConfig;
import org.janusgraph.diskstorage.common.AbstractStoreTransaction;

public class CosmosTx extends AbstractStoreTransaction {

    public CosmosTx(BaseTransactionConfig config) {
        super(config);
    }
}
