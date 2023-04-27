/*
 * Copyright 2014-2015 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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
package io.kcache.janusgraph.diskstorage.cosmos.iterator;

import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import org.janusgraph.diskstorage.Entry;
import org.janusgraph.diskstorage.StaticBuffer;
import org.janusgraph.diskstorage.keycolumnvalue.KeyIterator;
import org.janusgraph.diskstorage.util.RecordIterator;
import reactor.core.publisher.Flux;

/**
 * KeyIterator that is backed by a DynamoDB scan. This class is ignorant to the fact that
 * its backing scan might be running in parallel. The ScanContextInterpreter is expected to
 * be compatible with whatever scan order the scanner is using.
 *
 * @author Michael Rodaitis
 */
public class FluxBackedKeyIterator<T> implements KeyIterator {

    private final Flux<T> flux;
    private final FluxContextInterpreter<T> interpreter;

    private SingleKeyRecordIterator current;
    private Iterator<SingleKeyRecordIterator> recordIterators = Collections.emptyIterator();

    public FluxBackedKeyIterator(final Flux<T> flux, final FluxContextInterpreter<T> interpreter) {
        this.flux = flux;
        this.interpreter = interpreter;
        this.recordIterators = interpreter.buildRecordIterators(flux).iterator();
    }

    @Override
    public RecordIterator<Entry> getEntries() {
        return current.getRecordIterator();
    }

    @Override
    public void close() throws IOException {
        recordIterators = Collections.emptyIterator();
    }

    @Override
    public boolean hasNext() {
        return recordIterators.hasNext();
    }

    @Override
    public StaticBuffer next() {
        current = recordIterators.next();
        return current.getKey();
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException("remove");
    }
}
