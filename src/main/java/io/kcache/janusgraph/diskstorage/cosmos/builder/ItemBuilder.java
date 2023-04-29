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
package io.kcache.janusgraph.diskstorage.cosmos.builder;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.kcache.janusgraph.diskstorage.cosmos.Constants;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.janusgraph.diskstorage.StaticBuffer;

/**
 * ItemBuilder is responsible for translating from StaticBuffers to DynamoDB
 * item maps.
 *
 * @author Matthew Sowders
 *
 */
public class ItemBuilder extends AbstractBuilder {

    private static ObjectMapper MAPPER = new ObjectMapper();

    private final ObjectNode item = MAPPER.createObjectNode();

    public ItemBuilder partitionKey(final StaticBuffer key) {
        item.put(Constants.JANUSGRAPH_PARTITION_KEY, encodeKey(key));
        return this;
    }

    public ItemBuilder columnKey(final StaticBuffer key) {
        item.put(Constants.JANUSGRAPH_COLUMN_KEY, encodeKey(key));
        return this;
    }

    public ItemBuilder value(final StaticBuffer value) {
        item.put(Constants.JANUSGRAPH_VALUE, encodeValue(value));
        return this;
    }

    public ObjectNode build() {
        return item;
    }
}
