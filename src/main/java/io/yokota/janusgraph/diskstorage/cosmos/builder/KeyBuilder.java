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
package io.yokota.janusgraph.diskstorage.cosmos.builder;

import com.fasterxml.jackson.databind.node.ObjectNode;
import org.janusgraph.diskstorage.StaticBuffer;

/**
 * KeyBuilder is responsible for extracting constant string attribute names from DynamoDB item
 * maps.
 */
public class KeyBuilder extends AbstractBuilder {

  private final ObjectNode item;

  public KeyBuilder(final ObjectNode item) {
    this.item = item;
  }

  public StaticBuffer build(final String name) {
    return decodeKey(item, name);
  }
}
