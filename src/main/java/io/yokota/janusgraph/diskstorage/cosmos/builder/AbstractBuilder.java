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
package io.yokota.janusgraph.diskstorage.cosmos.builder;

import com.fasterxml.jackson.databind.node.ObjectNode;
import java.nio.ByteBuffer;
import java.util.Arrays;
import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.codec.binary.Hex;
import org.janusgraph.diskstorage.StaticBuffer;
import org.janusgraph.diskstorage.util.StaticArrayBuffer;

/**
 * AbstractBuilder is responsible for some of the StaticBuffer to String and visa-versa required for
 * working with the database.
 *
 * @author Matthew Sowders
 * @author Alexander Patrikalakis
 */
public abstract class AbstractBuilder {

  public static String encodeKey(final StaticBuffer input) {
    if (input == null || input.length() == 0) {
      return null;
    }
    final ByteBuffer buf = input.asByteBuffer();
    final byte[] bytes = Arrays.copyOf(buf.array(), buf.limit());
    // use hex to maintain sort order
    return Hex.encodeHexString(bytes);
  }

  public static StaticBuffer decodeKey(final String input) {
    try {
      // use hex to maintain sort order
      return new StaticArrayBuffer(Hex.decodeHex(input));
    } catch (DecoderException e) {
      throw new RuntimeException(e);
    }
  }

  public static StaticBuffer decodeKey(final ObjectNode key, final String name) {
    if (null == key || !key.has(name)) {
      return null;
    }
    return decodeKey(key.get(name).textValue());
  }

  public static String encodeValue(final StaticBuffer input) {
    if (input == null || input.length() == 0) {
      return null;
    }
    final ByteBuffer buf = input.asByteBuffer();
    final byte[] bytes = Arrays.copyOf(buf.array(), buf.limit());
    return Base64.encodeBase64String(bytes);
  }

  public static StaticBuffer decodeValue(final String input) {
    return new StaticArrayBuffer(Base64.decodeBase64(input));
  }
}
