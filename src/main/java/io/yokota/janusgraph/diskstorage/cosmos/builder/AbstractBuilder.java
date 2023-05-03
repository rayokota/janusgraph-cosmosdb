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
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.codec.binary.Hex;
import org.janusgraph.diskstorage.StaticBuffer;
import org.janusgraph.diskstorage.util.BufferUtil;
import org.janusgraph.diskstorage.util.StaticArrayBuffer;
import org.janusgraph.diskstorage.util.time.TimestampProviders;
import org.janusgraph.graphdb.database.serialize.DataOutput;
import org.janusgraph.graphdb.database.serialize.StandardSerializer;

/**
 * AbstractBuilder is responsible for some of the StaticBuffer to String and visa-versa required for
 * working with the database.
 */
public abstract class AbstractBuilder {

  public static char BASE64_PREFIX = 's';
  public static char HEX_PREFIX = 'x';

  public static String encodeKey(final StaticBuffer input) {
    if (input == null || input.length() == 0) {
      return null;
    }
    final ByteBuffer buf = input.asByteBuffer();
    final byte[] bytes = new byte[buf.remaining()];
    buf.get(bytes);
    // Use hex to maintain sort order, prepend an 'x' as otherwise
    // if the name is numeric, then Cosmos treats the name as an array index in patch operations,
    // with errors like "Token('0000000000000400') has length longer than expected".
    return HEX_PREFIX + Hex.encodeHexString(bytes);
  }

  public static StaticBuffer decodeKey(final String input) {
    try {
      // Use hex to maintain sort order, strip the leading 'x'
      return new StaticArrayBuffer(Hex.decodeHex(input.substring(1)));
    } catch (DecoderException e) {
      throw new RuntimeException(e);
    }
  }

  // TODO remove
  public static void main(String[] args) {
    String s = "ai there";
    String s2 = "ii there";
    String s3 = "ji there";
    String s4 = "ki there";
    String s5 = "zi there";
    String k = encodeKey(new StaticArrayBuffer(s.getBytes(StandardCharsets.UTF_8)));
    String k2 = encodeKey(new StaticArrayBuffer(s2.getBytes(StandardCharsets.UTF_8)));
    String k3 = encodeKey(new StaticArrayBuffer(s3.getBytes(StandardCharsets.UTF_8)));
    String k4 = encodeKey(new StaticArrayBuffer(s4.getBytes(StandardCharsets.UTF_8)));
    String k5 = encodeKey(new StaticArrayBuffer(s5.getBytes(StandardCharsets.UTF_8)));
    System.out.println(k.compareTo(k2));
    System.out.println(k2.compareTo(k3));
    System.out.println(k3.compareTo(k4));
    System.out.println(k4.compareTo(k5));

    Object x = TimestampProviders.MICRO;

    StaticBuffer sb1 = object2StaticBuffer(x);
    System.out.println("sb1 " + sb1);
    String v1 = AbstractBuilder.encodeValue(sb1);
    System.out.println("v1 " + v1);
    StaticBuffer sb2 = AbstractBuilder.decodeValue(v1);
    System.out.println("sb2 " + sb2);
    Object y = staticBuffer2Object(sb2, TimestampProviders.class);

    System.out.println("*** hi " + y);


  }

  private static <O> StaticBuffer object2StaticBuffer(final O value) {
    StandardSerializer serializer = new StandardSerializer();
    DataOutput out = serializer.getDataOutput(128);
    out.writeClassAndObject(value);
    return out.getStaticBuffer();
  }

  private static <O> O staticBuffer2Object(final StaticBuffer s, Class<O> dataType) {
    StandardSerializer serializer = new StandardSerializer();
    Object value = serializer.readClassAndObject(s.asReadBuffer());
    return (O) value;
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
    final byte[] bytes = new byte[buf.remaining()];
    buf.get(bytes);
    return BASE64_PREFIX + Base64.encodeBase64String(bytes);
  }

  public static StaticBuffer decodeValue(final String input) {
    if (input == null) {
      return BufferUtil.emptyBuffer();
    }
    return new StaticArrayBuffer(Base64.decodeBase64(input.substring(1)));
  }
}
