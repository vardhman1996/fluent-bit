/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pulsar.client.schema;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;

import io.netty.buffer.Unpooled;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.impl.schema.ByteBufSchema;
import org.apache.pulsar.client.impl.schema.ByteBufferSchema;
import org.apache.pulsar.client.impl.schema.ByteSchema;
import org.apache.pulsar.client.impl.schema.BytesSchema;
import org.apache.pulsar.client.impl.schema.DoubleSchema;
import org.apache.pulsar.client.impl.schema.FloatSchema;
import org.apache.pulsar.client.impl.schema.IntSchema;
import org.apache.pulsar.client.impl.schema.LongSchema;
import org.apache.pulsar.client.impl.schema.ShortSchema;
import org.apache.pulsar.client.impl.schema.StringSchema;
import org.apache.pulsar.common.schema.SchemaType;
import org.testng.annotations.Test;

/**
 * Unit tests primitive schemas.
 */
@Slf4j
public class PrimitiveSchemaTest {

    final private Map<Schema, List<Object>> testData = new HashMap() {
        {
            put(StringSchema.utf8(), Arrays.asList("my string"));
            put(ByteSchema.of(), Arrays.asList((byte) 32767, (byte) -32768));
            put(ShortSchema.of(), Arrays.asList((short) 32767, (short) -32768));
            put(IntSchema.of(), Arrays.asList((int) 423412424, (int) -41243432));
            put(LongSchema.of(), Arrays.asList(922337203685477580L, -922337203685477581L));
            put(FloatSchema.of(), Arrays.asList(5678567.12312f, -5678567.12341f));
            put(DoubleSchema.of(), Arrays.asList(5678567.12312d, -5678567.12341d));
            put(BytesSchema.of(), Arrays.asList("my string".getBytes(UTF_8)));
            put(ByteBufferSchema.of(), Arrays.asList(ByteBuffer.allocate(10).put("my string".getBytes(UTF_8))));
            put(ByteBufSchema.of(), Arrays.asList(Unpooled.wrappedBuffer("my string".getBytes(UTF_8))));
        }
    };

    @Test
    public void allSchemasShouldSupportNull() {
        for (Schema<?> schema : testData.keySet()) {
            assertNull(schema.encode(null),
                "Should support null in " + schema.getSchemaInfo().getName() + " serialization");
            assertNull(schema.decode( null),
                "Should support null in " + schema.getSchemaInfo().getName() + " deserialization");
        }
    }

    @Test
    public void allSchemasShouldRoundtripInput() {
        for (Map.Entry<Schema, List<Object>> test : testData.entrySet()) {
            log.info("Test schema {}", test.getKey());
            for (Object value : test.getValue()) {
                log.info("Encode : {}", value);
                assertEquals(value,
                    test.getKey().decode(test.getKey().encode(value)),
                    "Should get the original " + test.getKey().getSchemaInfo().getName() +
                        " after serialization and deserialization");
            }
        }
    }

    @Test
    public void allSchemasShouldHaveSchemaType() {
        assertEquals(SchemaType.INT8, ByteSchema.of().getSchemaInfo().getType());
        assertEquals(SchemaType.INT16, ShortSchema.of().getSchemaInfo().getType());
        assertEquals(SchemaType.INT32, IntSchema.of().getSchemaInfo().getType());
        assertEquals(SchemaType.INT64, LongSchema.of().getSchemaInfo().getType());
        assertEquals(SchemaType.FLOAT, FloatSchema.of().getSchemaInfo().getType());
        assertEquals(SchemaType.DOUBLE, DoubleSchema.of().getSchemaInfo().getType());
        assertEquals(SchemaType.STRING, StringSchema.utf8().getSchemaInfo().getType());
        assertEquals(SchemaType.BYTES, BytesSchema.of().getSchemaInfo().getType());
        assertEquals(SchemaType.BYTES, ByteBufferSchema.of().getSchemaInfo().getType());
        assertEquals(SchemaType.BYTES, ByteBufSchema.of().getSchemaInfo().getType());
    }


}
