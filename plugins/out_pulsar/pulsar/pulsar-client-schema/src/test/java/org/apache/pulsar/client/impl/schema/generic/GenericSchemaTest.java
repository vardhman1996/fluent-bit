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
package org.apache.pulsar.client.impl.schema.generic;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.client.impl.schema.AutoConsumeSchema;
import org.apache.pulsar.client.schema.SchemaTestUtils.Bar;
import org.apache.pulsar.client.schema.SchemaTestUtils.Foo;
import org.testng.annotations.Test;

/**
 * Unit testing generic schemas.
 */
@Slf4j
public class GenericSchemaTest {

    @Test
    public void testGenericAvroSchema() {
        Schema<Foo> encodeSchema = Schema.AVRO(Foo.class);
        GenericSchema decodeSchema = GenericSchema.of(encodeSchema.getSchemaInfo());
        testEncodeAndDecodeGenericRecord(encodeSchema, decodeSchema);
    }

    @Test
    public void testGenericJsonSchema() {
        Schema<Foo> encodeSchema = Schema.JSON(Foo.class);
        GenericSchema decodeSchema = GenericSchema.of(encodeSchema.getSchemaInfo());
        testEncodeAndDecodeGenericRecord(encodeSchema, decodeSchema);
    }

    @Test
    public void testAutoAvroSchema() {
        Schema<Foo> encodeSchema = Schema.AVRO(Foo.class);
        AutoConsumeSchema decodeSchema = new AutoConsumeSchema();
        decodeSchema.setSchema(GenericSchema.of(encodeSchema.getSchemaInfo()));
        testEncodeAndDecodeGenericRecord(encodeSchema, decodeSchema);
    }

    @Test
    public void testAutoJsonSchema() {
        Schema<Foo> encodeSchema = Schema.JSON(Foo.class);
        AutoConsumeSchema decodeSchema = new AutoConsumeSchema();
        decodeSchema.setSchema(GenericSchema.of(encodeSchema.getSchemaInfo()));
        testEncodeAndDecodeGenericRecord(encodeSchema, decodeSchema);
    }

    public void testEncodeAndDecodeGenericRecord(Schema<Foo> encodeSchema,
                                                 Schema<GenericRecord> decodeSchema) {
        int numRecords = 10;
        for (int i = 0; i < numRecords; i++) {
            Foo foo = new Foo();
            foo.setField1("field-1-" + i);
            foo.setField2("field-2-" + i);
            foo.setField3(i);
            Bar bar = new Bar();
            bar.setField1(i % 2 == 0);
            foo.setField4(bar);

            byte[] data = encodeSchema.encode(foo);

            log.info("Decoding : {}", new String(data, UTF_8));

            GenericRecord record = decodeSchema.decode(data);
            Object field1 = record.getField("field1");
            assertEquals("field-1-" + i, field1, "Field 1 is " + field1.getClass());
            Object field2 = record.getField("field2");
            assertEquals("field-2-" + i, field2, "Field 2 is " + field2.getClass());
            Object field3 = record.getField("field3");
            assertEquals(i, field3, "Field 3 is " + field3.getClass());
            Object field4 = record.getField("field4");
            assertTrue(field4 instanceof GenericRecord);
            GenericRecord field4Record = (GenericRecord) field4;
            assertEquals(i % 2 == 0, field4Record.getField("field1"));
        }
    }

}
