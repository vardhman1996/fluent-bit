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

import java.util.List;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.util.Utf8;
import org.apache.pulsar.client.api.schema.Field;
import org.apache.pulsar.client.api.schema.GenericRecord;

/**
 * A generic avro record.
 */
@Slf4j
class GenericAvroRecord implements GenericRecord {

    private final org.apache.avro.Schema schema;
    private final List<Field> fields;
    private final org.apache.avro.generic.GenericRecord record;

    GenericAvroRecord(org.apache.avro.Schema schema,
                      List<Field> fields,
                      org.apache.avro.generic.GenericRecord record) {
        this.schema = schema;
        this.fields = fields;
        this.record = record;
    }

    @Override
    public List<Field> getFields() {
        return fields;
    }

    @Override
    public Object getField(String fieldName) {
        Object value = record.get(fieldName);
        if (value instanceof Utf8) {
            return ((Utf8) value).toString();
        } else if (value instanceof org.apache.avro.generic.GenericRecord) {
            org.apache.avro.generic.GenericRecord avroRecord =
                (org.apache.avro.generic.GenericRecord) value;
            org.apache.avro.Schema recordSchema = avroRecord.getSchema();
            List<Field> fields = recordSchema.getFields()
                .stream()
                .map(f -> new Field(f.name(), f.pos()))
                .collect(Collectors.toList());
            return new GenericAvroRecord(schema, fields, avroRecord);
        } else {
            return value;
        }
    }

    org.apache.avro.generic.GenericRecord getAvroRecord() {
        return record;
    }

}
