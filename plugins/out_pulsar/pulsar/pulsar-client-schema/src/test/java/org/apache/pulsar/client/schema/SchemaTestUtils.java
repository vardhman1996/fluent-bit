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

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;

/**
 * Utils for testing avro.
 */
public class SchemaTestUtils {

    @Data
    @ToString
    @EqualsAndHashCode
    public static class Foo {
        private String field1;
        private String field2;
        private int field3;
        private Bar field4;
        private Color color;
    }

    @Data
    @ToString
    @EqualsAndHashCode
    public static class Bar {
        private boolean field1;
    }

    @Data
    @ToString
    @EqualsAndHashCode
    public static class DerivedFoo extends Foo {
        private String field5;
        private int field6;
        private Foo foo;
    }

    public enum  Color {
        RED,
        BLUE
    }

    @Data
    @ToString
    @EqualsAndHashCode
    public static class DerivedDerivedFoo extends DerivedFoo {
        private String field7;
        private int field8;
        private DerivedFoo derivedFoo;
        private Foo foo2;
    }

    public static final String SCHEMA_JSON
            = "{\"type\":\"record\",\"name\":\"Foo\",\"namespace\":\"org.apache.pulsar.client.schema" +
            ".SchemaTestUtils$\",\"fields\":[{\"name\":\"field1\",\"type\":[\"null\",\"string\"],\"default\":null}," +
            "{\"name\":\"field2\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"field3\"," +
            "\"type\":\"int\"},{\"name\":\"field4\",\"type\":[\"null\",{\"type\":\"record\",\"name\":\"Bar\"," +
            "\"fields\":[{\"name\":\"field1\",\"type\":\"boolean\"}]}],\"default\":null},{\"name\":\"color\"," +
            "\"type\":[\"null\",{\"type\":\"enum\",\"name\":\"Color\",\"symbols\":[\"RED\",\"BLUE\"]}]," +
            "\"default\":null}]}";

    public static String[] FOO_FIELDS = {
            "field1",
            "field2",
            "field3",
            "field4",
            "color"
    };

}
