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
package org.apache.pulsar.client.impl.conf;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.testng.annotations.Test;

/**
 * Unit test {@link ConfigurationDataUtils}.
 */
public class ConfigurationDataUtilsTest {

    @Test
    public void testLoadClientConfigurationData() {
        ClientConfigurationData confData = new ClientConfigurationData();
        confData.setServiceUrl("pulsar://unknown:6650");
        confData.setMaxLookupRequest(600);
        confData.setNumIoThreads(33);
        Map<String, Object> config = new HashMap<>();
        config.put("serviceUrl", "pulsar://localhost:6650");
        config.put("maxLookupRequest", 70000);
        confData = ConfigurationDataUtils.loadData(config, confData, ClientConfigurationData.class);
        assertEquals("pulsar://localhost:6650", confData.getServiceUrl());
        assertEquals(70000, confData.getMaxLookupRequest());
        assertEquals(33, confData.getNumIoThreads());
    }

    @Test
    public void testLoadProducerConfigurationData() {
        ProducerConfigurationData confData = new ProducerConfigurationData();
        confData.setProducerName("unset");
        confData.setBatchingEnabled(true);
        confData.setBatchingMaxMessages(1234);
        Map<String, Object> config = new HashMap<>();
        config.put("producerName", "test-producer");
        config.put("batchingEnabled", false);
        confData = ConfigurationDataUtils.loadData(config, confData, ProducerConfigurationData.class);
        assertEquals("test-producer", confData.getProducerName());
        assertEquals(false, confData.isBatchingEnabled());
        assertEquals(1234, confData.getBatchingMaxMessages());
    }

    @Test
    public void testLoadConsumerConfigurationData() {
        ConsumerConfigurationData confData = new ConsumerConfigurationData();
        confData.setSubscriptionName("unknown-subscription");
        confData.setPriorityLevel(10000);
        confData.setConsumerName("unknown-consumer");
        Map<String, Object> config = new HashMap<>();
        config.put("subscriptionName", "test-subscription");
        config.put("priorityLevel", 100);
        confData = ConfigurationDataUtils.loadData(config, confData, ConsumerConfigurationData.class);
        assertEquals("test-subscription", confData.getSubscriptionName());
        assertEquals(100, confData.getPriorityLevel());
        assertEquals("unknown-consumer", confData.getConsumerName());
    }

    @Test
    public void testLoadReaderConfigurationData() {
        ReaderConfigurationData confData = new ReaderConfigurationData();
        confData.setTopicName("unknown");
        confData.setReceiverQueueSize(1000000);
        confData.setReaderName("unknown-reader");
        Map<String, Object> config = new HashMap<>();
        config.put("topicName", "test-topic");
        config.put("receiverQueueSize", 100);
        confData = ConfigurationDataUtils.loadData(config, confData, ReaderConfigurationData.class);
        assertEquals("test-topic", confData.getTopicName());
        assertEquals(100, confData.getReceiverQueueSize());
        assertEquals("unknown-reader", confData.getReaderName());
    }

    @Test
    public void testLoadConfigurationDataWithUnknownFields() {
        ReaderConfigurationData confData = new ReaderConfigurationData();
        confData.setTopicName("unknown");
        confData.setReceiverQueueSize(1000000);
        confData.setReaderName("unknown-reader");
        Map<String, Object> config = new HashMap<>();
        config.put("unknown", "test-topic");
        config.put("receiverQueueSize", 100);
        try {
            ConfigurationDataUtils.loadData(config, confData, ReaderConfigurationData.class);
            fail("Should fail loading configuration data with unknown fields");
        } catch (RuntimeException re) {
            assertTrue(re.getCause() instanceof IOException);
        }
    }
}
