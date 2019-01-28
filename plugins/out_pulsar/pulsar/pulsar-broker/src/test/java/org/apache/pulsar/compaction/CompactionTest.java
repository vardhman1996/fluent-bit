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
package org.apache.pulsar.compaction;

import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import com.google.common.collect.Sets;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.api.OpenBuilder;
import org.apache.bookkeeper.mledger.ManagedLedgerInfo;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.client.api.CompressionType;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.CryptoKeyReader;
import org.apache.pulsar.client.api.EncryptionKeyInfo;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageBuilder;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.MessageRoutingMode;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Reader;
import org.apache.pulsar.client.impl.BatchMessageIdImpl;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.RetentionPolicies;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.testng.Assert;

import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class CompactionTest extends MockedPulsarServiceBaseTest {
    private ScheduledExecutorService compactionScheduler;
    private BookKeeper bk;

    @BeforeMethod
    @Override
    public void setup() throws Exception {
        super.internalSetup();

        admin.clusters().createCluster("use", new ClusterData("http://127.0.0.1:" + BROKER_WEBSERVICE_PORT));
        admin.tenants().createTenant("my-property",
                new TenantInfo(Sets.newHashSet("appid1", "appid2"), Sets.newHashSet("use")));
        admin.namespaces().createNamespace("my-property/use/my-ns");

        compactionScheduler = Executors.newSingleThreadScheduledExecutor(
                new ThreadFactoryBuilder().setNameFormat("compaction-%d").setDaemon(true).build());
        bk = pulsar.getBookKeeperClientFactory().create(this.conf, null);
    }

    @AfterMethod
    @Override
    public void cleanup() throws Exception {
        super.internalCleanup();

        compactionScheduler.shutdownNow();
    }

    @Test
    public void testCompaction() throws Exception {
        String topic = "persistent://my-property/use/my-ns/my-topic1";
        final int numMessages = 20;
        final int maxKeys = 10;

        Producer<byte[]> producer = pulsarClient.newProducer()
            .topic(topic)
            .enableBatching(false)
            .messageRoutingMode(MessageRoutingMode.SinglePartition)
            .create();

        Map<String, byte[]> expected = new HashMap<>();
        List<Pair<String, byte[]>> all = new ArrayList<>();
        Random r = new Random(0);

        pulsarClient.newConsumer().topic(topic).subscriptionName("sub1").readCompacted(true).subscribe().close();

        for (int j = 0; j < numMessages; j++) {
            int keyIndex = r.nextInt(maxKeys);
            String key = "key" + keyIndex;
            byte[] data = ("my-message-" + key + "-" + j).getBytes();
            producer.newMessage().key(key).value(data).send();
            expected.put(key, data);
            all.add(Pair.of(key, data));
        }

        Compactor compactor = new TwoPhaseCompactor(conf, pulsarClient, bk, compactionScheduler);
        compactor.compact(topic).get();

        // consumer with readCompacted enabled only get compacted entries
        try (Consumer<byte[]> consumer = pulsarClient.newConsumer().topic(topic).subscriptionName("sub1")
                .readCompacted(true).subscribe()) {
            while (true) {
                Message<byte[]> m = consumer.receive(2, TimeUnit.SECONDS);
                Assert.assertEquals(expected.remove(m.getKey()), m.getData());
                if (expected.isEmpty()) {
                    break;
                }
            }
            Assert.assertTrue(expected.isEmpty());
        }

        // can get full backlog if read compacted disabled
        try (Consumer<byte[]> consumer = pulsarClient.newConsumer().topic(topic).subscriptionName("sub1")
                .readCompacted(false).subscribe()) {
            while (true) {
                Message<byte[]> m = consumer.receive(2, TimeUnit.SECONDS);
                Pair<String, byte[]> expectedMessage = all.remove(0);
                Assert.assertEquals(expectedMessage.getLeft(), m.getKey());
                Assert.assertEquals(expectedMessage.getRight(), m.getData());
                if (all.isEmpty()) {
                    break;
                }
            }
            Assert.assertTrue(all.isEmpty());
        }
    }

    @Test
    public void testCompactionWithReader() throws Exception {
        String topic = "persistent://my-property/use/my-ns/my-topic1";
        final int numMessages = 20;
        final int maxKeys = 10;

        // Configure retention to ensue data is retained for reader
        admin.namespaces().setRetention("my-property/use/my-ns", new RetentionPolicies(-1, -1));

        Producer<byte[]> producer = pulsarClient.newProducer()
            .topic(topic)
            .enableBatching(false)
            .messageRoutingMode(MessageRoutingMode.SinglePartition)
            .create();

        Map<String, String> expected = new HashMap<>();
        List<Pair<String, String>> all = new ArrayList<>();
        Random r = new Random(0);

        for (int j = 0; j < numMessages; j++) {
            int keyIndex = r.nextInt(maxKeys);
            String key = "key" + keyIndex;
            String value = "my-message-" + key + "-" + j;
            producer.newMessage().key(key).value(value.getBytes()).send();
            expected.put(key, value);
            all.add(Pair.of(key, value));
        }

        Compactor compactor = new TwoPhaseCompactor(conf, pulsarClient, bk, compactionScheduler);
        compactor.compact(topic).get();



        // consumer with readCompacted enabled only get compacted entries
        try (Reader<byte[]> reader = pulsarClient.newReader().topic(topic).readCompacted(true)
                .startMessageId(MessageId.earliest).create()) {
            while (true) {
                Message<byte[]> m = reader.readNext(2, TimeUnit.SECONDS);
                Assert.assertEquals(expected.remove(m.getKey()), new String(m.getData()));
                if (expected.isEmpty()) {
                    break;
                }
            }
            Assert.assertTrue(expected.isEmpty());
        }

        // can get full backlog if read compacted disabled
        try (Reader<byte[]> reader = pulsarClient.newReader().topic(topic).readCompacted(false)
                .startMessageId(MessageId.earliest).create()) {
            while (true) {
                Message<byte[]> m = reader.readNext(2, TimeUnit.SECONDS);
                Pair<String, String> expectedMessage = all.remove(0);
                Assert.assertEquals(expectedMessage.getLeft(), m.getKey());
                Assert.assertEquals(expectedMessage.getRight(), new String(m.getData()));
                if (all.isEmpty()) {
                    break;
                }
            }
            Assert.assertTrue(all.isEmpty());
        }
    }


    @Test
    public void testReadCompactedBeforeCompaction() throws Exception {
        String topic = "persistent://my-property/use/my-ns/my-topic1";

        Producer<byte[]> producer = pulsarClient.newProducer()
            .topic(topic)
            .enableBatching(false)
            .create();

        pulsarClient.newConsumer().topic(topic).subscriptionName("sub1").readCompacted(true).subscribe().close();

        producer.newMessage().key("key0").value("content0".getBytes()).send();
        producer.newMessage().key("key0").value("content1".getBytes()).send();
        producer.newMessage().key("key0").value("content2".getBytes()).send();

        try (Consumer<byte[]> consumer = pulsarClient.newConsumer().topic(topic).subscriptionName("sub1")
                .readCompacted(true).subscribe()) {
            Message<byte[]> m = consumer.receive();
            Assert.assertEquals(m.getKey(), "key0");
            Assert.assertEquals(m.getData(), "content0".getBytes());

            m = consumer.receive();
            Assert.assertEquals(m.getKey(), "key0");
            Assert.assertEquals(m.getData(), "content1".getBytes());

            m = consumer.receive();
            Assert.assertEquals(m.getKey(), "key0");
            Assert.assertEquals(m.getData(), "content2".getBytes());
        }

        Compactor compactor = new TwoPhaseCompactor(conf, pulsarClient, bk, compactionScheduler);
        compactor.compact(topic).get();

        try (Consumer<byte[]> consumer = pulsarClient.newConsumer().topic(topic).subscriptionName("sub1")
                .readCompacted(true).subscribe()) {
            Message<byte[]> m = consumer.receive();
            Assert.assertEquals(m.getKey(), "key0");
            Assert.assertEquals(m.getData(), "content2".getBytes());
        }
    }

    @Test
    public void testReadEntriesAfterCompaction() throws Exception {
        String topic = "persistent://my-property/use/my-ns/my-topic1";

        Producer<byte[]> producer = pulsarClient.newProducer()
            .topic(topic)
            .enableBatching(false)
            .create();

        pulsarClient.newConsumer().topic(topic).subscriptionName("sub1").readCompacted(true).subscribe().close();

        producer.newMessage().key("key0").value("content0".getBytes()).send();
        producer.newMessage().key("key0").value("content1".getBytes()).send();
        producer.newMessage().key("key0").value("content2".getBytes()).send();

        Compactor compactor = new TwoPhaseCompactor(conf, pulsarClient, bk, compactionScheduler);
        compactor.compact(topic).get();

        producer.newMessage().key("key0").value("content3".getBytes()).send();

        try (Consumer<byte[]> consumer = pulsarClient.newConsumer().topic(topic).subscriptionName("sub1")
                .readCompacted(true).subscribe()) {
            Message<byte[]> m = consumer.receive();
            Assert.assertEquals(m.getKey(), "key0");
            Assert.assertEquals(m.getData(), "content2".getBytes());

            m = consumer.receive();
            Assert.assertEquals(m.getKey(), "key0");
            Assert.assertEquals(m.getData(), "content3".getBytes());
        }
    }

    @Test
    public void testSeekEarliestAfterCompaction() throws Exception {
        String topic = "persistent://my-property/use/my-ns/my-topic1";

        Producer<byte[]> producer = pulsarClient.newProducer()
            .topic(topic)
            .enableBatching(false)
            .create();

        producer.newMessage().key("key0").value("content0".getBytes()).send();
        producer.newMessage().key("key0").value("content1".getBytes()).send();
        producer.newMessage().key("key0").value("content2".getBytes()).send();

        Compactor compactor = new TwoPhaseCompactor(conf, pulsarClient, bk, compactionScheduler);
        compactor.compact(topic).get();

        try (Consumer<byte[]> consumer = pulsarClient.newConsumer().topic(topic).subscriptionName("sub1")
                .readCompacted(true).subscribe()) {
            consumer.seek(MessageId.earliest);
            Message<byte[]> m = consumer.receive();
            Assert.assertEquals(m.getKey(), "key0");
            Assert.assertEquals(m.getData(), "content2".getBytes());
        }

        try (Consumer<byte[]> consumer = pulsarClient.newConsumer().topic(topic).subscriptionName("sub1")
                .readCompacted(false).subscribe()) {
            consumer.seek(MessageId.earliest);

            Message<byte[]> m = consumer.receive();
            Assert.assertEquals(m.getKey(), "key0");
            Assert.assertEquals(m.getData(), "content0".getBytes());

            m = consumer.receive();
            Assert.assertEquals(m.getKey(), "key0");
            Assert.assertEquals(m.getData(), "content1".getBytes());

            m = consumer.receive();
            Assert.assertEquals(m.getKey(), "key0");
            Assert.assertEquals(m.getData(), "content2".getBytes());
        }
    }

    @Test
    public void testBrokerRestartAfterCompaction() throws Exception {
        String topic = "persistent://my-property/use/my-ns/my-topic1";

        Producer<byte[]> producer = pulsarClient.newProducer()
            .topic(topic)
            .enableBatching(false)
            .create();

        pulsarClient.newConsumer().topic(topic).subscriptionName("sub1").readCompacted(true).subscribe().close();

        producer.newMessage().key("key0").value("content0".getBytes()).send();
        producer.newMessage().key("key0").value("content1".getBytes()).send();
        producer.newMessage().key("key0").value("content2".getBytes()).send();

        Compactor compactor = new TwoPhaseCompactor(conf, pulsarClient, bk, compactionScheduler);
        compactor.compact(topic).get();

        try (Consumer<byte[]> consumer = pulsarClient.newConsumer().topic(topic).subscriptionName("sub1")
                .readCompacted(true).subscribe()) {
            Message<byte[]> m = consumer.receive();
            Assert.assertEquals(m.getKey(), "key0");
            Assert.assertEquals(m.getData(), "content2".getBytes());
        }

        stopBroker();
        try (Consumer<byte[]> consumer = pulsarClient.newConsumer().topic(topic).subscriptionName("sub1")
                .readCompacted(true).subscribe()) {
            consumer.receive();
            Assert.fail("Shouldn't have been able to receive anything");
        } catch (PulsarClientException e) {
            // correct behaviour
        }
        startBroker();

        try (Consumer<byte[]> consumer = pulsarClient.newConsumer().topic(topic).subscriptionName("sub1")
                .readCompacted(true).subscribe()) {
            Message<byte[]> m = consumer.receive();
            Assert.assertEquals(m.getKey(), "key0");
            Assert.assertEquals(m.getData(), "content2".getBytes());
        }
    }

    @Test
    public void testCompactEmptyTopic() throws Exception {
        String topic = "persistent://my-property/use/my-ns/my-topic1";

        Producer<byte[]> producer = pulsarClient.newProducer()
            .topic(topic)
            .enableBatching(false)
            .create();

        pulsarClient.newConsumer().topic(topic).subscriptionName("sub1").readCompacted(true).subscribe().close();

        new TwoPhaseCompactor(conf, pulsarClient, bk, compactionScheduler);

        producer.newMessage().key("key0").value("content0".getBytes()).send();

        try (Consumer<byte[]> consumer = pulsarClient.newConsumer().topic(topic).subscriptionName("sub1")
                .readCompacted(true).subscribe()) {
            Message<byte[]> m = consumer.receive();
            Assert.assertEquals(m.getKey(), "key0");
            Assert.assertEquals(m.getData(), "content0".getBytes());
        }
    }

    @Test
    public void testFirstMessageRetained() throws Exception {
        String topic = "persistent://my-property/use/my-ns/my-topic1";

        // subscribe before sending anything, so that we get all messages
        pulsarClient.newConsumer().topic(topic).subscriptionName("sub1")
            .readCompacted(true).subscribe().close();

        try (Producer<byte[]> producer = pulsarClient.newProducer().topic(topic).enableBatching(false).create()) {
            producer.newMessage().key("key1").value("my-message-1".getBytes()).sendAsync();
            producer.newMessage().key("key2").value("my-message-2".getBytes()).sendAsync();
            producer.newMessage().key("key2").value("my-message-3".getBytes()).send();
        }

        // Read messages before compaction to get ids
        List<Message<byte[]>> messages = new ArrayList<>();
        try (Consumer<byte[]> consumer = pulsarClient.newConsumer().topic(topic)
                .subscriptionName("sub1").readCompacted(true).subscribe()) {
            messages.add(consumer.receive());
            messages.add(consumer.receive());
            messages.add(consumer.receive());
        }

        // compact the topic
        Compactor compactor = new TwoPhaseCompactor(conf, pulsarClient, bk, compactionScheduler);
        compactor.compact(topic).get();

        // Check that messages after compaction have same ids
        try (Consumer<byte[]> consumer = pulsarClient.newConsumer().topic(topic)
                .subscriptionName("sub1").readCompacted(true).subscribe()){
            Message<byte[]> message1 = consumer.receive();
            Assert.assertEquals(message1.getKey(), "key1");
            Assert.assertEquals(new String(message1.getData()), "my-message-1");
            Assert.assertEquals(message1.getMessageId(), messages.get(0).getMessageId());

            Message<byte[]> message2 = consumer.receive();
            Assert.assertEquals(message2.getKey(), "key2");
            Assert.assertEquals(new String(message2.getData()), "my-message-3");
            Assert.assertEquals(message2.getMessageId(), messages.get(2).getMessageId());
        }
    }

    @Test
    public void testBatchMessageIdsDontChange() throws Exception {
        String topic = "persistent://my-property/use/my-ns/my-topic1";

        // subscribe before sending anything, so that we get all messages
        pulsarClient.newConsumer().topic(topic).subscriptionName("sub1")
            .readCompacted(true).subscribe().close();

        try (Producer<byte[]> producer = pulsarClient.newProducer().topic(topic)
            .maxPendingMessages(3)
            .enableBatching(true)
            .batchingMaxMessages(3)
            .batchingMaxPublishDelay(1, TimeUnit.HOURS)
            .messageRoutingMode(MessageRoutingMode.SinglePartition)
            .create()
        ) {
            producer.newMessage().key("key1").value("my-message-1".getBytes()).sendAsync();
            producer.newMessage().key("key2").value("my-message-2".getBytes()).sendAsync();
            producer.newMessage().key("key2").value("my-message-3".getBytes()).send();
        }

        // Read messages before compaction to get ids
        List<Message<byte[]>> messages = new ArrayList<>();
        try (Consumer<byte[]> consumer = pulsarClient.newConsumer().topic(topic)
             .subscriptionName("sub1").readCompacted(true).subscribe()) {
            messages.add(consumer.receive());
            messages.add(consumer.receive());
            messages.add(consumer.receive());
        }

        // Ensure all messages are in same batch
        Assert.assertEquals(((BatchMessageIdImpl)messages.get(0).getMessageId()).getLedgerId(),
                            ((BatchMessageIdImpl)messages.get(1).getMessageId()).getLedgerId());
        Assert.assertEquals(((BatchMessageIdImpl)messages.get(0).getMessageId()).getLedgerId(),
                            ((BatchMessageIdImpl)messages.get(2).getMessageId()).getLedgerId());
        Assert.assertEquals(((BatchMessageIdImpl)messages.get(0).getMessageId()).getEntryId(),
                            ((BatchMessageIdImpl)messages.get(1).getMessageId()).getEntryId());
        Assert.assertEquals(((BatchMessageIdImpl)messages.get(0).getMessageId()).getEntryId(),
                            ((BatchMessageIdImpl)messages.get(2).getMessageId()).getEntryId());

        // compact the topic
        Compactor compactor = new TwoPhaseCompactor(conf, pulsarClient, bk, compactionScheduler);
        compactor.compact(topic).get();

        // Check that messages after compaction have same ids
        try (Consumer<byte[]> consumer = pulsarClient.newConsumer().topic(topic)
                .subscriptionName("sub1").readCompacted(true).subscribe()){
            Message<byte[]> message1 = consumer.receive();
            Assert.assertEquals(message1.getKey(), "key1");
            Assert.assertEquals(new String(message1.getData()), "my-message-1");
            Assert.assertEquals(message1.getMessageId(), messages.get(0).getMessageId());

            Message<byte[]> message2 = consumer.receive();
            Assert.assertEquals(message2.getKey(), "key2");
            Assert.assertEquals(new String(message2.getData()), "my-message-3");
            Assert.assertEquals(message2.getMessageId(), messages.get(2).getMessageId());
        }
    }

    @Test
    public void testWholeBatchCompactedOut() throws Exception {
        String topic = "persistent://my-property/use/my-ns/my-topic1";

        // subscribe before sending anything, so that we get all messages
        pulsarClient.newConsumer().topic(topic).subscriptionName("sub1")
            .readCompacted(true).subscribe().close();

        try (Producer<byte[]> producerNormal = pulsarClient.newProducer().topic(topic)
                 .enableBatching(false)
                 .messageRoutingMode(MessageRoutingMode.SinglePartition)
                 .create();
             Producer<byte[]> producerBatch = pulsarClient.newProducer().topic(topic)
                 .maxPendingMessages(3)
                 .enableBatching(true)
                 .batchingMaxMessages(3)
                 .batchingMaxPublishDelay(1, TimeUnit.HOURS)
                 .messageRoutingMode(MessageRoutingMode.SinglePartition)
                 .create()) {
            producerBatch.newMessage().key("key1").value("my-message-1".getBytes()).sendAsync();
            producerBatch.newMessage().key("key1").value("my-message-2".getBytes()).sendAsync();
            producerBatch.newMessage().key("key1").value("my-message-3".getBytes()).sendAsync();
            producerNormal.newMessage().key("key1").value("my-message-4".getBytes()).send();
        }

        // compact the topic
        Compactor compactor = new TwoPhaseCompactor(conf, pulsarClient, bk, compactionScheduler);
        compactor.compact(topic).get();

        try (Consumer<byte[]> consumer = pulsarClient.newConsumer().topic(topic)
                .subscriptionName("sub1").readCompacted(true).subscribe()){
            Message<byte[]> message = consumer.receive();
            Assert.assertEquals(message.getKey(), "key1");
            Assert.assertEquals(new String(message.getData()), "my-message-4");
        }
    }

    @Test
    public void testKeyLessMessagesPassThrough() throws Exception {
        String topic = "persistent://my-property/use/my-ns/my-topic1";

        // subscribe before sending anything, so that we get all messages
        pulsarClient.newConsumer().topic(topic).subscriptionName("sub1")
            .readCompacted(true).subscribe().close();

        try (Producer<byte[]> producerNormal = pulsarClient.newProducer().topic(topic).create();
                Producer<byte[]> producerBatch = pulsarClient.newProducer().topic(topic).maxPendingMessages(3)
                .enableBatching(true).batchingMaxMessages(3)
                .batchingMaxPublishDelay(1, TimeUnit.HOURS).create()) {
            producerNormal.newMessage().value("my-message-1".getBytes()).send();

            producerBatch.newMessage().value("my-message-2".getBytes()).sendAsync();
            producerBatch.newMessage().key("key1").value("my-message-3".getBytes()).sendAsync();
            producerBatch.newMessage().key("key1").value("my-message-4".getBytes()).send();

            producerBatch.newMessage().key("key2").value("my-message-5".getBytes()).sendAsync();
            producerBatch.newMessage().key("key2").value("my-message-6".getBytes()).sendAsync();
            producerBatch.newMessage().value("my-message-7".getBytes()).send();

            producerNormal.newMessage().value("my-message-8".getBytes()).send();
        }

        // compact the topic
        Compactor compactor = new TwoPhaseCompactor(conf, pulsarClient, bk, compactionScheduler);
        compactor.compact(topic).get();

        try (Consumer<byte[]> consumer = pulsarClient.newConsumer().topic(topic)
                .subscriptionName("sub1").readCompacted(true).subscribe()){
            Message<byte[]> message1 = consumer.receive();
            Assert.assertFalse(message1.hasKey());
            Assert.assertEquals(new String(message1.getData()), "my-message-1");

            Message<byte[]> message2 = consumer.receive();
            Assert.assertFalse(message2.hasKey());
            Assert.assertEquals(new String(message2.getData()), "my-message-2");

            Message<byte[]> message3 = consumer.receive();
            Assert.assertEquals(message3.getKey(), "key1");
            Assert.assertEquals(new String(message3.getData()), "my-message-4");

            Message<byte[]> message4 = consumer.receive();
            Assert.assertEquals(message4.getKey(), "key2");
            Assert.assertEquals(new String(message4.getData()), "my-message-6");

            Message<byte[]> message5 = consumer.receive();
            Assert.assertFalse(message5.hasKey());
            Assert.assertEquals(new String(message5.getData()), "my-message-7");

            Message<byte[]> message6 = consumer.receive();
            Assert.assertFalse(message6.hasKey());
            Assert.assertEquals(new String(message6.getData()), "my-message-8");
        }
    }


    @Test
    public void testEmptyPayloadDeletes() throws Exception {
        String topic = "persistent://my-property/use/my-ns/my-topic1";

        // subscribe before sending anything, so that we get all messages
        pulsarClient.newConsumer().topic(topic).subscriptionName("sub1")
            .readCompacted(true).subscribe().close();

        try (Producer<byte[]> producerNormal = pulsarClient.newProducer()
                 .topic(topic)
                 .enableBatching(false)
                 .create();
             Producer<byte[]> producerBatch = pulsarClient.newProducer()
                 .topic(topic)
                 .maxPendingMessages(3)
                 .enableBatching(true)
                 .batchingMaxMessages(3)
                 .batchingMaxPublishDelay(1, TimeUnit.HOURS)
                 .create()) {

            // key0 persists through it all
            producerNormal.sendAsync(MessageBuilder.create()
                                     .setKey("key0")
                                     .setContent("my-message-0".getBytes()).build()).get();

            // key1 is added but then deleted
            producerNormal.sendAsync(MessageBuilder.create()
                                     .setKey("key1")
                                     .setContent("my-message-1".getBytes()).build()).get();

            producerNormal.sendAsync(MessageBuilder.create()
                                     .setKey("key1").build()).get();

            // key2 is added but deleted in same batch
            producerBatch.sendAsync(MessageBuilder.create()
                                    .setKey("key2")
                                    .setContent("my-message-2".getBytes()).build());
            producerBatch.sendAsync(MessageBuilder.create()
                                    .setKey("key3")
                                    .setContent("my-message-3".getBytes()).build());
            producerBatch.sendAsync(MessageBuilder.create()
                                    .setKey("key2").build()).get();

            // key3 is added in previous batch, deleted in this batch
            producerBatch.sendAsync(MessageBuilder.create()
                                    .setKey("key3").build());
            producerBatch.sendAsync(MessageBuilder.create()
                                    .setKey("key4")
                                    .setContent("my-message-3".getBytes()).build());
            producerBatch.sendAsync(MessageBuilder.create()
                                    .setKey("key4").build()).get();

            // key4 is added, deleted, then resurrected
            producerNormal.sendAsync(MessageBuilder.create()
                                     .setKey("key4")
                                     .setContent("my-message-4".getBytes()).build()).get();
        }

        // compact the topic
        Compactor compactor = new TwoPhaseCompactor(conf, pulsarClient, bk, compactionScheduler);
        compactor.compact(topic).get();

        try (Consumer<byte[]> consumer = pulsarClient.newConsumer().topic(topic)
                .subscriptionName("sub1").readCompacted(true).subscribe()){
            Message<byte[]> message1 = consumer.receive();
            Assert.assertEquals(message1.getKey(), "key0");
            Assert.assertEquals(new String(message1.getData()), "my-message-0");

            Message<byte[]> message2 = consumer.receive();
            Assert.assertEquals(message2.getKey(), "key4");
            Assert.assertEquals(new String(message2.getData()), "my-message-4");
        }
    }

    @Test
    public void testEmptyPayloadDeletesWhenCompressed() throws Exception {
        String topic = "persistent://my-property/use/my-ns/my-topic1";

        // subscribe before sending anything, so that we get all messages
        pulsarClient.newConsumer().topic(topic).subscriptionName("sub1")
            .readCompacted(true).subscribe().close();

        try (Producer<byte[]> producerNormal = pulsarClient.newProducer()
                 .topic(topic)
                 .enableBatching(false)
                 .compressionType(CompressionType.LZ4)
                 .create();
             Producer<byte[]> producerBatch = pulsarClient.newProducer()
                 .topic(topic)
                 .maxPendingMessages(3)
                 .enableBatching(true)
                 .compressionType(CompressionType.LZ4)
                 .batchingMaxMessages(3)
                 .batchingMaxPublishDelay(1, TimeUnit.HOURS)
                 .create()) {

            // key0 persists through it all
            producerNormal.sendAsync(MessageBuilder.create()
                                     .setKey("key0")
                                     .setContent("my-message-0".getBytes()).build()).get();

            // key1 is added but then deleted
            producerNormal.sendAsync(MessageBuilder.create()
                                     .setKey("key1")
                                     .setContent("my-message-1".getBytes()).build()).get();

            producerNormal.sendAsync(MessageBuilder.create()
                                     .setKey("key1").build()).get();

            // key2 is added but deleted in same batch
            producerBatch.sendAsync(MessageBuilder.create()
                                    .setKey("key2")
                                    .setContent("my-message-2".getBytes()).build());
            producerBatch.sendAsync(MessageBuilder.create()
                                    .setKey("key3")
                                    .setContent("my-message-3".getBytes()).build());
            producerBatch.sendAsync(MessageBuilder.create()
                                    .setKey("key2").build()).get();

            // key3 is added in previous batch, deleted in this batch
            producerBatch.sendAsync(MessageBuilder.create()
                                    .setKey("key3").build());
            producerBatch.sendAsync(MessageBuilder.create()
                                    .setKey("key4")
                                    .setContent("my-message-3".getBytes()).build());
            producerBatch.sendAsync(MessageBuilder.create()
                                    .setKey("key4").build()).get();

            // key4 is added, deleted, then resurrected
            producerNormal.sendAsync(MessageBuilder.create()
                                     .setKey("key4")
                                     .setContent("my-message-4".getBytes()).build()).get();
        }

        // compact the topic
        Compactor compactor = new TwoPhaseCompactor(conf, pulsarClient, bk, compactionScheduler);
        compactor.compact(topic).get();

        try (Consumer<byte[]> consumer = pulsarClient.newConsumer().topic(topic)
                .subscriptionName("sub1").readCompacted(true).subscribe()){
            Message<byte[]> message1 = consumer.receive();
            Assert.assertEquals(message1.getKey(), "key0");
            Assert.assertEquals(new String(message1.getData()), "my-message-0");

            Message<byte[]> message2 = consumer.receive();
            Assert.assertEquals(message2.getKey(), "key4");
            Assert.assertEquals(new String(message2.getData()), "my-message-4");
        }
    }

    // test compact no keys

    @Test
    public void testCompactorReadsCompacted() throws Exception {
        String topic = "persistent://my-property/use/my-ns/my-topic1";

        // capture opened ledgers
        Set<Long> ledgersOpened = Sets.newConcurrentHashSet();
        when(mockBookKeeper.newOpenLedgerOp()).thenAnswer(
                (invocation) -> {
                    OpenBuilder builder = (OpenBuilder)spy(invocation.callRealMethod());
                    when(builder.withLedgerId(anyLong())).thenAnswer(
                            (invocation2) -> {
                                ledgersOpened.add((Long)invocation2.getArguments()[0]);
                                return invocation2.callRealMethod();
                            });
                    return builder;
                });

        // subscribe before sending anything, so that we get all messages in sub1
        pulsarClient.newConsumer().topic(topic).subscriptionName("sub1").subscribe().close();

        // create the topic on the broker
        try (Producer<byte[]> producerNormal = pulsarClient.newProducer().topic(topic).create()) {
            producerNormal.send(MessageBuilder.create()
                                .setKey("key0")
                                .setContent("my-message-0".getBytes()).build());
        }

        // force ledger roll
        pulsar.getBrokerService().getTopicReference(topic).get().close().get();

        // write a message to avoid issue #1517
        try (Producer<byte[]> producerNormal = pulsarClient.newProducer().topic(topic).create()) {
            producerNormal.send(MessageBuilder.create()
                                .setKey("key1")
                                .setContent("my-message-1".getBytes()).build());
        }

        // verify second ledger created
        String managedLedgerName = ((PersistentTopic)pulsar.getBrokerService().getTopicReference(topic).get())
            .getManagedLedger().getName();
        ManagedLedgerInfo info = pulsar.getManagedLedgerFactory().getManagedLedgerInfo(managedLedgerName);
        Assert.assertEquals(info.ledgers.size(), 2);
        Assert.assertTrue(ledgersOpened.isEmpty()); // no ledgers should have been opened

        // compact the topic
        Compactor compactor = new TwoPhaseCompactor(conf, pulsarClient, bk, compactionScheduler);
        compactor.compact(topic).get();

        // should have opened all except last to read
        Assert.assertTrue(ledgersOpened.contains(info.ledgers.get(0).ledgerId));
        Assert.assertFalse(ledgersOpened.contains(info.ledgers.get(1).ledgerId));
        ledgersOpened.clear();

        // force broker to close resources for topic
        pulsar.getBrokerService().getTopicReference(topic).get().close().get();

        // write a message to avoid issue #1517
        try (Producer<byte[]> producerNormal = pulsarClient.newProducer().topic(topic).create()) {
            producerNormal.send(MessageBuilder.create()
                                .setKey("key2")
                                .setContent("my-message-2".getBytes()).build());
        }

        info = pulsar.getManagedLedgerFactory().getManagedLedgerInfo(managedLedgerName);
        Assert.assertEquals(info.ledgers.size(), 3);

        // should only have opened the penultimate ledger to get stat
        Assert.assertFalse(ledgersOpened.contains(info.ledgers.get(0).ledgerId));
        Assert.assertFalse(ledgersOpened.contains(info.ledgers.get(1).ledgerId));
        Assert.assertFalse(ledgersOpened.contains(info.ledgers.get(2).ledgerId));
        ledgersOpened.clear();

        // compact the topic again
        compactor.compact(topic).get();

        // shouldn't have opened first ledger (already compacted), penultimate would have some uncompacted data.
        // last ledger already open for writing
        Assert.assertFalse(ledgersOpened.contains(info.ledgers.get(0).ledgerId));
        Assert.assertTrue(ledgersOpened.contains(info.ledgers.get(1).ledgerId));
        Assert.assertFalse(ledgersOpened.contains(info.ledgers.get(2).ledgerId));

        // all three messages should be there when we read compacted
        try (Consumer<byte[]> consumer = pulsarClient.newConsumer().topic(topic)
                .subscriptionName("sub1").readCompacted(true).subscribe()){
            Message<byte[]> message1 = consumer.receive();
            Assert.assertEquals(message1.getKey(), "key0");
            Assert.assertEquals(new String(message1.getData()), "my-message-0");

            Message<byte[]> message2 = consumer.receive();
            Assert.assertEquals(message2.getKey(), "key1");
            Assert.assertEquals(new String(message2.getData()), "my-message-1");

            Message<byte[]> message3 = consumer.receive();
            Assert.assertEquals(message3.getKey(), "key2");
            Assert.assertEquals(new String(message3.getData()), "my-message-2");
        }
    }

    @Test
    public void testCompactCompressedNoBatch() throws Exception {
        String topic = "persistent://my-property/use/my-ns/my-topic1";

        // subscribe before sending anything, so that we get all messages
        pulsarClient.newConsumer().topic(topic).subscriptionName("sub1")
            .readCompacted(true).subscribe().close();

        try (Producer<byte[]> producer = pulsarClient.newProducer().topic(topic)
                .compressionType(CompressionType.LZ4).enableBatching(false).create()) {
            producer.sendAsync(MessageBuilder.create()
                               .setKey("key1")
                               .setContent("my-message-1".getBytes()).build());
            producer.sendAsync(MessageBuilder.create()
                               .setKey("key2")
                               .setContent("my-message-2".getBytes()).build());
            producer.sendAsync(MessageBuilder.create()
                               .setKey("key2")
                               .setContent("my-message-3".getBytes()).build()).get();
        }

        // compact the topic
        Compactor compactor = new TwoPhaseCompactor(conf, pulsarClient, bk, compactionScheduler);
        compactor.compact(topic).get();

        try (Consumer<byte[]> consumer = pulsarClient.newConsumer().topic(topic)
                .subscriptionName("sub1").readCompacted(true).subscribe()){
            Message<byte[]> message1 = consumer.receive();
            Assert.assertEquals(message1.getKey(), "key1");
            Assert.assertEquals(new String(message1.getData()), "my-message-1");

            Message<byte[]> message2 = consumer.receive();
            Assert.assertEquals(message2.getKey(), "key2");
            Assert.assertEquals(new String(message2.getData()), "my-message-3");
        }
    }

    @Test
    public void testCompactCompressedBatching() throws Exception {
        String topic = "persistent://my-property/use/my-ns/my-topic1";

        // subscribe before sending anything, so that we get all messages
        pulsarClient.newConsumer().topic(topic).subscriptionName("sub1")
            .readCompacted(true).subscribe().close();

        try (Producer<byte[]> producer = pulsarClient.newProducer().topic(topic)
                .compressionType(CompressionType.LZ4)
                .maxPendingMessages(3)
                .enableBatching(true)
                .batchingMaxMessages(3)
                .batchingMaxPublishDelay(1, TimeUnit.HOURS).create()) {
            producer.sendAsync(MessageBuilder.create()
                               .setKey("key1")
                               .setContent("my-message-1".getBytes()).build());
            producer.sendAsync(MessageBuilder.create()
                               .setKey("key2")
                               .setContent("my-message-2".getBytes()).build());
            producer.sendAsync(MessageBuilder.create()
                               .setKey("key2")
                               .setContent("my-message-3".getBytes()).build()).get();
        }

        // compact the topic
        Compactor compactor = new TwoPhaseCompactor(conf, pulsarClient, bk, compactionScheduler);
        compactor.compact(topic).get();

        try (Consumer<byte[]> consumer = pulsarClient.newConsumer().topic(topic)
                .subscriptionName("sub1").readCompacted(true).subscribe()){
            Message<byte[]> message1 = consumer.receive();
            Assert.assertEquals(message1.getKey(), "key1");
            Assert.assertEquals(new String(message1.getData()), "my-message-1");

            Message<byte[]> message2 = consumer.receive();
            Assert.assertEquals(message2.getKey(), "key2");
            Assert.assertEquals(new String(message2.getData()), "my-message-3");
        }
    }

    class EncKeyReader implements CryptoKeyReader {
        EncryptionKeyInfo keyInfo = new EncryptionKeyInfo();

        @Override
        public EncryptionKeyInfo getPublicKey(String keyName, Map<String, String> keyMeta) {
            String CERT_FILE_PATH = "./src/test/resources/certificate/public-key." + keyName;
            if (Files.isReadable(Paths.get(CERT_FILE_PATH))) {
                try {
                    keyInfo.setKey(Files.readAllBytes(Paths.get(CERT_FILE_PATH)));
                    return keyInfo;
                } catch (IOException e) {
                    Assert.fail("Failed to read certificate from " + CERT_FILE_PATH);
                }
            } else {
                Assert.fail("Certificate file " + CERT_FILE_PATH + " is not present or not readable.");
            }
            return null;
        }

        @Override
        public EncryptionKeyInfo getPrivateKey(String keyName, Map<String, String> keyMeta) {
            String CERT_FILE_PATH = "./src/test/resources/certificate/private-key." + keyName;
            if (Files.isReadable(Paths.get(CERT_FILE_PATH))) {
                try {
                    keyInfo.setKey(Files.readAllBytes(Paths.get(CERT_FILE_PATH)));
                    return keyInfo;
                } catch (IOException e) {
                    Assert.fail("Failed to read certificate from " + CERT_FILE_PATH);
                }
            } else {
                Assert.fail("Certificate file " + CERT_FILE_PATH + " is not present or not readable.");
            }
            return null;
        }
    }

    @Test
    public void testCompactEncryptedNoBatch() throws Exception {
        String topic = "persistent://my-property/use/my-ns/my-topic1";

        // subscribe before sending anything, so that we get all messages
        pulsarClient.newConsumer().topic(topic).subscriptionName("sub1")
            .readCompacted(true).subscribe().close();

        try (Producer<byte[]> producer = pulsarClient.newProducer().topic(topic)
                .addEncryptionKey("client-ecdsa.pem").cryptoKeyReader(new EncKeyReader())
                .enableBatching(false).create()) {
            producer.sendAsync(MessageBuilder.create()
                               .setKey("key1")
                               .setContent("my-message-1".getBytes()).build());
            producer.sendAsync(MessageBuilder.create()
                               .setKey("key2")
                               .setContent("my-message-2".getBytes()).build());
            producer.sendAsync(MessageBuilder.create()
                               .setKey("key2")
                               .setContent("my-message-3".getBytes()).build()).get();
        }

        // compact the topic
        Compactor compactor = new TwoPhaseCompactor(conf, pulsarClient, bk, compactionScheduler);
        compactor.compact(topic).get();

        // Check that messages after compaction have same ids
        try (Consumer<byte[]> consumer = pulsarClient.newConsumer().topic(topic)
                .subscriptionName("sub1").cryptoKeyReader(new EncKeyReader())
                .readCompacted(true).subscribe()){
            Message<byte[]> message1 = consumer.receive();
            Assert.assertEquals(message1.getKey(), "key1");
            Assert.assertEquals(new String(message1.getData()), "my-message-1");

            Message<byte[]> message2 = consumer.receive();
            Assert.assertEquals(message2.getKey(), "key2");
            Assert.assertEquals(new String(message2.getData()), "my-message-3");
        }
    }

    @Test
    public void testCompactEncryptedBatching() throws Exception {
        String topic = "persistent://my-property/use/my-ns/my-topic1";

        // subscribe before sending anything, so that we get all messages
        pulsarClient.newConsumer().topic(topic).subscriptionName("sub1")
            .readCompacted(true).subscribe().close();

        try (Producer<byte[]> producer = pulsarClient.newProducer().topic(topic)
                .addEncryptionKey("client-ecdsa.pem").cryptoKeyReader(new EncKeyReader())
                .maxPendingMessages(3)
                .enableBatching(true)
                .batchingMaxMessages(3)
                .batchingMaxPublishDelay(1, TimeUnit.HOURS).create()) {
            producer.sendAsync(MessageBuilder.create()
                               .setKey("key1")
                               .setContent("my-message-1".getBytes()).build());
            producer.sendAsync(MessageBuilder.create()
                               .setKey("key2")
                               .setContent("my-message-2".getBytes()).build());
            producer.sendAsync(MessageBuilder.create()
                               .setKey("key2")
                               .setContent("my-message-3".getBytes()).build()).get();
        }

        // compact the topic
        Compactor compactor = new TwoPhaseCompactor(conf, pulsarClient, bk, compactionScheduler);
        compactor.compact(topic).get();

        // with encryption, all messages are passed through compaction as it doesn't
        // have the keys to decrypt the batch payload
        try (Consumer<byte[]> consumer = pulsarClient.newConsumer().topic(topic)
                .subscriptionName("sub1").cryptoKeyReader(new EncKeyReader())
                .readCompacted(true).subscribe()){
            Message<byte[]> message1 = consumer.receive();
            Assert.assertEquals(message1.getKey(), "key1");
            Assert.assertEquals(new String(message1.getData()), "my-message-1");

            Message<byte[]> message2 = consumer.receive();
            Assert.assertEquals(message2.getKey(), "key2");
            Assert.assertEquals(new String(message2.getData()), "my-message-2");

            Message<byte[]> message3 = consumer.receive();
            Assert.assertEquals(message3.getKey(), "key2");
            Assert.assertEquals(new String(message3.getData()), "my-message-3");
        }
    }

    @Test
    public void testCompactEncryptedAndCompressedNoBatch() throws Exception {
        String topic = "persistent://my-property/use/my-ns/my-topic1";

        // subscribe before sending anything, so that we get all messages
        pulsarClient.newConsumer().topic(topic).subscriptionName("sub1")
            .readCompacted(true).subscribe().close();

        try (Producer<byte[]> producer = pulsarClient.newProducer().topic(topic)
                .addEncryptionKey("client-ecdsa.pem").cryptoKeyReader(new EncKeyReader())
                .compressionType(CompressionType.LZ4)
                .enableBatching(false).create()) {
            producer.sendAsync(MessageBuilder.create()
                               .setKey("key1")
                               .setContent("my-message-1".getBytes()).build());
            producer.sendAsync(MessageBuilder.create()
                               .setKey("key2")
                               .setContent("my-message-2".getBytes()).build());
            producer.sendAsync(MessageBuilder.create()
                               .setKey("key2")
                               .setContent("my-message-3".getBytes()).build()).get();
        }

        // compact the topic
        Compactor compactor = new TwoPhaseCompactor(conf, pulsarClient, bk, compactionScheduler);
        compactor.compact(topic).get();

        // Check that messages after compaction have same ids
        try (Consumer<byte[]> consumer = pulsarClient.newConsumer().topic(topic)
                .subscriptionName("sub1").cryptoKeyReader(new EncKeyReader())
                .readCompacted(true).subscribe()){
            Message<byte[]> message1 = consumer.receive();
            Assert.assertEquals(message1.getKey(), "key1");
            Assert.assertEquals(new String(message1.getData()), "my-message-1");

            Message<byte[]> message2 = consumer.receive();
            Assert.assertEquals(message2.getKey(), "key2");
            Assert.assertEquals(new String(message2.getData()), "my-message-3");
        }
    }

    @Test
    public void testCompactEncryptedAndCompressedBatching() throws Exception {
        String topic = "persistent://my-property/use/my-ns/my-topic1";

        // subscribe before sending anything, so that we get all messages
        pulsarClient.newConsumer().topic(topic).subscriptionName("sub1")
            .readCompacted(true).subscribe().close();

        try (Producer<byte[]> producer = pulsarClient.newProducer().topic(topic)
                .addEncryptionKey("client-ecdsa.pem").cryptoKeyReader(new EncKeyReader())
                .compressionType(CompressionType.LZ4)
                .maxPendingMessages(3)
                .enableBatching(true)
                .batchingMaxMessages(3)
                .batchingMaxPublishDelay(1, TimeUnit.HOURS).create()) {
            producer.sendAsync(MessageBuilder.create()
                               .setKey("key1")
                               .setContent("my-message-1".getBytes()).build());
            producer.sendAsync(MessageBuilder.create()
                               .setKey("key2")
                               .setContent("my-message-2".getBytes()).build());
            producer.sendAsync(MessageBuilder.create()
                               .setKey("key2")
                               .setContent("my-message-3".getBytes()).build()).get();
        }

        // compact the topic
        Compactor compactor = new TwoPhaseCompactor(conf, pulsarClient, bk, compactionScheduler);
        compactor.compact(topic).get();

        // with encryption, all messages are passed through compaction as it doesn't
        // have the keys to decrypt the batch payload
        try (Consumer<byte[]> consumer = pulsarClient.newConsumer().topic(topic)
                .subscriptionName("sub1").cryptoKeyReader(new EncKeyReader())
                .readCompacted(true).subscribe()){
            Message<byte[]> message1 = consumer.receive();
            Assert.assertEquals(message1.getKey(), "key1");
            Assert.assertEquals(new String(message1.getData()), "my-message-1");

            Message<byte[]> message2 = consumer.receive();
            Assert.assertEquals(message2.getKey(), "key2");
            Assert.assertEquals(new String(message2.getData()), "my-message-2");

            Message<byte[]> message3 = consumer.receive();
            Assert.assertEquals(message3.getKey(), "key2");
            Assert.assertEquals(new String(message3.getData()), "my-message-3");
        }
    }

    @Test
    public void testEmptyPayloadDeletesWhenEncrypted() throws Exception {
        String topic = "persistent://my-property/use/my-ns/my-topic1";

        // subscribe before sending anything, so that we get all messages
        pulsarClient.newConsumer().topic(topic).subscriptionName("sub1")
            .readCompacted(true).subscribe().close();

        try (Producer<byte[]> producerNormal = pulsarClient.newProducer()
                 .topic(topic)
                 .enableBatching(false)
                 .addEncryptionKey("client-ecdsa.pem").cryptoKeyReader(new EncKeyReader())
                 .create();
             Producer<byte[]> producerBatch = pulsarClient.newProducer()
                 .topic(topic)
                 .maxPendingMessages(3)
                 .enableBatching(true)
                 .addEncryptionKey("client-ecdsa.pem").cryptoKeyReader(new EncKeyReader())
                 .batchingMaxMessages(3)
                 .batchingMaxPublishDelay(1, TimeUnit.HOURS)
                 .create()) {

            // key0 persists through it all
            producerNormal.sendAsync(MessageBuilder.create()
                                     .setKey("key0")
                                     .setContent("my-message-0".getBytes()).build()).get();

            // key1 is added but then deleted
            producerNormal.sendAsync(MessageBuilder.create()
                                     .setKey("key1")
                                     .setContent("my-message-1".getBytes()).build()).get();

            producerNormal.sendAsync(MessageBuilder.create()
                                     .setKey("key1").build()).get();

            // key2 is added but deleted in same batch
            producerBatch.sendAsync(MessageBuilder.create()
                                    .setKey("key2")
                                    .setContent("my-message-2".getBytes()).build());
            producerBatch.sendAsync(MessageBuilder.create()
                                    .setKey("key3")
                                    .setContent("my-message-3".getBytes()).build());
            producerBatch.sendAsync(MessageBuilder.create()
                                    .setKey("key2").build()).get();

            // key4 is added, deleted, then resurrected
            producerNormal.sendAsync(MessageBuilder.create()
                                     .setKey("key4")
                                     .setContent("my-message-4".getBytes()).build()).get();
        }

        // compact the topic
        Compactor compactor = new TwoPhaseCompactor(conf, pulsarClient, bk, compactionScheduler);
        compactor.compact(topic).get();

        try (Consumer<byte[]> consumer = pulsarClient.newConsumer().topic(topic)
                .cryptoKeyReader(new EncKeyReader())
                .subscriptionName("sub1").readCompacted(true).subscribe()){
            Message<byte[]> message1 = consumer.receive();
            Assert.assertEquals(message1.getKey(), "key0");
            Assert.assertEquals(new String(message1.getData()), "my-message-0");

            // see all messages from batch
            Message<byte[]> message2 = consumer.receive();
            Assert.assertEquals(message2.getKey(), "key2");
            Assert.assertEquals(new String(message2.getData()), "my-message-2");

            Message<byte[]> message3 = consumer.receive();
            Assert.assertEquals(message3.getKey(), "key3");
            Assert.assertEquals(new String(message3.getData()), "my-message-3");

            Message<byte[]> message4 = consumer.receive();
            Assert.assertEquals(message4.getKey(), "key2");
            Assert.assertEquals(new String(message4.getData()), "");

            Message<byte[]> message5 = consumer.receive();
            Assert.assertEquals(message5.getKey(), "key4");
            Assert.assertEquals(new String(message5.getData()), "my-message-4");
        }
    }
    
    @Test(timeOut = 20000)
    public void testCompactionWithLastDeletedKey() throws Exception {
        String topic = "persistent://my-property/use/my-ns/my-topic1";

        Producer<byte[]> producer = pulsarClient.newProducer().topic(topic).enableBatching(false)
                .messageRoutingMode(MessageRoutingMode.SinglePartition).create();

        pulsarClient.newConsumer().topic(topic).subscriptionName("sub1").readCompacted(true).subscribe().close();

        producer.newMessage().key("1").value("1".getBytes()).send();
        producer.newMessage().key("2").value("2".getBytes()).send();
        producer.newMessage().key("3").value("3".getBytes()).send();
        producer.newMessage().key("1").value("".getBytes()).send();
        producer.newMessage().key("2").value("".getBytes()).send();

        Compactor compactor = new TwoPhaseCompactor(conf, pulsarClient, bk, compactionScheduler);
        compactor.compact(topic).get();

        Set<String> expected = Sets.newHashSet("3");
        // consumer with readCompacted enabled only get compacted entries
        try (Consumer<byte[]> consumer = pulsarClient.newConsumer().topic(topic).subscriptionName("sub1")
                .readCompacted(true).subscribe()) {
            Message<byte[]> m = consumer.receive(2, TimeUnit.SECONDS);
            assertTrue(expected.remove(m.getKey()));
        }
    }
    
    @Test(timeOut = 20000)
    public void testEmptyCompactionLedger() throws Exception {
        String topic = "persistent://my-property/use/my-ns/my-topic1";

        Producer<byte[]> producer = pulsarClient.newProducer().topic(topic).enableBatching(false)
                .messageRoutingMode(MessageRoutingMode.SinglePartition).create();

        pulsarClient.newConsumer().topic(topic).subscriptionName("sub1").readCompacted(true).subscribe().close();

        producer.newMessage().key("1").value("1".getBytes()).send();
        producer.newMessage().key("2").value("2".getBytes()).send();
        producer.newMessage().key("1").value("".getBytes()).send();
        producer.newMessage().key("2").value("".getBytes()).send();

        Compactor compactor = new TwoPhaseCompactor(conf, pulsarClient, bk, compactionScheduler);
        compactor.compact(topic).get();

        // consumer with readCompacted enabled only get compacted entries
        try (Consumer<byte[]> consumer = pulsarClient.newConsumer().topic(topic).subscriptionName("sub1")
                .readCompacted(true).subscribe()) {
            Message<byte[]> m = consumer.receive(2, TimeUnit.SECONDS);
            assertNull(m);
        }
    }

}