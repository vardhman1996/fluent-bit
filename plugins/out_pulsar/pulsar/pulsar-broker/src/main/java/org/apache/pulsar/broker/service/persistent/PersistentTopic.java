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
package org.apache.pulsar.broker.service.persistent;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.commons.lang3.StringUtils.isBlank;
import static org.apache.pulsar.broker.cache.ConfigurationCacheService.POLICIES;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.bookkeeper.mledger.AsyncCallbacks;
import org.apache.bookkeeper.mledger.AsyncCallbacks.AddEntryCallback;
import org.apache.bookkeeper.mledger.AsyncCallbacks.CloseCallback;
import org.apache.bookkeeper.mledger.AsyncCallbacks.DeleteCursorCallback;
import org.apache.bookkeeper.mledger.AsyncCallbacks.OffloadCallback;
import org.apache.bookkeeper.mledger.AsyncCallbacks.OpenCursorCallback;
import org.apache.bookkeeper.mledger.AsyncCallbacks.TerminateCallback;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.ManagedCursor.IndividualDeletedEntries;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.ManagedLedgerException.ManagedLedgerAlreadyClosedException;
import org.apache.bookkeeper.mledger.ManagedLedgerException.ManagedLedgerFencedException;
import org.apache.bookkeeper.mledger.ManagedLedgerException.ManagedLedgerTerminatedException;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.impl.ManagedCursorImpl;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerImpl;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.pulsar.broker.PulsarServerException;
import org.apache.pulsar.broker.admin.AdminResource;
import org.apache.pulsar.broker.service.BrokerService;
import org.apache.pulsar.broker.service.BrokerServiceException;
import org.apache.pulsar.broker.service.BrokerServiceException.AlreadyRunningException;
import org.apache.pulsar.broker.service.BrokerServiceException.ConsumerBusyException;
import org.apache.pulsar.broker.service.BrokerServiceException.NamingException;
import org.apache.pulsar.broker.service.BrokerServiceException.NotAllowedException;
import org.apache.pulsar.broker.service.BrokerServiceException.PersistenceException;
import org.apache.pulsar.broker.service.BrokerServiceException.ProducerBusyException;
import org.apache.pulsar.broker.service.BrokerServiceException.ServerMetadataException;
import org.apache.pulsar.broker.service.BrokerServiceException.SubscriptionBusyException;
import org.apache.pulsar.broker.service.BrokerServiceException.TopicBusyException;
import org.apache.pulsar.broker.service.BrokerServiceException.TopicClosedException;
import org.apache.pulsar.broker.service.BrokerServiceException.TopicFencedException;
import org.apache.pulsar.broker.service.BrokerServiceException.TopicTerminatedException;
import org.apache.pulsar.broker.service.BrokerServiceException.UnsupportedVersionException;
import org.apache.pulsar.broker.service.Consumer;
import org.apache.pulsar.broker.service.Producer;
import org.apache.pulsar.broker.service.Replicator;
import org.apache.pulsar.broker.service.ServerCnx;
import org.apache.pulsar.broker.service.StreamingStats;
import org.apache.pulsar.broker.service.Subscription;
import org.apache.pulsar.broker.service.Topic;
import org.apache.pulsar.broker.stats.ClusterReplicationMetrics;
import org.apache.pulsar.broker.stats.NamespaceStats;
import org.apache.pulsar.broker.stats.ReplicationMetrics;
import org.apache.pulsar.client.admin.LongRunningProcessStatus;
import org.apache.pulsar.client.admin.OffloadProcessStatus;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.impl.BatchMessageIdImpl;
import org.apache.pulsar.client.impl.MessageIdImpl;
import org.apache.pulsar.client.impl.MessageImpl;
import org.apache.pulsar.common.api.proto.PulsarApi.CommandSubscribe.InitialPosition;
import org.apache.pulsar.common.api.proto.PulsarApi.CommandSubscribe.SubType;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.BacklogQuota;
import org.apache.pulsar.common.policies.data.ConsumerStats;
import org.apache.pulsar.common.policies.data.PersistentTopicInternalStats;
import org.apache.pulsar.common.policies.data.PersistentTopicInternalStats.CursorStats;
import org.apache.pulsar.common.policies.data.PersistentTopicInternalStats.LedgerInfo;
import org.apache.pulsar.common.policies.data.TopicStats;
import org.apache.pulsar.common.policies.data.Policies;
import org.apache.pulsar.common.policies.data.PublisherStats;
import org.apache.pulsar.common.policies.data.ReplicatorStats;
import org.apache.pulsar.common.policies.data.SubscriptionStats;
import org.apache.pulsar.common.schema.SchemaData;
import org.apache.pulsar.common.schema.SchemaVersion;
import org.apache.pulsar.common.util.Codec;
import org.apache.pulsar.common.util.DateFormatter;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.common.util.collections.ConcurrentOpenHashMap;
import org.apache.pulsar.common.util.collections.ConcurrentOpenHashSet;
import org.apache.pulsar.compaction.CompactedTopic;
import org.apache.pulsar.compaction.CompactedTopicImpl;
import org.apache.pulsar.compaction.Compactor;
import org.apache.pulsar.policies.data.loadbalancer.NamespaceBundleStats;
import org.apache.pulsar.utils.StatsOutputStream;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.carrotsearch.hppc.ObjectObjectHashMap;
import com.google.common.base.MoreObjects;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import io.netty.buffer.ByteBuf;
import io.netty.util.concurrent.FastThreadLocal;

public class PersistentTopic implements Topic, AddEntryCallback {
    private final String topic;

    // Managed ledger associated with the topic
    private final ManagedLedger ledger;

    // Producers currently connected to this topic
    private final ConcurrentOpenHashSet<Producer> producers;

    // Subscriptions to this topic
    private final ConcurrentOpenHashMap<String, PersistentSubscription> subscriptions;

    private final ConcurrentOpenHashMap<String, Replicator> replicators;

    private final BrokerService brokerService;

    private volatile boolean isFenced;

    protected static final AtomicLongFieldUpdater<PersistentTopic> USAGE_COUNT_UPDATER =
            AtomicLongFieldUpdater.newUpdater(PersistentTopic.class, "usageCount");
    @SuppressWarnings("unused")
    private volatile long usageCount = 0;

    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    // Prefix for replication cursors
    public final String replicatorPrefix;

    static final String DEDUPLICATION_CURSOR_NAME = "pulsar.dedup";

    private static final double MESSAGE_EXPIRY_THRESHOLD = 1.5;

    private static final long POLICY_UPDATE_FAILURE_RETRY_TIME_SECONDS = 60;

    // Timestamp of when this topic was last seen active
    private volatile long lastActive;

    // Flag to signal that producer of this topic has published batch-message so, broker should not allow consumer which
    // doesn't support batch-message
    private volatile boolean hasBatchMessagePublished = false;
    private final DispatchRateLimiter dispatchRateLimiter;
    public static final int MESSAGE_RATE_BACKOFF_MS = 1000;

    private final MessageDeduplication messageDeduplication;

    private static final long COMPACTION_NEVER_RUN = -0xfebecffeL;
    CompletableFuture<Long> currentCompaction = CompletableFuture.completedFuture(COMPACTION_NEVER_RUN);
    final CompactedTopic compactedTopic;

    CompletableFuture<MessageIdImpl> currentOffload = CompletableFuture.completedFuture(
            (MessageIdImpl)MessageId.earliest);

    // Whether messages published must be encrypted or not in this topic
    private volatile boolean isEncryptionRequired = false;

    private static final FastThreadLocal<TopicStatsHelper> threadLocalTopicStats = new FastThreadLocal<TopicStatsHelper>() {
        @Override
        protected TopicStatsHelper initialValue() {
            return new TopicStatsHelper();
        }
    };

    private static class TopicStatsHelper {
        public double averageMsgSize;
        public double aggMsgRateIn;
        public double aggMsgThroughputIn;
        public double aggMsgRateOut;
        public double aggMsgThroughputOut;
        public final ObjectObjectHashMap<String, PublisherStats> remotePublishersStats;

        public TopicStatsHelper() {
            remotePublishersStats = new ObjectObjectHashMap<String, PublisherStats>();
            reset();
        }

        public void reset() {
            averageMsgSize = 0;
            aggMsgRateIn = 0;
            aggMsgThroughputIn = 0;
            aggMsgRateOut = 0;
            aggMsgThroughputOut = 0;
            remotePublishersStats.clear();
        }
    }

    public PersistentTopic(String topic, ManagedLedger ledger, BrokerService brokerService) throws NamingException {
        this.topic = topic;
        this.ledger = ledger;
        this.brokerService = brokerService;
        this.producers = new ConcurrentOpenHashSet<Producer>(16, 1);
        this.subscriptions = new ConcurrentOpenHashMap<>(16, 1);
        this.replicators = new ConcurrentOpenHashMap<>(16, 1);
        this.isFenced = false;
        this.replicatorPrefix = brokerService.pulsar().getConfiguration().getReplicatorPrefix();
        USAGE_COUNT_UPDATER.set(this, 0);

        this.dispatchRateLimiter = new DispatchRateLimiter(this);

        this.compactedTopic = new CompactedTopicImpl(brokerService.pulsar().getBookKeeperClient());

        for (ManagedCursor cursor : ledger.getCursors()) {
            if (cursor.getName().startsWith(replicatorPrefix)) {
                String localCluster = brokerService.pulsar().getConfiguration().getClusterName();
                String remoteCluster = PersistentReplicator.getRemoteCluster(cursor.getName());
                boolean isReplicatorStarted = addReplicationCluster(remoteCluster, this, cursor, localCluster);
                if (!isReplicatorStarted) {
                    throw new NamingException(
                            PersistentTopic.this.getName() + " Failed to start replicator " + remoteCluster);
                }
            } else if (cursor.getName().equals(DEDUPLICATION_CURSOR_NAME)) {
                // This is not a regular subscription, we are going to ignore it for now and let the message dedup logic
                // to take care of it
            } else {
                final String subscriptionName = Codec.decode(cursor.getName());
                subscriptions.put(subscriptionName, createPersistentSubscription(subscriptionName, cursor));
                // subscription-cursor gets activated by default: deactivate as there is no active subscription right
                // now
                subscriptions.get(subscriptionName).deactivateCursor();
            }
        }
        this.lastActive = System.nanoTime();

        this.messageDeduplication = new MessageDeduplication(brokerService.pulsar(), this, ledger);

        try {
            Policies policies = brokerService.pulsar().getConfigurationCache().policiesCache()
                    .get(AdminResource.path(POLICIES, TopicName.get(topic).getNamespace()))
                    .orElseThrow(() -> new KeeperException.NoNodeException());
            isEncryptionRequired = policies.encryption_required;
        } catch (Exception e) {
            log.warn("[{}] Error getting policies {} and isEncryptionRequired will be set to false", topic, e.getMessage());
            isEncryptionRequired = false;
        }
    }

    private PersistentSubscription createPersistentSubscription(String subscriptionName, ManagedCursor cursor) {
        checkNotNull(compactedTopic);
        if (subscriptionName.equals(Compactor.COMPACTION_SUBSCRIPTION)) {
            return new CompactorSubscription(this, compactedTopic, subscriptionName, cursor);
        } else {
            return new PersistentSubscription(this, subscriptionName, cursor);
        }
    }

    @Override
    public void publishMessage(ByteBuf headersAndPayload, PublishContext publishContext) {
        if (messageDeduplication.shouldPublishNextMessage(publishContext, headersAndPayload)) {
            ledger.asyncAddEntry(headersAndPayload, this, publishContext);
        } else {
            // Immediately acknowledge duplicated message
            publishContext.completed(null, -1, -1);
        }
    }

    @Override
    public void addComplete(Position pos, Object ctx) {
        PublishContext publishContext = (PublishContext) ctx;
        PositionImpl position = (PositionImpl) pos;

        // Message has been successfully persisted
        messageDeduplication.recordMessagePersisted(publishContext, position);
        publishContext.completed(null, position.getLedgerId(), position.getEntryId());
    }

    @Override
    public void addFailed(ManagedLedgerException exception, Object ctx) {
        PublishContext callback = (PublishContext) ctx;

        if (exception instanceof ManagedLedgerAlreadyClosedException) {
            if (log.isDebugEnabled()) {
                log.debug("[{}] Failed to persist msg in store: {}", topic, exception.getMessage());
            }

            callback.completed(new TopicClosedException(exception), -1, -1);
            return;

        } else {
            log.warn("[{}] Failed to persist msg in store: {}", topic, exception.getMessage());
        }

        if (exception instanceof ManagedLedgerTerminatedException) {
            // Signal the producer that this topic is no longer available
            callback.completed(new TopicTerminatedException(exception), -1, -1);
        } else {
            // Use generic persistence exception
            callback.completed(new PersistenceException(exception), -1, -1);
        }

        if (exception instanceof ManagedLedgerFencedException) {
            // If the managed ledger has been fenced, we cannot continue using it. We need to close and reopen
            close();
        }
    }

    @Override
    public void addProducer(Producer producer) throws BrokerServiceException {
        checkArgument(producer.getTopic() == this);

        lock.readLock().lock();
        try {
            if (isFenced) {
                log.warn("[{}] Attempting to add producer to a fenced topic", topic);
                throw new TopicFencedException("Topic is temporarily unavailable");
            }

            if (ledger.isTerminated()) {
                log.warn("[{}] Attempting to add producer to a terminated topic", topic);
                throw new TopicTerminatedException("Topic was already terminated");
            }

            if (isProducersExceeded()) {
                log.warn("[{}] Attempting to add producer to topic which reached max producers limit", topic);
                throw new ProducerBusyException("Topic reached max producers limit");
            }

            if (log.isDebugEnabled()) {
                log.debug("[{}] {} Got request to create producer ", topic, producer.getProducerName());
            }

            if (!producers.add(producer)) {
                throw new NamingException(
                        "Producer with name '" + producer.getProducerName() + "' is already connected to topic");
            }

            USAGE_COUNT_UPDATER.incrementAndGet(this);
            if (log.isDebugEnabled()) {
                log.debug("[{}] [{}] Added producer -- count: {}", topic, producer.getProducerName(), USAGE_COUNT_UPDATER.get(this));
            }

            messageDeduplication.producerAdded(producer.getProducerName());

            // Start replication producers if not already
            startReplProducers();
        } finally {
            lock.readLock().unlock();
        }
    }

    private boolean isProducersExceeded() {
        Policies policies;
        try {
            policies =  brokerService.pulsar().getConfigurationCache().policiesCache()
                    .get(AdminResource.path(POLICIES, TopicName.get(topic).getNamespace()))
                    .orElseGet(() -> new Policies());
        } catch (Exception e) {
            policies = new Policies();
        }
        final int maxProducers = policies.max_producers_per_topic > 0 ?
                policies.max_producers_per_topic :
                brokerService.pulsar().getConfiguration().getMaxProducersPerTopic();
        if (maxProducers > 0 && maxProducers <= producers.size()) {
            return true;
        }
        return false;
    }

    private boolean hasLocalProducers() {
        AtomicBoolean foundLocal = new AtomicBoolean(false);
        producers.forEach(producer -> {
            if (!producer.isRemote()) {
                foundLocal.set(true);
            }
        });

        return foundLocal.get();
    }

    private boolean hasRemoteProducers() {
        AtomicBoolean foundRemote = new AtomicBoolean(false);
        producers.forEach(producer -> {
            if (producer.isRemote()) {
                foundRemote.set(true);
            }
        });

        return foundRemote.get();
    }

    public void startReplProducers() {
        // read repl-cluster from policies to avoid restart of replicator which are in process of disconnect and close
        try {
            Policies policies = brokerService.pulsar().getConfigurationCache().policiesCache()
                    .get(AdminResource.path(POLICIES, TopicName.get(topic).getNamespace()))
                    .orElseThrow(() -> new KeeperException.NoNodeException());
            if (policies.replication_clusters != null) {
                Set<String> configuredClusters = Sets.newTreeSet(policies.replication_clusters);
                replicators.forEach((region, replicator) -> {
                    if (configuredClusters.contains(region)) {
                        replicator.startProducer();
                    }
                });
            }
        } catch (Exception e) {
            if (log.isDebugEnabled()) {
                log.debug("[{}] Error getting policies while starting repl-producers {}", topic, e.getMessage());
            }
            replicators.forEach((region, replicator) -> replicator.startProducer());
        }
    }

    public CompletableFuture<Void> stopReplProducers() {
        List<CompletableFuture<Void>> closeFutures = Lists.newArrayList();
        replicators.forEach((region, replicator) -> closeFutures.add(replicator.disconnect()));
        return FutureUtil.waitForAll(closeFutures);
    }

    private synchronized CompletableFuture<Void> closeReplProducersIfNoBacklog() {
        List<CompletableFuture<Void>> closeFutures = Lists.newArrayList();
        replicators.forEach((region, replicator) -> closeFutures.add(replicator.disconnect(true)));
        return FutureUtil.waitForAll(closeFutures);
    }

    @Override
    public void removeProducer(Producer producer) {
        checkArgument(producer.getTopic() == this);
        if (producers.remove(producer)) {
            // decrement usage only if this was a valid producer close
            USAGE_COUNT_UPDATER.decrementAndGet(this);
            if (log.isDebugEnabled()) {
                log.debug("[{}] [{}] Removed producer -- count: {}", topic, producer.getProducerName(),
                        USAGE_COUNT_UPDATER.get(this));
            }
            lastActive = System.nanoTime();

            messageDeduplication.producerRemoved(producer.getProducerName());
        }
    }

    @Override
    public CompletableFuture<Consumer> subscribe(final ServerCnx cnx, String subscriptionName, long consumerId,
            SubType subType, int priorityLevel, String consumerName, boolean isDurable, MessageId startMessageId,
            Map<String, String> metadata, boolean readCompacted, InitialPosition initialPosition) {

        final CompletableFuture<Consumer> future = new CompletableFuture<>();

        if (readCompacted && !(subType == SubType.Failover || subType == SubType.Exclusive)) {
            future.completeExceptionally(
                    new NotAllowedException("readCompacted only allowed on failover or exclusive subscriptions"));
            return future;
        }
        if (isBlank(subscriptionName)) {
            if (log.isDebugEnabled()) {
                log.debug("[{}] Empty subscription name", topic);
            }
            future.completeExceptionally(new NamingException("Empty subscription name"));
            return future;
        }

        if (hasBatchMessagePublished && !cnx.isBatchMessageCompatibleVersion()) {
            if (log.isDebugEnabled()) {
                log.debug("[{}] Consumer doesn't support batch-message {}", topic, subscriptionName);
            }
            future.completeExceptionally(new UnsupportedVersionException("Consumer doesn't support batch-message"));
            return future;
        }

        if (subscriptionName.startsWith(replicatorPrefix) || subscriptionName.equals(DEDUPLICATION_CURSOR_NAME)) {
            log.warn("[{}] Failed to create subscription for {}", topic, subscriptionName);
            future.completeExceptionally(new NamingException("Subscription with reserved subscription name attempted"));
            return future;
        }

        lock.readLock().lock();
        try {
            if (isFenced) {
                log.warn("[{}] Attempting to subscribe to a fenced topic", topic);
                future.completeExceptionally(new TopicFencedException("Topic is temporarily unavailable"));
                return future;
            }
            USAGE_COUNT_UPDATER.incrementAndGet(this);
            if (log.isDebugEnabled()) {
                log.debug("[{}] [{}] [{}] Added consumer -- count: {}", topic, subscriptionName, consumerName,
                        USAGE_COUNT_UPDATER.get(this));
            }
        } finally {
            lock.readLock().unlock();
        }

        CompletableFuture<? extends Subscription> subscriptionFuture = isDurable ? //
                getDurableSubscription(subscriptionName, initialPosition) //
                : getNonDurableSubscription(subscriptionName, startMessageId);

        int maxUnackedMessages  = isDurable ? brokerService.pulsar().getConfiguration().getMaxUnackedMessagesPerConsumer() :0;

        subscriptionFuture.thenAccept(subscription -> {
            try {
                Consumer consumer = new Consumer(subscription, subType, topic, consumerId, priorityLevel, consumerName,
                                                 maxUnackedMessages, cnx, cnx.getRole(), metadata, readCompacted, initialPosition);
                subscription.addConsumer(consumer);
                if (!cnx.isActive()) {
                    consumer.close();
                    if (log.isDebugEnabled()) {
                        log.debug("[{}] [{}] [{}] Subscribe failed -- count: {}", topic, subscriptionName,
                                consumer.consumerName(), USAGE_COUNT_UPDATER.get(PersistentTopic.this));
                    }
                    future.completeExceptionally(
                            new BrokerServiceException("Connection was closed while the opening the cursor "));
                } else {
                    log.info("[{}][{}] Created new subscription for {}", topic, subscriptionName, consumerId);
                    future.complete(consumer);
                }
            } catch (BrokerServiceException e) {
                if (e instanceof ConsumerBusyException) {
                    log.warn("[{}][{}] Consumer {} {} already connected", topic, subscriptionName, consumerId,
                            consumerName);
                } else if (e instanceof SubscriptionBusyException) {
                    log.warn("[{}][{}] {}", topic, subscriptionName, e.getMessage());
                }

                USAGE_COUNT_UPDATER.decrementAndGet(PersistentTopic.this);
                future.completeExceptionally(e);
            }
        }).exceptionally(ex -> {
            log.warn("[{}] Failed to create subscription for {}: ", topic, subscriptionName, ex.getMessage());
            USAGE_COUNT_UPDATER.decrementAndGet(PersistentTopic.this);
            future.completeExceptionally(new PersistenceException(ex));
            return null;
        });

        return future;
    }

    private CompletableFuture<Subscription> getDurableSubscription(String subscriptionName, InitialPosition initialPosition) {
        CompletableFuture<Subscription> subscriptionFuture = new CompletableFuture<>();
        ledger.asyncOpenCursor(Codec.encode(subscriptionName), initialPosition, new OpenCursorCallback() {
            @Override
            public void openCursorComplete(ManagedCursor cursor, Object ctx) {
                if (log.isDebugEnabled()) {
                    log.debug("[{}][{}] Opened cursor", topic, subscriptionName);
                }

                subscriptionFuture.complete(subscriptions.computeIfAbsent(subscriptionName,
                        name -> createPersistentSubscription(subscriptionName, cursor)));
            }

            @Override
            public void openCursorFailed(ManagedLedgerException exception, Object ctx) {
                log.warn("[{}] Failed to create subscription for {}: {}", topic, subscriptionName, exception.getMessage());
                USAGE_COUNT_UPDATER.decrementAndGet(PersistentTopic.this);
                subscriptionFuture.completeExceptionally(new PersistenceException(exception));
                if (exception instanceof ManagedLedgerFencedException) {
                    // If the managed ledger has been fenced, we cannot continue using it. We need to close and reopen
                    close();
                }
            }
        }, null);
        return subscriptionFuture;
    }

    private CompletableFuture<? extends Subscription> getNonDurableSubscription(String subscriptionName, MessageId startMessageId) {
        CompletableFuture<Subscription> subscriptionFuture = new CompletableFuture<>();
        log.info("[{}][{}] Creating non-durable subscription at msg id {}", topic, subscriptionName, startMessageId);

        // Create a new non-durable cursor only for the first consumer that connects
        Subscription subscription = subscriptions.computeIfAbsent(subscriptionName, name -> {
            MessageIdImpl msgId = startMessageId != null ? (MessageIdImpl) startMessageId
                    : (MessageIdImpl) MessageId.latest;

            long ledgerId = msgId.getLedgerId();
            long entryId = msgId.getEntryId();
            if (msgId instanceof BatchMessageIdImpl) {
                // When the start message is relative to a batch, we need to take one step back on the previous message,
                // because the "batch" might not have been consumed in its entirety.
                // The client will then be able to discard the first messages in the batch.
                if (((BatchMessageIdImpl) msgId).getBatchIndex() >= 0) {
                    entryId = msgId.getEntryId() - 1;
                }
            }
            Position startPosition = new PositionImpl(ledgerId, entryId);
            ManagedCursor cursor = null;
            try {
                cursor = ledger.newNonDurableCursor(startPosition);
            } catch (ManagedLedgerException e) {
                subscriptionFuture.completeExceptionally(e);
            }

            return new PersistentSubscription(this, subscriptionName, cursor);
        });

        if (!subscriptionFuture.isDone()) {
            subscriptionFuture.complete(subscription);
        } else {
            // failed to initialize managed-cursor: clean up created subscription
            subscriptions.remove(subscriptionName);
        }

        return subscriptionFuture;
    }

    @SuppressWarnings("unchecked")
    @Override
    public CompletableFuture<Subscription> createSubscription(String subscriptionName, InitialPosition initialPosition) {
        return getDurableSubscription(subscriptionName, initialPosition);
    }

    /**
     * Delete the cursor ledger for a given subscription
     *
     * @param subscriptionName
     *            Subscription for which the cursor ledger is to be deleted
     * @return Completable future indicating completion of unsubscribe operation Completed exceptionally with:
     *         ManagedLedgerException if cursor ledger delete fails
     */
    @Override
    public CompletableFuture<Void> unsubscribe(String subscriptionName) {
        CompletableFuture<Void> unsubscribeFuture = new CompletableFuture<>();

        ledger.asyncDeleteCursor(Codec.encode(subscriptionName), new DeleteCursorCallback() {
            @Override
            public void deleteCursorComplete(Object ctx) {
                if (log.isDebugEnabled()) {
                    log.debug("[{}][{}] Cursor deleted successfully", topic, subscriptionName);
                }
                subscriptions.remove(subscriptionName);
                unsubscribeFuture.complete(null);
                lastActive = System.nanoTime();
            }

            @Override
            public void deleteCursorFailed(ManagedLedgerException exception, Object ctx) {
                if (log.isDebugEnabled()) {
                    log.debug("[{}][{}] Error deleting cursor for subscription", topic, subscriptionName, exception);
                }
                unsubscribeFuture.completeExceptionally(new PersistenceException(exception));
            }
        }, null);

        return unsubscribeFuture;
    }

    void removeSubscription(String subscriptionName) {
        subscriptions.remove(subscriptionName);
    }

    /**
     * Delete the managed ledger associated with this topic
     *
     * @return Completable future indicating completion of delete operation Completed exceptionally with:
     *         IllegalStateException if topic is still active ManagedLedgerException if ledger delete operation fails
     */
    @Override
    public CompletableFuture<Void> delete() {
        return delete(false);
    }

    private CompletableFuture<Void> delete(boolean failIfHasSubscriptions) {
        return delete(failIfHasSubscriptions, false);
    }

    /**
     * Forcefully close all producers/consumers/replicators and deletes the topic. this function is used when local
     * cluster is removed from global-namespace replication list. Because broker doesn't allow lookup if local cluster
     * is not part of replication cluster list.
     *
     * @return
     */
    @Override
    public CompletableFuture<Void> deleteForcefully() {
        return delete(false, true);
    }

    /**
     * Delete the managed ledger associated with this topic
     *
     * @param failIfHasSubscriptions
     *            Flag indicating whether delete should succeed if topic still has unconnected subscriptions. Set to
     *            false when called from admin API (it will delete the subs too), and set to true when called from GC
     *            thread
     * @param closeIfClientsConnected
     *            Flag indicate whether explicitly close connected producers/consumers/replicators before trying to delete topic. If
     *            any client is connected to a topic and if this flag is disable then this operation fails.
     *
     * @return Completable future indicating completion of delete operation Completed exceptionally with:
     *         IllegalStateException if topic is still active ManagedLedgerException if ledger delete operation fails
     */
    private CompletableFuture<Void> delete(boolean failIfHasSubscriptions, boolean closeIfClientsConnected) {
        CompletableFuture<Void> deleteFuture = new CompletableFuture<>();

        lock.writeLock().lock();
        try {
            if (isFenced) {
                log.warn("[{}] Topic is already being closed or deleted", topic);
                deleteFuture.completeExceptionally(new TopicFencedException("Topic is already fenced"));
                return deleteFuture;
            }

            CompletableFuture<Void> closeClientFuture = new CompletableFuture<>();
            if (closeIfClientsConnected) {
                List<CompletableFuture<Void>> futures = Lists.newArrayList();
                replicators.forEach((cluster, replicator) -> futures.add(replicator.disconnect()));
                producers.forEach(producer -> futures.add(producer.disconnect()));
                subscriptions.forEach((s, sub) -> futures.add(sub.disconnect()));
                FutureUtil.waitForAll(futures).thenRun(() -> {
                    closeClientFuture.complete(null);
                }).exceptionally(ex -> {
                    log.error("[{}] Error closing clients", topic, ex);
                    isFenced = false;
                    closeClientFuture.completeExceptionally(ex);
                    return null;
                });
            } else {
                closeClientFuture.complete(null);
            }

            closeClientFuture.thenAccept(delete -> {
                if (USAGE_COUNT_UPDATER.get(this) == 0) {
                    isFenced = true;

                    List<CompletableFuture<Void>> futures = Lists.newArrayList();

                    if (failIfHasSubscriptions) {
                        if (!subscriptions.isEmpty()) {
                            isFenced = false;
                            deleteFuture.completeExceptionally(new TopicBusyException("Topic has subscriptions"));
                            return;
                        }
                    } else {
                        subscriptions.forEach((s, sub) -> futures.add(sub.delete()));
                    }

                    FutureUtil.waitForAll(futures).whenComplete((v, ex) -> {
                        if (ex != null) {
                            log.error("[{}] Error deleting topic", topic, ex);
                            isFenced = false;
                            deleteFuture.completeExceptionally(ex);
                        } else {
                            ledger.asyncDelete(new AsyncCallbacks.DeleteLedgerCallback() {
                                @Override
                                public void deleteLedgerComplete(Object ctx) {
                                    brokerService.removeTopicFromCache(topic);
                                    log.info("[{}] Topic deleted", topic);
                                    deleteFuture.complete(null);
                                }

                                @Override
                                public void deleteLedgerFailed(ManagedLedgerException exception, Object ctx) {
                                    isFenced = false;
                                    log.error("[{}] Error deleting topic", topic, exception);
                                    deleteFuture.completeExceptionally(new PersistenceException(exception));
                                }
                            }, null);
                        }
                    });
                } else {
                    deleteFuture.completeExceptionally(new TopicBusyException(
                            "Topic has " + USAGE_COUNT_UPDATER.get(this) + " connected producers/consumers"));
                }
            }).exceptionally(ex->{
                deleteFuture.completeExceptionally(
                        new TopicBusyException("Failed to close clients before deleting topic."));
                return null;
            });
        } finally {
            lock.writeLock().unlock();
        }

        return deleteFuture;
    }

    /**
     * Close this topic - close all producers and subscriptions associated with this topic
     *
     * @return Completable future indicating completion of close operation
     */
    @Override
    public CompletableFuture<Void> close() {
        CompletableFuture<Void> closeFuture = new CompletableFuture<>();

        lock.writeLock().lock();
        try {
            if (!isFenced) {
                isFenced = true;
            } else {
                log.warn("[{}] Topic is already being closed or deleted", topic);
                closeFuture.completeExceptionally(new TopicFencedException("Topic is already fenced"));
                return closeFuture;
            }
        } finally {
            lock.writeLock().unlock();
        }

        List<CompletableFuture<Void>> futures = Lists.newArrayList();

        replicators.forEach((cluster, replicator) -> futures.add(replicator.disconnect()));
        producers.forEach(producer -> futures.add(producer.disconnect()));
        subscriptions.forEach((s, sub) -> futures.add(sub.disconnect()));

        FutureUtil.waitForAll(futures).thenRun(() -> {
            // After having disconnected all producers/consumers, close the managed ledger
            ledger.asyncClose(new CloseCallback() {
                @Override
                public void closeComplete(Object ctx) {
                    // Everything is now closed, remove the topic from map
                    brokerService.removeTopicFromCache(topic);

                    log.info("[{}] Topic closed", topic);
                    closeFuture.complete(null);
                }

                @Override
                public void closeFailed(ManagedLedgerException exception, Object ctx) {
                    log.error("[{}] Failed to close managed ledger, proceeding anyway.", topic, exception);
                    brokerService.removeTopicFromCache(topic);
                    closeFuture.complete(null);
                }
            }, null);

            dispatchRateLimiter.close();

        }).exceptionally(exception -> {
            log.error("[{}] Error closing topic", topic, exception);
            isFenced = false;
            closeFuture.completeExceptionally(exception);
            return null;
        });

        return closeFuture;
    }

    private CompletableFuture<Void> checkReplicationAndRetryOnFailure() {
        CompletableFuture<Void> result = new CompletableFuture<Void>();
        checkReplication().thenAccept(res -> {
            log.info("[{}] Policies updated successfully", topic);
            result.complete(null);
        }).exceptionally(th -> {
            log.error("[{}] Policies update failed {}, scheduled retry in {} seconds", topic, th.getMessage(),
                    POLICY_UPDATE_FAILURE_RETRY_TIME_SECONDS, th);
            brokerService.executor().schedule(this::checkReplicationAndRetryOnFailure,
                    POLICY_UPDATE_FAILURE_RETRY_TIME_SECONDS, TimeUnit.SECONDS);
            result.completeExceptionally(th);
            return null;
        });
        return result;
    }

    public CompletableFuture<Void> checkDeduplicationStatus() {
        return messageDeduplication.checkStatus();
    }

    private CompletableFuture<Void> checkPersistencePolicies() {
        TopicName topicName = TopicName.get(topic);
        CompletableFuture<Void> future = new CompletableFuture<>();
        brokerService.getManagedLedgerConfig(topicName).thenAccept(config -> {
            // update managed-ledger config and managed-cursor.markDeleteRate
            this.ledger.setConfig(config);
            future.complete(null);
        }).exceptionally(ex -> {
            log.warn("[{}] Failed to update persistence-policies {}", topic, ex.getMessage());
            future.completeExceptionally(ex);
            return null;
        });
        return future;
    }

    @Override
    public CompletableFuture<Void> checkReplication() {
        TopicName name = TopicName.get(topic);
        if (!name.isGlobal()) {
            return CompletableFuture.completedFuture(null);
        }

        if (log.isDebugEnabled()) {
            log.debug("[{}] Checking replication status", name);
        }

        Policies policies = null;
        try {
            policies = brokerService.pulsar().getConfigurationCache().policiesCache()
                    .get(AdminResource.path(POLICIES, name.getNamespace()))
                    .orElseThrow(() -> new KeeperException.NoNodeException());
        } catch (Exception e) {
            CompletableFuture<Void> future = new CompletableFuture<>();
            future.completeExceptionally(new ServerMetadataException(e));
            return future;
        }

        final int newMessageTTLinSeconds = policies.message_ttl_in_seconds;

        Set<String> configuredClusters;
        if (policies.replication_clusters != null) {
            configuredClusters = Sets.newTreeSet(policies.replication_clusters);
        } else {
            configuredClusters = Collections.emptySet();
        }

        String localCluster = brokerService.pulsar().getConfiguration().getClusterName();

        // if local cluster is removed from global namespace cluster-list : then delete topic forcefully because pulsar
        // doesn't serve global topic without local repl-cluster configured.
        if (TopicName.get(topic).isGlobal() && !configuredClusters.contains(localCluster)) {
            log.info("Deleting topic [{}] because local cluster is not part of global namespace repl list {}",
                    configuredClusters);
            return deleteForcefully();
        }

        List<CompletableFuture<Void>> futures = Lists.newArrayList();

        // Check for missing replicators
        for (String cluster : configuredClusters) {
            if (cluster.equals(localCluster)) {
                continue;
            }

            if (!replicators.containsKey(cluster)) {
                futures.add(startReplicator(cluster));
            }
        }

        // Check for replicators to be stopped
        replicators.forEach((cluster, replicator) -> {
            // Update message TTL
            ((PersistentReplicator) replicator).updateMessageTTL(newMessageTTLinSeconds);

            if (!cluster.equals(localCluster)) {
                if (!configuredClusters.contains(cluster)) {
                    futures.add(removeReplicator(cluster));
                }
            }

        });

        return FutureUtil.waitForAll(futures);
    }

    @Override
    public void checkMessageExpiry() {
        TopicName name = TopicName.get(topic);
        Policies policies;
        try {
            policies = brokerService.pulsar().getConfigurationCache().policiesCache()
                    .get(AdminResource.path(POLICIES, name.getNamespace()))
                    .orElseThrow(() -> new KeeperException.NoNodeException());
            if (policies.message_ttl_in_seconds != 0) {
                subscriptions.forEach((subName, sub) -> sub.expireMessages(policies.message_ttl_in_seconds));
                replicators.forEach((region, replicator) -> ((PersistentReplicator)replicator).expireMessages(policies.message_ttl_in_seconds));
            }
        } catch (Exception e) {
            if (log.isDebugEnabled()) {
                log.debug("[{}] Error getting policies", topic);
            }
        }
    }

    @Override
    public void checkMessageDeduplicationInfo() {
        messageDeduplication.purgeInactiveProducers();
    }

    public void checkCompaction() {
        TopicName name = TopicName.get(topic);
        try {
            Policies policies = brokerService.pulsar().getConfigurationCache().policiesCache()
                .get(AdminResource.path(POLICIES, name.getNamespace()))
                .orElseThrow(() -> new KeeperException.NoNodeException());


            if (policies.compaction_threshold != 0
                && currentCompaction.isDone()) {

                long backlogEstimate = 0;

                PersistentSubscription compactionSub = subscriptions.get(Compactor.COMPACTION_SUBSCRIPTION);
                if (compactionSub != null) {
                    backlogEstimate = compactionSub.estimateBacklogSize();
                } else {
                    // compaction has never run, so take full backlog size
                    backlogEstimate = ledger.getEstimatedBacklogSize();
                }

                if (backlogEstimate > policies.compaction_threshold) {
                    try {
                        triggerCompaction();
                    } catch (AlreadyRunningException are) {
                        log.debug("[{}] Compaction already running, so don't trigger again, "
                                  + "even though backlog({}) is over threshold({})",
                                  name, backlogEstimate, policies.compaction_threshold);
                    }
                }
            }
        } catch (Exception e) {
            log.debug("[{}] Error getting policies", topic);
        }
    }

    CompletableFuture<Void> startReplicator(String remoteCluster) {
        log.info("[{}] Starting replicator to remote: {}", topic, remoteCluster);
        final CompletableFuture<Void> future = new CompletableFuture<>();

        String name = PersistentReplicator.getReplicatorName(replicatorPrefix, remoteCluster);
        ledger.asyncOpenCursor(name, new OpenCursorCallback() {
            @Override
            public void openCursorComplete(ManagedCursor cursor, Object ctx) {
                String localCluster = brokerService.pulsar().getConfiguration().getClusterName();
                boolean isReplicatorStarted = addReplicationCluster(remoteCluster, PersistentTopic.this, cursor, localCluster);
                if (isReplicatorStarted) {
                    future.complete(null);
                } else {
                    future.completeExceptionally(new NamingException(
                            PersistentTopic.this.getName() + " Failed to start replicator " + remoteCluster));
                }
            }

            @Override
            public void openCursorFailed(ManagedLedgerException exception, Object ctx) {
                future.completeExceptionally(new PersistenceException(exception));
            }

        }, null);

        return future;
    }

    protected boolean addReplicationCluster(String remoteCluster, PersistentTopic persistentTopic, ManagedCursor cursor,
            String localCluster) {
        AtomicBoolean isReplicatorStarted = new AtomicBoolean(true);
        replicators.computeIfAbsent(remoteCluster, r -> {
            try {
                return new PersistentReplicator(PersistentTopic.this, cursor, localCluster, remoteCluster,
                        brokerService);
            } catch (NamingException e) {
                isReplicatorStarted.set(false);
                log.error("[{}] Replicator startup failed due to partitioned-topic {}", topic, remoteCluster);
            }
            return null;
        });
        // clean up replicator if startup is failed
        if (!isReplicatorStarted.get()) {
            replicators.remove(remoteCluster);
        }
        return isReplicatorStarted.get();
    }

    CompletableFuture<Void> removeReplicator(String remoteCluster) {
        log.info("[{}] Removing replicator to {}", topic, remoteCluster);
        final CompletableFuture<Void> future = new CompletableFuture<>();

        String name = PersistentReplicator.getReplicatorName(replicatorPrefix, remoteCluster);

        replicators.get(remoteCluster).disconnect().thenRun(() -> {

            ledger.asyncDeleteCursor(name, new DeleteCursorCallback() {
                @Override
                public void deleteCursorComplete(Object ctx) {
                    replicators.remove(remoteCluster);
                    future.complete(null);
                }

                @Override
                public void deleteCursorFailed(ManagedLedgerException exception, Object ctx) {
                    log.error("[{}] Failed to delete cursor {} {}", topic, name, exception.getMessage(), exception);
                    future.completeExceptionally(new PersistenceException(exception));
                }
            }, null);

        }).exceptionally(e -> {
            log.error("[{}] Failed to close replication producer {} {}", topic, name, e.getMessage(), e);
            future.completeExceptionally(e);
            return null;
        });

        return future;
    }

    public boolean isDeduplicationEnabled() {
        return messageDeduplication.isEnabled();
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this).add("topic", topic).toString();
    }

    @Override
    public ConcurrentOpenHashSet<Producer> getProducers() {
        return producers;
    }

    public int getNumberOfConsumers() {
        int count = 0;
        for (PersistentSubscription subscription : subscriptions.values()) {
            count += subscription.getConsumers().size();
        }
        return count;
    }

    @Override
    public ConcurrentOpenHashMap<String, PersistentSubscription> getSubscriptions() {
        return subscriptions;
    }

    public PersistentSubscription getSubscription(String subscriptionName) {
        return subscriptions.get(subscriptionName);
    }

    public BrokerService getBrokerService() {
        return brokerService;
    }

    public ConcurrentOpenHashMap<String, Replicator> getReplicators() {
        return replicators;
    }

    public Replicator getPersistentReplicator(String remoteCluster) {
        return replicators.get(remoteCluster);
    }

    @Override
    public String getName() {
        return topic;
    }

    public ManagedLedger getManagedLedger() {
        return ledger;
    }

    public void updateRates(NamespaceStats nsStats, NamespaceBundleStats bundleStats, StatsOutputStream topicStatsStream,
            ClusterReplicationMetrics replStats, String namespace, boolean hydratePublishers) {

        TopicStatsHelper topicStatsHelper = threadLocalTopicStats.get();
        topicStatsHelper.reset();

        replicators.forEach((region, replicator) -> replicator.updateRates());

        nsStats.producerCount += producers.size();
        bundleStats.producerCount += producers.size();
        topicStatsStream.startObject(topic);

        // start publisher stats
        topicStatsStream.startList("publishers");
        producers.forEach(producer -> {
            producer.updateRates();
            PublisherStats publisherStats = producer.getStats();

            topicStatsHelper.aggMsgRateIn += publisherStats.msgRateIn;
            topicStatsHelper.aggMsgThroughputIn += publisherStats.msgThroughputIn;

            if (producer.isRemote()) {
                topicStatsHelper.remotePublishersStats.put(producer.getRemoteCluster(), publisherStats);
            }

            // Populate consumer specific stats here
            if (hydratePublishers) {
                StreamingStats.writePublisherStats(topicStatsStream, publisherStats);
            }
        });
        topicStatsStream.endList();

        // Start replicator stats
        topicStatsStream.startObject("replication");
        nsStats.replicatorCount += topicStatsHelper.remotePublishersStats.size();
        replicators.forEach((cluster, replicator) -> {
            // Update replicator cursor state
            try {
                ((PersistentReplicator) replicator).updateCursorState();
            } catch (Exception e) {
                log.warn("[{}] Failed to update cursro state ", topic, e);
            }


            // Update replicator stats
            ReplicatorStats rStat = replicator.getStats();

            // Add incoming msg rates
            PublisherStats pubStats = topicStatsHelper.remotePublishersStats.get(replicator.getRemoteCluster());
            if (pubStats != null) {
                rStat.msgRateIn = pubStats.msgRateIn;
                rStat.msgThroughputIn = pubStats.msgThroughputIn;
                rStat.inboundConnection = pubStats.getAddress();
                rStat.inboundConnectedSince = pubStats.getConnectedSince();
            }

            topicStatsHelper.aggMsgRateOut += rStat.msgRateOut;
            topicStatsHelper.aggMsgThroughputOut += rStat.msgThroughputOut;

            // Populate replicator specific stats here
            topicStatsStream.startObject(cluster);
            topicStatsStream.writePair("connected", rStat.connected);
            topicStatsStream.writePair("msgRateExpired", rStat.msgRateExpired);
            topicStatsStream.writePair("msgRateIn", rStat.msgRateIn);
            topicStatsStream.writePair("msgRateOut", rStat.msgRateOut);
            topicStatsStream.writePair("msgThroughputIn", rStat.msgThroughputIn);
            topicStatsStream.writePair("msgThroughputOut", rStat.msgThroughputOut);
            topicStatsStream.writePair("replicationBacklog", rStat.replicationBacklog);
            topicStatsStream.writePair("replicationDelayInSeconds", rStat.replicationDelayInSeconds);
            topicStatsStream.writePair("inboundConnection", rStat.inboundConnection);
            topicStatsStream.writePair("inboundConnectedSince", rStat.inboundConnectedSince);
            topicStatsStream.writePair("outboundConnection", rStat.outboundConnection);
            topicStatsStream.writePair("outboundConnectedSince", rStat.outboundConnectedSince);
            topicStatsStream.endObject();

            nsStats.msgReplBacklog += rStat.replicationBacklog;

            if (replStats.isMetricsEnabled()) {
                String namespaceClusterKey = replStats.getKeyName(namespace, cluster);
                ReplicationMetrics replicationMetrics = replStats.get(namespaceClusterKey);
                boolean update = false;
                if (replicationMetrics == null) {
                    replicationMetrics = ReplicationMetrics.get();
                    update = true;
                }
                replicationMetrics.connected += rStat.connected ? 1 : 0;
                replicationMetrics.msgRateOut += rStat.msgRateOut;
                replicationMetrics.msgThroughputOut += rStat.msgThroughputOut;
                replicationMetrics.msgReplBacklog += rStat.replicationBacklog;
                if (update) {
                    replStats.put(namespaceClusterKey, replicationMetrics);
                }
            }
        });

        // Close replication
        topicStatsStream.endObject();

        // Start subscription stats
        topicStatsStream.startObject("subscriptions");
        nsStats.subsCount += subscriptions.size();

        subscriptions.forEach((subscriptionName, subscription) -> {
            double subMsgRateOut = 0;
            double subMsgThroughputOut = 0;
            double subMsgRateRedeliver = 0;

            // Start subscription name & consumers
            try {
                topicStatsStream.startObject(subscriptionName);
                Object[] consumers = subscription.getConsumers().array();
                nsStats.consumerCount += consumers.length;
                bundleStats.consumerCount += consumers.length;

                topicStatsStream.startList("consumers");

                for (Object consumerObj : consumers) {
                    Consumer consumer = (Consumer) consumerObj;
                    consumer.updateRates();

                    ConsumerStats consumerStats = consumer.getStats();
                    subMsgRateOut += consumerStats.msgRateOut;
                    subMsgThroughputOut += consumerStats.msgThroughputOut;
                    subMsgRateRedeliver += consumerStats.msgRateRedeliver;

                    StreamingStats.writeConsumerStats(topicStatsStream, subscription.getType(), consumerStats);
                }

                // Close Consumer stats
                topicStatsStream.endList();

                // Populate subscription specific stats here
                topicStatsStream.writePair("msgBacklog", subscription.getNumberOfEntriesInBacklog());
                topicStatsStream.writePair("msgRateExpired", subscription.getExpiredMessageRate());
                topicStatsStream.writePair("msgRateOut", subMsgRateOut);
                topicStatsStream.writePair("msgThroughputOut", subMsgThroughputOut);
                topicStatsStream.writePair("msgRateRedeliver", subMsgRateRedeliver);
                topicStatsStream.writePair("numberOfEntriesSinceFirstNotAckedMessage", subscription.getNumberOfEntriesSinceFirstNotAckedMessage());
                topicStatsStream.writePair("totalNonContiguousDeletedMessagesRange", subscription.getTotalNonContiguousDeletedMessagesRange());
                topicStatsStream.writePair("type", subscription.getTypeString());
                if (SubType.Shared.equals(subscription.getType())) {
                    if(subscription.getDispatcher() instanceof PersistentDispatcherMultipleConsumers) {
                        PersistentDispatcherMultipleConsumers dispatcher = (PersistentDispatcherMultipleConsumers)subscription.getDispatcher();
                        topicStatsStream.writePair("blockedSubscriptionOnUnackedMsgs",  dispatcher.isBlockedDispatcherOnUnackedMsgs());
                        topicStatsStream.writePair("unackedMessages", dispatcher.getTotalUnackedMessages());
                    }
                }

                // Close consumers
                topicStatsStream.endObject();

                topicStatsHelper.aggMsgRateOut += subMsgRateOut;
                topicStatsHelper.aggMsgThroughputOut += subMsgThroughputOut;
                nsStats.msgBacklog += subscription.getNumberOfEntriesInBacklog();
            } catch (Exception e) {
                log.error("Got exception when creating consumer stats for subscription {}: {}", subscriptionName,
                        e.getMessage(), e);
            }
        });

        // Close subscription
        topicStatsStream.endObject();

        // Remaining dest stats.
        topicStatsHelper.averageMsgSize = topicStatsHelper.aggMsgRateIn == 0.0 ? 0.0
                : (topicStatsHelper.aggMsgThroughputIn / topicStatsHelper.aggMsgRateIn);
        topicStatsStream.writePair("producerCount", producers.size());
        topicStatsStream.writePair("averageMsgSize", topicStatsHelper.averageMsgSize);
        topicStatsStream.writePair("msgRateIn", topicStatsHelper.aggMsgRateIn);
        topicStatsStream.writePair("msgRateOut", topicStatsHelper.aggMsgRateOut);
        topicStatsStream.writePair("msgThroughputIn", topicStatsHelper.aggMsgThroughputIn);
        topicStatsStream.writePair("msgThroughputOut", topicStatsHelper.aggMsgThroughputOut);
        topicStatsStream.writePair("storageSize", ledger.getEstimatedBacklogSize());
        topicStatsStream.writePair("pendingAddEntriesCount", ((ManagedLedgerImpl) ledger).getPendingAddEntriesCount());

        nsStats.msgRateIn += topicStatsHelper.aggMsgRateIn;
        nsStats.msgRateOut += topicStatsHelper.aggMsgRateOut;
        nsStats.msgThroughputIn += topicStatsHelper.aggMsgThroughputIn;
        nsStats.msgThroughputOut += topicStatsHelper.aggMsgThroughputOut;
        nsStats.storageSize += ledger.getEstimatedBacklogSize();

        bundleStats.msgRateIn += topicStatsHelper.aggMsgRateIn;
        bundleStats.msgRateOut += topicStatsHelper.aggMsgRateOut;
        bundleStats.msgThroughputIn += topicStatsHelper.aggMsgThroughputIn;
        bundleStats.msgThroughputOut += topicStatsHelper.aggMsgThroughputOut;
        bundleStats.cacheSize += ((ManagedLedgerImpl) ledger).getCacheSize();

        // Close topic object
        topicStatsStream.endObject();
    }

    public TopicStats getStats() {

        TopicStats stats = new TopicStats();

        ObjectObjectHashMap<String, PublisherStats> remotePublishersStats = new ObjectObjectHashMap<String, PublisherStats>();

        producers.forEach(producer -> {
            PublisherStats publisherStats = producer.getStats();
            stats.msgRateIn += publisherStats.msgRateIn;
            stats.msgThroughputIn += publisherStats.msgThroughputIn;

            if (producer.isRemote()) {
                remotePublishersStats.put(producer.getRemoteCluster(), publisherStats);
            } else {
                stats.publishers.add(publisherStats);
            }
        });

        stats.averageMsgSize = stats.msgRateIn == 0.0 ? 0.0 : (stats.msgThroughputIn / stats.msgRateIn);

        subscriptions.forEach((name, subscription) -> {
            SubscriptionStats subStats = subscription.getStats();

            stats.msgRateOut += subStats.msgRateOut;
            stats.msgThroughputOut += subStats.msgThroughputOut;
            stats.subscriptions.put(name, subStats);
        });

        replicators.forEach((cluster, replicator) -> {
            ReplicatorStats replicatorStats = replicator.getStats();

            // Add incoming msg rates
            PublisherStats pubStats = remotePublishersStats.get(replicator.getRemoteCluster());
            if (pubStats != null) {
                replicatorStats.msgRateIn = pubStats.msgRateIn;
                replicatorStats.msgThroughputIn = pubStats.msgThroughputIn;
                replicatorStats.inboundConnection = pubStats.getAddress();
                replicatorStats.inboundConnectedSince = pubStats.getConnectedSince();
            }

            stats.msgRateOut += replicatorStats.msgRateOut;
            stats.msgThroughputOut += replicatorStats.msgThroughputOut;

            stats.replication.put(replicator.getRemoteCluster(), replicatorStats);
        });

        stats.storageSize = ledger.getEstimatedBacklogSize();
        stats.deduplicationStatus = messageDeduplication.getStatus().toString();
        return stats;
    }

    public PersistentTopicInternalStats getInternalStats() {
        PersistentTopicInternalStats stats = new PersistentTopicInternalStats();

        ManagedLedgerImpl ml = (ManagedLedgerImpl) ledger;
        stats.entriesAddedCounter = ml.getEntriesAddedCounter();
        stats.numberOfEntries = ml.getNumberOfEntries();
        stats.totalSize = ml.getTotalSize();
        stats.currentLedgerEntries = ml.getCurrentLedgerEntries();
        stats.currentLedgerSize = ml.getCurrentLedgerSize();
        stats.lastLedgerCreatedTimestamp = DateFormatter.format(ml.getLastLedgerCreatedTimestamp());
        if (ml.getLastLedgerCreationFailureTimestamp() != 0) {
            stats.lastLedgerCreationFailureTimestamp = DateFormatter.format(ml.getLastLedgerCreationFailureTimestamp());
        }

        stats.waitingCursorsCount = ml.getWaitingCursorsCount();
        stats.pendingAddEntriesCount = ml.getPendingAddEntriesCount();

        stats.lastConfirmedEntry = ml.getLastConfirmedEntry().toString();
        stats.state = ml.getState().toString();

        stats.ledgers = Lists.newArrayList();
        ml.getLedgersInfo().forEach((id, li) -> {
            LedgerInfo info = new LedgerInfo();
            info.ledgerId = li.getLedgerId();
            info.entries = li.getEntries();
            info.size = li.getSize();
            info.offloaded = li.hasOffloadContext() && li.getOffloadContext().getComplete();
            stats.ledgers.add(info);
        });

        stats.cursors = Maps.newTreeMap();
        ml.getCursors().forEach(c -> {
            ManagedCursorImpl cursor = (ManagedCursorImpl) c;
            CursorStats cs = new CursorStats();
            cs.markDeletePosition = cursor.getMarkDeletedPosition().toString();
            cs.readPosition = cursor.getReadPosition().toString();
            cs.waitingReadOp = cursor.hasPendingReadRequest();
            cs.pendingReadOps = cursor.getPendingReadOpsCount();
            cs.messagesConsumedCounter = cursor.getMessagesConsumedCounter();
            cs.cursorLedger = cursor.getCursorLedger();
            cs.cursorLedgerLastEntry = cursor.getCursorLedgerLastEntry();
            cs.individuallyDeletedMessages = cursor.getIndividuallyDeletedMessages();
            cs.lastLedgerSwitchTimestamp = DateFormatter.format(cursor.getLastLedgerSwitchTimestamp());
            cs.state = cursor.getState();
            cs.numberOfEntriesSinceFirstNotAckedMessage = cursor.getNumberOfEntriesSinceFirstNotAckedMessage();
            cs.totalNonContiguousDeletedMessagesRange = cursor.getTotalNonContiguousDeletedMessagesRange();
            cs.properties = cursor.getProperties();
            stats.cursors.put(cursor.getName(), cs);
        });
        return stats;
    }

    public long getBacklogSize() {
        return ledger.getEstimatedBacklogSize();
    }

    public boolean isActive() {
        if (TopicName.get(topic).isGlobal()) {
            // No local consumers and no local producers
            return !subscriptions.isEmpty() || hasLocalProducers();
        }
        return USAGE_COUNT_UPDATER.get(this) != 0 || !subscriptions.isEmpty();
    }

    @Override
    public void checkGC(int gcIntervalInSeconds) {
        if (isActive()) {
            lastActive = System.nanoTime();
        } else if (System.nanoTime() - lastActive < TimeUnit.SECONDS.toNanos(gcIntervalInSeconds)) {
            // Gc interval did not expire yet
            return;
        } else if (shouldTopicBeRetained()) {
            // Topic activity is still within the retention period
            return;
        } else {
            CompletableFuture<Void> replCloseFuture = new CompletableFuture<>();

            if (TopicName.get(topic).isGlobal()) {
                // For global namespace, close repl producers first.
                // Once all repl producers are closed, we can delete the topic,
                // provided no remote producers connected to the broker.
                if (log.isDebugEnabled()) {
                    log.debug("[{}] Global topic inactive for {} seconds, closing repl producers.", topic,
                            gcIntervalInSeconds);
                }
                closeReplProducersIfNoBacklog().thenRun(() -> {
                    if (hasRemoteProducers()) {
                        if (log.isDebugEnabled()) {
                            log.debug("[{}] Global topic has connected remote producers. Not a candidate for GC",
                                    topic);
                        }
                        replCloseFuture
                                .completeExceptionally(new TopicBusyException("Topic has connected remote producers"));
                    } else {
                        log.info("[{}] Global topic inactive for {} seconds, closed repl producers", topic,
                                gcIntervalInSeconds);
                        replCloseFuture.complete(null);
                    }
                }).exceptionally(e -> {
                    if (log.isDebugEnabled()) {
                        log.debug("[{}] Global topic has replication backlog. Not a candidate for GC", topic);
                    }
                    replCloseFuture.completeExceptionally(e.getCause());
                    return null;
                });
            } else {
                replCloseFuture.complete(null);
            }

            replCloseFuture.thenCompose(v -> delete(true))
                    .thenRun(() -> log.info("[{}] Topic deleted successfully due to inactivity", topic))
                    .exceptionally(e -> {
                        if (e.getCause() instanceof TopicBusyException) {
                            // topic became active again
                            if (log.isDebugEnabled()) {
                                log.debug("[{}] Did not delete busy topic: {}", topic, e.getCause().getMessage());
                            }
                        } else {
                            log.warn("[{}] Inactive topic deletion failed", topic, e);
                        }
                        return null;
                    });

        }
    }

    @Override
    public void checkInactiveSubscriptions() {
        final long expirationTime = TimeUnit.MINUTES.toMillis(brokerService.pulsar().getConfiguration().getSubscriptionExpirationTimeMinutes());
        if (expirationTime <= 0) return;
        subscriptions.forEach((subName, sub) -> {
            if (sub.dispatcher != null && sub.dispatcher.isConsumerConnected()) return;
            if (System.currentTimeMillis() - sub.cursor.getLastActive() > expirationTime) {
                sub.delete().thenAccept(
                        v -> log.info("[{}][{}] The subscription was deleted due to expiration", topic, subName));
            }
        });
    }

    /**
     * Check whether the topic should be retained (based on time), even tough there are no producers/consumers and it's
     * marked as inactive.
     */
    private boolean shouldTopicBeRetained() {
        TopicName name = TopicName.get(topic);
        try {
            Optional<Policies> policies = brokerService.pulsar().getConfigurationCache().policiesCache()
                    .get(AdminResource.path(POLICIES, name.getNamespace()));
            // If no policies, the default is to have no retention and delete the inactive topic
            return policies.map(p -> p.retention_policies).map(rp -> {
                long retentionTime = TimeUnit.MINUTES.toNanos(rp.getRetentionTimeInMinutes());

                // Negative retention time means the topic should be retained indefinitely,
                // because its own data has to be retained
                return retentionTime < 0 || (System.nanoTime() - lastActive) < retentionTime;
            }).orElse(false).booleanValue();
        } catch (Exception e) {
            if (log.isDebugEnabled()) {
                log.debug("[{}] Error getting policies", topic);
            }

            // Don't delete in case we cannot get the policies
            return true;
        }
    }

    @Override
    public CompletableFuture<Void> onPoliciesUpdate(Policies data) {
        if (log.isDebugEnabled()) {
            log.debug("[{}] isEncryptionRequired changes: {} -> {}", topic, isEncryptionRequired, data.encryption_required);
        }
        isEncryptionRequired = data.encryption_required;
        producers.forEach(producer -> {
            producer.checkPermissions();
            producer.checkEncryption();
        });
        subscriptions.forEach((subName, sub) -> {
            sub.getConsumers().forEach(Consumer::checkPermissions);
            if (sub.getDispatcher().getRateLimiter() != null) {
                sub.getDispatcher().getRateLimiter().onPoliciesUpdate(data);
            }
        });
        checkMessageExpiry();
        CompletableFuture<Void> replicationFuture = checkReplicationAndRetryOnFailure();
        CompletableFuture<Void> dedupFuture = checkDeduplicationStatus();
        CompletableFuture<Void> persistentPoliciesFuture = checkPersistencePolicies();
        dispatchRateLimiter.onPoliciesUpdate(data);
        return CompletableFuture.allOf(replicationFuture, dedupFuture, persistentPoliciesFuture);
    }

    /**
     *
     * @return Backlog quota for topic
     */
    @Override
    public BacklogQuota getBacklogQuota() {
        TopicName topicName = TopicName.get(this.getName());
        String namespace = topicName.getNamespace();
        String policyPath = AdminResource.path(POLICIES, namespace);

        BacklogQuota backlogQuota = brokerService.getBacklogQuotaManager().getBacklogQuota(namespace, policyPath);
        return backlogQuota;
    }

    /**
     *
     * @return quota exceeded status for blocking producer creation
     */
    @Override
    public boolean isBacklogQuotaExceeded(String producerName) {
        BacklogQuota backlogQuota = getBacklogQuota();

        if (backlogQuota != null) {
            BacklogQuota.RetentionPolicy retentionPolicy = backlogQuota.getPolicy();

            if ((retentionPolicy == BacklogQuota.RetentionPolicy.producer_request_hold
                    || retentionPolicy == BacklogQuota.RetentionPolicy.producer_exception)
                    && brokerService.isBacklogExceeded(this)) {
                log.info("[{}] Backlog quota exceeded. Cannot create producer [{}]", this.getName(), producerName);
                return true;
            } else {
                return false;
            }
        }
        return false;
    }

    @Override
    public boolean isEncryptionRequired() {
        return isEncryptionRequired;
    }

    @Override
    public boolean isReplicated() {
        return !replicators.isEmpty();
    }

    public CompletableFuture<MessageId> terminate() {
        CompletableFuture<MessageId> future = new CompletableFuture<>();
        ledger.asyncTerminate(new TerminateCallback() {
            @Override
            public void terminateComplete(Position lastCommittedPosition, Object ctx) {
                producers.forEach(Producer::disconnect);
                subscriptions.forEach((name, sub) -> sub.topicTerminated());

                PositionImpl lastPosition = (PositionImpl) lastCommittedPosition;
                MessageId messageId = new MessageIdImpl(lastPosition.getLedgerId(), lastPosition.getEntryId(), -1);

                log.info("[{}] Topic terminated at {}", getName(), messageId);
                future.complete(messageId);
            }

            @Override
            public void terminateFailed(ManagedLedgerException exception, Object ctx) {
                future.completeExceptionally(exception);
            }
        }, null);

        return future;
    }

    public boolean isOldestMessageExpired(ManagedCursor cursor, long messageTTLInSeconds) {
        MessageImpl msg = null;
        Entry entry = null;
        boolean isOldestMessageExpired = false;
        try {
            entry = cursor.getNthEntry(1, IndividualDeletedEntries.Include);
            if (entry != null) {
                msg = MessageImpl.deserialize(entry.getDataBuffer());
                isOldestMessageExpired = messageTTLInSeconds != 0 && System.currentTimeMillis() > (msg.getPublishTime()
                        + TimeUnit.SECONDS.toMillis((long) (messageTTLInSeconds * MESSAGE_EXPIRY_THRESHOLD)));
            }
        } catch (Exception e) {
            log.warn("[{}] Error while getting the oldest message", topic, e);
        } finally {
            if (entry != null) {
                entry.release();
            }
            if (msg != null) {
                msg.recycle();
            }
        }

        return isOldestMessageExpired;
    }

    /**
     * Clears backlog for all cursors in the topic
     *
     * @return
     */
    public CompletableFuture<Void> clearBacklog() {
        log.info("[{}] Clearing backlog on all cursors in the topic.", topic);
        List<CompletableFuture<Void>> futures = Lists.newArrayList();
        List<String> cursors = getSubscriptions().keys();
        cursors.addAll(getReplicators().keys());
        for (String cursor : cursors) {
            futures.add(clearBacklog(cursor));
        }
        return FutureUtil.waitForAll(futures);
    }

    /**
     * Clears backlog for a given cursor in the topic.
     * <p>
     * Note: For a replication cursor, just provide the remote cluster name
     * </p>
     *
     * @param cursorName
     * @return
     */
    public CompletableFuture<Void> clearBacklog(String cursorName) {
        log.info("[{}] Clearing backlog for cursor {} in the topic.", topic, cursorName);
        PersistentSubscription sub = getSubscription(cursorName);
        if (sub != null) {
            return sub.clearBacklog();
        }

        PersistentReplicator repl = (PersistentReplicator) getPersistentReplicator(cursorName);
        if (repl != null) {
            return repl.clearBacklog();
        }

        return FutureUtil.failedFuture(new BrokerServiceException("Cursor not found"));
    }

    public void markBatchMessagePublished() {
        this.hasBatchMessagePublished = true;
    }

    public DispatchRateLimiter getDispatchRateLimiter() {
        return this.dispatchRateLimiter;
    }

    public long getLastPublishedSequenceId(String producerName) {
        return messageDeduplication.getLastPublishedSequenceId(producerName);
    }

    @Override
    public Position getLastMessageId() {
        return ledger.getLastConfirmedEntry();
    }

    public synchronized void triggerCompaction()
            throws PulsarServerException, AlreadyRunningException {
        if (currentCompaction.isDone()) {
            currentCompaction = brokerService.pulsar().getCompactor().compact(topic);
        } else {
            throw new AlreadyRunningException("Compaction already in progress");
        }
    }

    public synchronized LongRunningProcessStatus compactionStatus() {
        final CompletableFuture<Long> current;
        synchronized (this) {
            current = currentCompaction;
        }
        if (!current.isDone()) {
            return LongRunningProcessStatus.forStatus(LongRunningProcessStatus.Status.RUNNING);
        } else {
            try {
                if (current.join() == COMPACTION_NEVER_RUN) {
                    return LongRunningProcessStatus.forStatus(LongRunningProcessStatus.Status.NOT_RUN);
                } else {
                    return LongRunningProcessStatus.forStatus(LongRunningProcessStatus.Status.SUCCESS);
                }
            } catch (CancellationException | CompletionException e) {
                return LongRunningProcessStatus.forError(e.getMessage());
            }
        }
    }

    public synchronized void triggerOffload(MessageIdImpl messageId) throws AlreadyRunningException {
        if (currentOffload.isDone()) {
            CompletableFuture<MessageIdImpl> promise = currentOffload = new CompletableFuture<>();
            getManagedLedger().asyncOffloadPrefix(
                    PositionImpl.get(messageId.getLedgerId(), messageId.getEntryId()),
                    new OffloadCallback() {
                        @Override
                        public void offloadComplete(Position pos, Object ctx) {
                            PositionImpl impl = (PositionImpl)pos;

                            promise.complete(new MessageIdImpl(impl.getLedgerId(), impl.getEntryId(), -1));
                        }

                        @Override
                        public void offloadFailed(ManagedLedgerException exception, Object ctx) {
                            promise.completeExceptionally(exception);
                        }
                    }, null);
        } else {
            throw new AlreadyRunningException("Offload already in progress");
        }
    }

    public synchronized OffloadProcessStatus offloadStatus() {
        if (!currentOffload.isDone()) {
            return OffloadProcessStatus.forStatus(LongRunningProcessStatus.Status.RUNNING);
        } else {
            try {
                if (currentOffload.join() == MessageId.earliest) {
                    return OffloadProcessStatus.forStatus(LongRunningProcessStatus.Status.NOT_RUN);
                } else {
                    return OffloadProcessStatus.forSuccess(currentOffload.join());
                }
            } catch (CancellationException | CompletionException e) {
                return OffloadProcessStatus.forError(e.getMessage());
            }
        }
    }

    private static final Logger log = LoggerFactory.getLogger(PersistentTopic.class);

    @Override
    public CompletableFuture<Boolean> hasSchema() {
        String base = TopicName.get(getName()).getPartitionedTopicName();
        String id = TopicName.get(base).getSchemaName();
        return brokerService.pulsar()
            .getSchemaRegistryService()
            .getSchema(id).thenApply((schema) -> schema != null);
    }

    @Override
    public CompletableFuture<SchemaVersion> addSchema(SchemaData schema) {
        if (schema == null) {
            return CompletableFuture.completedFuture(SchemaVersion.Empty);
        }

        String base = TopicName.get(getName()).getPartitionedTopicName();
        String id = TopicName.get(base).getSchemaName();
        return brokerService.pulsar()
            .getSchemaRegistryService()
            .putSchemaIfAbsent(id, schema);
    }

    @Override
    public CompletableFuture<Boolean> isSchemaCompatible(SchemaData schema) {
        String base = TopicName.get(getName()).getPartitionedTopicName();
        String id = TopicName.get(base).getSchemaName();
        return brokerService.pulsar()
            .getSchemaRegistryService()
            .isCompatibleWithLatestVersion(id, schema);
    }

    @Override
    public CompletableFuture<Boolean> addSchemaIfIdleOrCheckCompatible(SchemaData schema) {
        return hasSchema()
            .thenCompose((hasSchema) -> {
                    if (hasSchema || isActive() || ledger.getTotalSize() != 0) {
                        return isSchemaCompatible(schema);
                    } else {
                        return addSchema(schema).thenApply((ignore) -> true);
                    }
                });
    }
}
