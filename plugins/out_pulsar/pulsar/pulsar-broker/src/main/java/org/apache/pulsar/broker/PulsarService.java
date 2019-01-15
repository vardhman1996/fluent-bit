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
package org.apache.pulsar.broker;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.commons.lang3.StringUtils.isNotBlank;
import static org.apache.pulsar.broker.admin.impl.NamespacesBase.getBundles;
import static org.apache.pulsar.broker.cache.ConfigurationCacheService.POLICIES;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import io.netty.util.concurrent.DefaultThreadFactory;

import java.io.IOException;
import java.net.URI;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;

import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.common.util.OrderedExecutor;
import org.apache.bookkeeper.common.util.OrderedScheduler;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.mledger.LedgerOffloader;
import org.apache.bookkeeper.mledger.LedgerOffloaderFactory;
import org.apache.bookkeeper.mledger.ManagedLedgerFactory;
import org.apache.bookkeeper.mledger.impl.NullLedgerOffloader;
import org.apache.bookkeeper.mledger.offload.OffloaderUtils;
import org.apache.bookkeeper.mledger.offload.Offloaders;
import org.apache.bookkeeper.util.ZkUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.builder.ReflectionToStringBuilder;
import org.apache.pulsar.broker.admin.AdminResource;
import org.apache.pulsar.broker.cache.ConfigurationCacheService;
import org.apache.pulsar.broker.cache.LocalZooKeeperCacheService;
import org.apache.pulsar.broker.loadbalance.LeaderElectionService;
import org.apache.pulsar.broker.loadbalance.LeaderElectionService.LeaderListener;
import org.apache.pulsar.broker.loadbalance.LoadManager;
import org.apache.pulsar.broker.loadbalance.LoadReportUpdaterTask;
import org.apache.pulsar.broker.loadbalance.LoadResourceQuotaUpdaterTask;
import org.apache.pulsar.broker.loadbalance.LoadSheddingTask;
import org.apache.pulsar.broker.loadbalance.impl.LoadManagerShared;
import org.apache.pulsar.broker.namespace.NamespaceService;
import org.apache.pulsar.broker.service.BrokerService;
import org.apache.pulsar.broker.service.Topic;
import org.apache.pulsar.broker.service.schema.SchemaRegistryService;
import org.apache.pulsar.broker.stats.MetricsGenerator;
import org.apache.pulsar.broker.stats.prometheus.PrometheusMetricsServlet;
import org.apache.pulsar.broker.web.WebService;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminBuilder;
import org.apache.pulsar.client.api.ClientBuilder;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.common.conf.InternalConfigurationData;
import org.apache.pulsar.common.configuration.PulsarConfigurationLoader;
import org.apache.pulsar.common.configuration.VipStatus;
import org.apache.pulsar.common.naming.NamedEntity;
import org.apache.pulsar.common.naming.NamespaceBundle;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.Policies;
import org.apache.pulsar.common.policies.data.RetentionPolicies;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.common.util.ObjectMapperFactory;
import org.apache.pulsar.compaction.Compactor;
import org.apache.pulsar.compaction.TwoPhaseCompactor;
import org.apache.pulsar.functions.worker.Utils;
import org.apache.pulsar.functions.worker.WorkerService;
import org.apache.pulsar.utils.PulsarBrokerVersionStringUtils;
import org.apache.pulsar.websocket.WebSocketConsumerServlet;
import org.apache.pulsar.websocket.WebSocketProducerServlet;
import org.apache.pulsar.websocket.WebSocketReaderServlet;
import org.apache.pulsar.websocket.WebSocketService;
import org.apache.pulsar.zookeeper.GlobalZooKeeperCache;
import org.apache.pulsar.zookeeper.LocalZooKeeperCache;
import org.apache.pulsar.zookeeper.LocalZooKeeperConnectionService;
import org.apache.pulsar.zookeeper.ZooKeeperCache;
import org.apache.pulsar.zookeeper.ZooKeeperClientFactory;
import org.apache.pulsar.zookeeper.ZookeeperBkClientFactoryImpl;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.eclipse.jetty.servlet.ServletHolder;
import org.eclipse.jetty.websocket.servlet.WebSocketServlet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Main class for Pulsar broker service
 */

public class PulsarService implements AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(PulsarService.class);
    private ServiceConfiguration config = null;
    private NamespaceService nsservice = null;
    private ManagedLedgerClientFactory managedLedgerClientFactory = null;
    private LeaderElectionService leaderElectionService = null;
    private BrokerService brokerService = null;
    private WebService webService = null;
    private WebSocketService webSocketService = null;
    private ConfigurationCacheService configurationCacheService = null;
    private LocalZooKeeperCacheService localZkCacheService = null;
    private BookKeeperClientFactory bkClientFactory;
    private ZooKeeperCache localZkCache;
    private GlobalZooKeeperCache globalZkCache;
    private LocalZooKeeperConnectionService localZooKeeperConnectionProvider;
    private Compactor compactor;

    private final ScheduledExecutorService executor = Executors.newScheduledThreadPool(20,
            new DefaultThreadFactory("pulsar"));
    private final ScheduledExecutorService cacheExecutor = Executors.newScheduledThreadPool(10,
            new DefaultThreadFactory("zk-cache-callback"));
    private final OrderedExecutor orderedExecutor = OrderedExecutor.newBuilder().numThreads(8).name("pulsar-ordered")
            .build();
    private final ScheduledExecutorService loadManagerExecutor;
    private ScheduledExecutorService compactorExecutor;
    private OrderedScheduler offloaderScheduler;
    private Offloaders offloaderManager = new Offloaders();
    private LedgerOffloader offloader;
    private ScheduledFuture<?> loadReportTask = null;
    private ScheduledFuture<?> loadSheddingTask = null;
    private ScheduledFuture<?> loadResourceQuotaTask = null;
    private final AtomicReference<LoadManager> loadManager = new AtomicReference<>();
    private PulsarAdmin adminClient = null;
    private PulsarClient client = null;
    private ZooKeeperClientFactory zkClientFactory = null;
    private final String bindAddress;
    private final String advertisedAddress;
    private final String webServiceAddress;
    private final String webServiceAddressTls;
    private final String brokerServiceUrl;
    private final String brokerServiceUrlTls;
    private final String brokerVersion;
    private SchemaRegistryService schemaRegistryService = null;
    private final Optional<WorkerService> functionWorkerService;

    private final MessagingServiceShutdownHook shutdownService;

    private MetricsGenerator metricsGenerator;

    public enum State {
        Init, Started, Closed
    }

    private volatile State state;

    private final ReentrantLock mutex = new ReentrantLock();
    private final Condition isClosedCondition = mutex.newCondition();

    public PulsarService(ServiceConfiguration config) {
        this(config, Optional.empty());
    }

    public PulsarService(ServiceConfiguration config, Optional<WorkerService> functionWorkerService) {
        // Validate correctness of configuration
        PulsarConfigurationLoader.isComplete(config);

        state = State.Init;
        this.bindAddress = ServiceConfigurationUtils.getDefaultOrConfiguredAddress(config.getBindAddress());
        this.advertisedAddress = advertisedAddress(config);
        this.webServiceAddress = webAddress(config);
        this.webServiceAddressTls = webAddressTls(config);
        this.brokerServiceUrl = brokerUrl(config);
        this.brokerServiceUrlTls = brokerUrlTls(config);
        this.brokerVersion = PulsarBrokerVersionStringUtils.getNormalizedVersionString();
        this.config = config;
        this.shutdownService = new MessagingServiceShutdownHook(this);
        this.loadManagerExecutor = Executors
                .newSingleThreadScheduledExecutor(new DefaultThreadFactory("pulsar-load-manager"));
        this.functionWorkerService = functionWorkerService;
    }

    /**
     * Close the current pulsar service. All resources are released.
     */
    @Override
    public void close() throws PulsarServerException {
        mutex.lock();

        try {
            if (state == State.Closed) {
                return;
            }

            // close the service in reverse order v.s. in which they are started
            if (this.webService != null) {
                this.webService.close();
                this.webService = null;
            }

            if (this.brokerService != null) {
                this.brokerService.close();
                this.brokerService = null;
            }

            if (this.managedLedgerClientFactory != null) {
                this.managedLedgerClientFactory.close();
                this.managedLedgerClientFactory = null;
            }

            if (bkClientFactory != null) {
                this.bkClientFactory.close();
                this.bkClientFactory = null;
            }

            if (this.leaderElectionService != null) {
                this.leaderElectionService.stop();
                this.leaderElectionService = null;
            }

            loadManagerExecutor.shutdown();

            if (globalZkCache != null) {
                globalZkCache.close();
                globalZkCache = null;
                localZooKeeperConnectionProvider.close();
                localZooKeeperConnectionProvider = null;
            }

            configurationCacheService = null;
            localZkCacheService = null;
            if (localZkCache != null) {
                localZkCache.stop();
                localZkCache = null;
            }

            if (adminClient != null) {
                adminClient.close();
                adminClient = null;
            }

            if (client != null) {
                client.close();
                client = null;
            }

            nsservice = null;

            if (compactorExecutor != null) {
                compactorExecutor.shutdown();
            }

            if (offloaderScheduler != null) {
                offloaderScheduler.shutdown();
            }

            // executor is not initialized in mocks even when real close method is called
            // guard against null executors
            if (executor != null) {
                executor.shutdown();
            }

            orderedExecutor.shutdown();
            cacheExecutor.shutdown();

            LoadManager loadManager = this.loadManager.get();
            if (loadManager != null) {
                loadManager.stop();
            }

            if (schemaRegistryService != null) {
                schemaRegistryService.close();
            }

            offloaderManager.close();

            state = State.Closed;

        } catch (Exception e) {
            throw new PulsarServerException(e);
        } finally {
            mutex.unlock();
        }
    }

    /**
     * Get the current service configuration.
     *
     * @return the current service configuration
     */
    public ServiceConfiguration getConfiguration() {
        return this.config;
    }

    /**
     * Start the pulsar service instance.
     */
    public void start() throws PulsarServerException {
        mutex.lock();

        LOG.info("Starting Pulsar Broker service; version: '{}'", ( brokerVersion != null ? brokerVersion : "unknown" )  );
        LOG.info("Git Revision {}", PulsarBrokerVersionStringUtils.getGitSha());
        LOG.info("Built by {} on {} at {}",
                 PulsarBrokerVersionStringUtils.getBuildUser(),
                 PulsarBrokerVersionStringUtils.getBuildHost(),
                 PulsarBrokerVersionStringUtils.getBuildTime());

        try {
            if (state != State.Init) {
                throw new PulsarServerException("Cannot start the service once it was stopped");
            }

            // Now we are ready to start services
            localZooKeeperConnectionProvider = new LocalZooKeeperConnectionService(getZooKeeperClientFactory(),
                    config.getZookeeperServers(), config.getZooKeeperSessionTimeoutMillis());
            localZooKeeperConnectionProvider.start(shutdownService);

            // Initialize and start service to access configuration repository.
            this.startZkCacheService();

            this.bkClientFactory = newBookKeeperClientFactory();
            managedLedgerClientFactory = new ManagedLedgerClientFactory(config, getZkClient(), bkClientFactory);

            this.brokerService = new BrokerService(this);

            // Start load management service (even if load balancing is disabled)
            this.loadManager.set(LoadManager.create(this));

            // needs load management service
            this.startNamespaceService();

            this.offloader = createManagedLedgerOffloader(this.getConfiguration());

            brokerService.start();

            this.webService = new WebService(this);
            Map<String, Object> attributeMap = Maps.newHashMap();
            attributeMap.put(WebService.ATTRIBUTE_PULSAR_NAME, this);
            Map<String, Object> vipAttributeMap = Maps.newHashMap();
            vipAttributeMap.put(VipStatus.ATTRIBUTE_STATUS_FILE_PATH, this.config.getStatusFilePath());
            vipAttributeMap.put(VipStatus.ATTRIBUTE_IS_READY_PROBE, new Supplier<Boolean>() {
                @Override
                public Boolean get() {
                    // Ensure the VIP status is only visible when the broker is fully initialized
                    return state == State.Started;
                }
            });
            this.webService.addRestResources("/", VipStatus.class.getPackage().getName(), false, vipAttributeMap);
            this.webService.addRestResources("/", "org.apache.pulsar.broker.web", false, attributeMap);
            this.webService.addRestResources("/admin", "org.apache.pulsar.broker.admin.v1", true, attributeMap);
            this.webService.addRestResources("/admin/v2", "org.apache.pulsar.broker.admin.v2", true, attributeMap);
            this.webService.addRestResources("/lookup", "org.apache.pulsar.broker.lookup", true, attributeMap);

            this.webService.addServlet("/metrics",
                    new ServletHolder(new PrometheusMetricsServlet(this, config.exposeTopicLevelMetricsInPrometheus(), config.exposeConsumerLevelMetricsInPrometheus())),
                    false, attributeMap);

            if (config.isWebSocketServiceEnabled()) {
                // Use local broker address to avoid different IP address when using a VIP for service discovery
                this.webSocketService = new WebSocketService(
                        new ClusterData(webServiceAddress, webServiceAddressTls, brokerServiceUrl, brokerServiceUrlTls),
                        config);
                this.webSocketService.start();

                final WebSocketServlet producerWebSocketServlet = new WebSocketProducerServlet(webSocketService);
                this.webService.addServlet(WebSocketProducerServlet.SERVLET_PATH,
                        new ServletHolder(producerWebSocketServlet), true, attributeMap);
                this.webService.addServlet(WebSocketProducerServlet.SERVLET_PATH_V2,
                        new ServletHolder(producerWebSocketServlet), true, attributeMap);

                final WebSocketServlet consumerWebSocketServlet = new WebSocketConsumerServlet(webSocketService);
                this.webService.addServlet(WebSocketConsumerServlet.SERVLET_PATH,
                        new ServletHolder(consumerWebSocketServlet), true, attributeMap);
                this.webService.addServlet(WebSocketConsumerServlet.SERVLET_PATH_V2,
                        new ServletHolder(consumerWebSocketServlet), true, attributeMap);

                final WebSocketServlet readerWebSocketServlet = new WebSocketReaderServlet(webSocketService);
                this.webService.addServlet(WebSocketReaderServlet.SERVLET_PATH,
                        new ServletHolder(readerWebSocketServlet), true, attributeMap);
                this.webService.addServlet(WebSocketReaderServlet.SERVLET_PATH_V2,
                        new ServletHolder(readerWebSocketServlet), true, attributeMap);
            }

            if (LOG.isDebugEnabled()) {
                LOG.debug("Attempting to add static directory");
            }
            this.webService.addStaticResources("/static", "/static");

            // Register heartbeat and bootstrap namespaces.
            this.nsservice.registerBootstrapNamespaces();

            // Start the leader election service
            this.leaderElectionService = new LeaderElectionService(this, new LeaderListener() {
                @Override
                public synchronized void brokerIsTheLeaderNow() {
                    if (getConfiguration().isLoadBalancerEnabled()) {
                        long loadSheddingInterval = TimeUnit.MINUTES
                                .toMillis(getConfiguration().getLoadBalancerSheddingIntervalMinutes());
                        long resourceQuotaUpdateInterval = TimeUnit.MINUTES
                                .toMillis(getConfiguration().getLoadBalancerResourceQuotaUpdateIntervalMinutes());

                        loadSheddingTask = loadManagerExecutor.scheduleAtFixedRate(new LoadSheddingTask(loadManager),
                                loadSheddingInterval, loadSheddingInterval, TimeUnit.MILLISECONDS);
                        loadResourceQuotaTask = loadManagerExecutor.scheduleAtFixedRate(
                                new LoadResourceQuotaUpdaterTask(loadManager), resourceQuotaUpdateInterval,
                                resourceQuotaUpdateInterval, TimeUnit.MILLISECONDS);
                    }
                }

                @Override
                public synchronized void brokerIsAFollowerNow() {
                    if (loadSheddingTask != null) {
                        loadSheddingTask.cancel(false);
                        loadSheddingTask = null;
                    }
                    if (loadResourceQuotaTask != null) {
                        loadResourceQuotaTask.cancel(false);
                        loadResourceQuotaTask = null;
                    }
                }
            });

            leaderElectionService.start();

            schemaRegistryService = SchemaRegistryService.create(this);

            webService.start();

            this.metricsGenerator = new MetricsGenerator(this);

            // By starting the Load manager service, the broker will also become visible
            // to the rest of the broker by creating the registration z-node. This needs
            // to be done only when the broker is fully operative.
            this.startLoadManagementService();

            state = State.Started;

            acquireSLANamespace();

            // start function worker service if necessary
            this.startWorkerService();

            LOG.info("messaging service is ready, bootstrap service on port={}, broker url={}, cluster={}, configs={}",
                    config.getWebServicePort(), brokerServiceUrl, config.getClusterName(),
                    ReflectionToStringBuilder.toString(config));
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
            throw new PulsarServerException(e);
        } finally {
            mutex.unlock();
        }
    }

    private void acquireSLANamespace() {
        try {
            // Namespace not created hence no need to unload it
            String nsName = NamespaceService.getSLAMonitorNamespace(getAdvertisedAddress(), config);
            if (!this.globalZkCache.exists(
                    AdminResource.path(POLICIES) + "/" + nsName)) {
                LOG.info("SLA Namespace = {} doesn't exist.", nsName);
                return;
            }

            boolean acquiredSLANamespace;
            try {
                acquiredSLANamespace = nsservice.registerSLANamespace();
                LOG.info("Register SLA Namespace = {}, returned - {}.", nsName, acquiredSLANamespace);
            } catch (PulsarServerException e) {
                acquiredSLANamespace = false;
            }

            if (!acquiredSLANamespace) {
                this.nsservice.unloadSLANamespace();
            }
        } catch (Exception ex) {
            LOG.warn(
                    "Exception while trying to unload the SLA namespace, will try to unload the namespace again after 1 minute. Exception:",
                    ex);
            executor.schedule(this::acquireSLANamespace, 1, TimeUnit.MINUTES);
        } catch (Throwable ex) {
            // To make sure SLA monitor doesn't interfere with the normal broker flow
            LOG.warn(
                    "Exception while trying to unload the SLA namespace, will not try to unload the namespace again. Exception:",
                    ex);
        }
    }

    /**
     * Block until the service is finally closed
     */
    public void waitUntilClosed() throws InterruptedException {
        mutex.lock();

        try {
            while (state != State.Closed) {
                isClosedCondition.await();
            }
        } finally {
            mutex.unlock();
        }
    }

    private void startZkCacheService() throws PulsarServerException {

        LOG.info("starting configuration cache service");

        this.localZkCache = new LocalZooKeeperCache(getZkClient(), getOrderedExecutor());
        this.globalZkCache = new GlobalZooKeeperCache(getZooKeeperClientFactory(),
                (int) config.getZooKeeperSessionTimeoutMillis(), config.getConfigurationStoreServers(),
                getOrderedExecutor(), this.cacheExecutor);
        try {
            this.globalZkCache.start();
        } catch (IOException e) {
            throw new PulsarServerException(e);
        }

        this.configurationCacheService = new ConfigurationCacheService(getGlobalZkCache(), this.config.getClusterName());
        this.localZkCacheService = new LocalZooKeeperCacheService(getLocalZkCache(), this.configurationCacheService);
    }

    private void startNamespaceService() throws PulsarServerException {

        LOG.info("starting name space service, bootstrap namespaces=" + config.getBootstrapNamespaces());

        this.nsservice = getNamespaceServiceProvider().get();
    }

    public Supplier<NamespaceService> getNamespaceServiceProvider() throws PulsarServerException {
        return () -> new NamespaceService(PulsarService.this);
    }

    private void startLoadManagementService() throws PulsarServerException {
        LOG.info("Starting load management service ...");
        this.loadManager.get().start();

        if (config.isLoadBalancerEnabled()) {
            LOG.info("Starting load balancer");
            if (this.loadReportTask == null) {
                long loadReportMinInterval = LoadManagerShared.LOAD_REPORT_UPDATE_MIMIMUM_INTERVAL;
                this.loadReportTask = this.loadManagerExecutor.scheduleAtFixedRate(
                        new LoadReportUpdaterTask(loadManager), loadReportMinInterval, loadReportMinInterval,
                        TimeUnit.MILLISECONDS);
            }
        }
    }

    /**
     * Load all the topics contained in a namespace
     *
     * @param bundle
     *            <code>NamespaceBundle</code> to identify the service unit
     * @throws Exception
     */
    public void loadNamespaceTopics(NamespaceBundle bundle) {
        executor.submit(() -> {
            LOG.info("Loading all topics on bundle: {}", bundle);

            NamespaceName nsName = bundle.getNamespaceObject();
            List<CompletableFuture<Topic>> persistentTopics = Lists.newArrayList();
            long topicLoadStart = System.nanoTime();

            for (String topic : getNamespaceService().getListOfPersistentTopics(nsName)) {
                try {
                    TopicName topicName = TopicName.get(topic);
                    if (bundle.includes(topicName)) {
                        CompletableFuture<Topic> future = brokerService.getOrCreateTopic(topic);
                        if (future != null) {
                            persistentTopics.add(future);
                        }
                    }
                } catch (Throwable t) {
                    LOG.warn("Failed to preload topic {}", topic, t);
                }
            }

            if (!persistentTopics.isEmpty()) {
                FutureUtil.waitForAll(persistentTopics).thenRun(() -> {
                    double topicLoadTimeSeconds = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - topicLoadStart)
                            / 1000.0;
                    LOG.info("Loaded {} topics on {} -- time taken: {} seconds", persistentTopics.size(), bundle,
                            topicLoadTimeSeconds);
                });
            }
            return null;
        });
    }

    // No need to synchronize since config is only init once
    // We only read this from memory later
    public String getStatusFilePath() {
        if (config == null) {
            return null;
        }
        return config.getStatusFilePath();
    }

    public ZooKeeper getZkClient() {
        return this.localZooKeeperConnectionProvider.getLocalZooKeeper();
    }

    public ConfigurationCacheService getConfigurationCache() {
        return configurationCacheService;
    }

    /**
     * Get the current pulsar state.
     */
    public State getState() {
        return this.state;
    }

    /**
     * Get a reference of the current <code>LeaderElectionService</code> instance associated with the current
     * <code>PulsarService<code> instance.
     *
     * @return a reference of the current <code>LeaderElectionService</code> instance.
     */
    public LeaderElectionService getLeaderElectionService() {
        return this.leaderElectionService;
    }

    /**
     * Get a reference of the current namespace service instance.
     *
     * @return a reference of the current namespace service instance.
     */
    public NamespaceService getNamespaceService() {
        return this.nsservice;
    }

    public WorkerService getWorkerService() {
        return functionWorkerService.orElse(null);
    }

    /**
     * Get a reference of the current <code>BrokerService</code> instance associated with the current
     * <code>PulsarService</code> instance.
     *
     * @return a reference of the current <code>BrokerService</code> instance.
     */
    public BrokerService getBrokerService() {
        return this.brokerService;
    }

    public BookKeeper getBookKeeperClient() {
        return managedLedgerClientFactory.getBookKeeperClient();
    }

    public ManagedLedgerFactory getManagedLedgerFactory() {
        return managedLedgerClientFactory.getManagedLedgerFactory();
    }

    public LedgerOffloader getManagedLedgerOffloader() {
        return offloader;
    }

    // TODO: improve the user metadata in subsequent changes
    static final String METADATA_SOFTWARE_VERSION_KEY = "S3ManagedLedgerOffloaderSoftwareVersion";
    static final String METADATA_SOFTWARE_GITSHA_KEY = "S3ManagedLedgerOffloaderSoftwareGitSha";


    public synchronized LedgerOffloader createManagedLedgerOffloader(ServiceConfiguration conf)
            throws PulsarServerException {
        try {
            if (StringUtils.isNotBlank(conf.getManagedLedgerOffloadDriver())) {
                checkNotNull(conf.getOffloadersDirectory(),
                    "Offloader driver is configured to be '%s' but no offloaders directory is configured.",
                    conf.getManagedLedgerOffloadDriver());
                this.offloaderManager = OffloaderUtils.searchForOffloaders(conf.getOffloadersDirectory());
                LedgerOffloaderFactory offloaderFactory = this.offloaderManager.getOffloaderFactory(
                    conf.getManagedLedgerOffloadDriver());
                try {
                    return offloaderFactory.create(
                        conf.getProperties(),
                        ImmutableMap.of(
                            METADATA_SOFTWARE_VERSION_KEY.toLowerCase(), PulsarBrokerVersionStringUtils.getNormalizedVersionString(),
                            METADATA_SOFTWARE_GITSHA_KEY.toLowerCase(), PulsarBrokerVersionStringUtils.getGitSha()
                        ),
                        getOffloaderScheduler(conf));
                } catch (IOException ioe) {
                    throw new PulsarServerException(ioe.getMessage(), ioe.getCause());
                }
            } else {
                return NullLedgerOffloader.INSTANCE;
            }
        } catch (Throwable t) {
            throw new PulsarServerException(t);
        }
    }

    public ZooKeeperCache getLocalZkCache() {
        return localZkCache;
    }

    public ZooKeeperCache getGlobalZkCache() {
        return globalZkCache;
    }

    public ScheduledExecutorService getExecutor() {
        return executor;
    }

    public ScheduledExecutorService getCacheExecutor() {
        return cacheExecutor;
    }

    public ScheduledExecutorService getLoadManagerExecutor() {
        return loadManagerExecutor;
    }

    public OrderedExecutor getOrderedExecutor() {
        return orderedExecutor;
    }

    public LocalZooKeeperCacheService getLocalZkCacheService() {
        return this.localZkCacheService;
    }

    public ZooKeeperClientFactory getZooKeeperClientFactory() {
        if (zkClientFactory == null) {
            zkClientFactory = new ZookeeperBkClientFactoryImpl(orderedExecutor);
        }
        // Return default factory
        return zkClientFactory;
    }

    public BookKeeperClientFactory newBookKeeperClientFactory() {
        return new BookKeeperClientFactoryImpl();
    }

    public BookKeeperClientFactory getBookKeeperClientFactory() {
        return bkClientFactory;
    }

    protected synchronized ScheduledExecutorService getCompactorExecutor() {
        if (this.compactorExecutor == null) {
            compactorExecutor = Executors.newSingleThreadScheduledExecutor(new DefaultThreadFactory("compaction"));
        }
        return this.compactorExecutor;
    }

    public synchronized Compactor getCompactor() throws PulsarServerException {
        if (this.compactor == null) {
            try {
                this.compactor = new TwoPhaseCompactor(this.getConfiguration(),
                                                       getClient(), getBookKeeperClient(),
                                                       getCompactorExecutor());
            } catch (Exception e) {
                throw new PulsarServerException(e);
            }
        }
        return this.compactor;
    }

    protected synchronized OrderedScheduler getOffloaderScheduler(ServiceConfiguration conf) {
        if (this.offloaderScheduler == null) {
            this.offloaderScheduler = OrderedScheduler.newSchedulerBuilder()
                .numThreads(conf.getManagedLedgerOffloadMaxThreads())
                .name("offloader").build();
        }
        return this.offloaderScheduler;
    }

    public synchronized PulsarClient getClient() throws PulsarServerException {
        if (this.client == null) {
            try {
                ClientBuilder builder = PulsarClient.builder()
                    .serviceUrl(this.getConfiguration().isTlsEnabled()
                                ? this.brokerServiceUrlTls : this.brokerServiceUrl)
                    .enableTls(this.getConfiguration().isTlsEnabled())
                    .allowTlsInsecureConnection(this.getConfiguration().isTlsAllowInsecureConnection())
                    .tlsTrustCertsFilePath(this.getConfiguration().getTlsCertificateFilePath());
                if (isNotBlank(this.getConfiguration().getBrokerClientAuthenticationPlugin())) {
                    builder.authentication(this.getConfiguration().getBrokerClientAuthenticationPlugin(),
                                           this.getConfiguration().getBrokerClientAuthenticationParameters());
                }
                this.client = builder.build();
            } catch (Exception e) {
                throw new PulsarServerException(e);
            }
        }
        return this.client;
    }

    public synchronized PulsarAdmin getAdminClient() throws PulsarServerException {
        if (this.adminClient == null) {
            try {
                ServiceConfiguration conf = this.getConfiguration();
                String adminApiUrl = conf.isBrokerClientTlsEnabled() ? webAddressTls(config) : webAddress(config);
                PulsarAdminBuilder builder = PulsarAdmin.builder().serviceHttpUrl(adminApiUrl) //
                        .authentication( //
                                conf.getBrokerClientAuthenticationPlugin(), //
                                conf.getBrokerClientAuthenticationParameters());

                if (conf.isBrokerClientTlsEnabled()) {
                    builder.tlsTrustCertsFilePath(conf.getBrokerClientTrustCertsFilePath());
                    builder.allowTlsInsecureConnection(conf.isTlsAllowInsecureConnection());
                }

                this.adminClient = builder.build();
                LOG.info("Admin api url: " + adminApiUrl);
            } catch (Exception e) {
                throw new PulsarServerException(e);
            }
        }

        return this.adminClient;
    }

    public MetricsGenerator getMetricsGenerator() {
        return metricsGenerator;
    }

    public MessagingServiceShutdownHook getShutdownService() {
        return shutdownService;
    }

    /**
     * Advertised service address.
     *
     * @return Hostname or IP address the service advertises to the outside world.
     */
    public static String advertisedAddress(ServiceConfiguration config) {
        return ServiceConfigurationUtils.getDefaultOrConfiguredAddress(config.getAdvertisedAddress());
    }

    public static String brokerUrl(ServiceConfiguration config) {
        return brokerUrl(advertisedAddress(config), config.getBrokerServicePort());
    }

    public static String brokerUrl(String host, int port) {
        return String.format("pulsar://%s:%d", host, port);
    }

    public static String brokerUrlTls(ServiceConfiguration config) {
        if (config.isTlsEnabled()) {
            return brokerUrlTls(advertisedAddress(config), config.getBrokerServicePortTls());
        } else {
            return "";
        }
    }

    public static String brokerUrlTls(String host, int port) {
        return String.format("pulsar+ssl://%s:%d", host, port);
    }

    public static String webAddress(ServiceConfiguration config) {
        return webAddress(advertisedAddress(config), config.getWebServicePort());
    }

    public static String webAddress(String host, int port) {
        return String.format("http://%s:%d", host, port);
    }

    public static String webAddressTls(ServiceConfiguration config) {
        if (config.isTlsEnabled()) {
            return webAddressTls(advertisedAddress(config), config.getWebServicePortTls());
        } else {
            return "";
        }
    }

    public static String webAddressTls(String host, int port) {
        return String.format("https://%s:%d", host, port);
    }

    public String getBindAddress() {
        return bindAddress;
    }

    public String getAdvertisedAddress() {
        return advertisedAddress;
    }

    public String getWebServiceAddress() {
        return webServiceAddress;
    }

    public String getWebServiceAddressTls() {
        return webServiceAddressTls;
    }

    public String getBrokerServiceUrl() {
        return brokerServiceUrl;
    }

    public String getBrokerServiceUrlTls() {
        return brokerServiceUrlTls;
    }

    public AtomicReference<LoadManager> getLoadManager() {
        return loadManager;
    }

    public String getBrokerVersion() {
        return brokerVersion;
    }

    public SchemaRegistryService getSchemaRegistryService() {
        return schemaRegistryService;
    }

    private void startWorkerService() throws InterruptedException, IOException, KeeperException {
        if (functionWorkerService.isPresent()) {
            LOG.info("Starting function worker service");
            String namespace = functionWorkerService.get()
                    .getWorkerConfig().getPulsarFunctionsNamespace();
            String[] a = functionWorkerService.get().getWorkerConfig().getPulsarFunctionsNamespace().split("/");
            String property = a[0];
            String cluster = functionWorkerService.get().getWorkerConfig().getPulsarFunctionsCluster();

                /*
                multiple brokers may be trying to create the property, cluster, and namespace
                for function worker service this in parallel. The function worker service uses the namespace
                to create topics for internal function
                */

            // create property for function worker service
            try {
                NamedEntity.checkName(property);
                this.getGlobalZkCache().getZooKeeper().create(
                        AdminResource.path(POLICIES, property),
                        ObjectMapperFactory.getThreadLocal().writeValueAsBytes(
                                new TenantInfo(
                                        Sets.newHashSet(config.getSuperUserRoles()),
                                        Sets.newHashSet(cluster))),
                        ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                LOG.info("Created property {} for function worker", property);
            } catch (KeeperException.NodeExistsException e) {
                LOG.debug("Failed to create already existing property {} for function worker service", cluster, e);
            } catch (IllegalArgumentException e) {
                LOG.error("Failed to create property with invalid name {} for function worker service", cluster, e);
                throw e;
            } catch (Exception e) {
                LOG.error("Failed to create property {} for function worker", cluster, e);
                throw e;
            }

            // create cluster for function worker service
            try {
                NamedEntity.checkName(cluster);
                ClusterData clusterData = new ClusterData(this.getWebServiceAddress(), null /* serviceUrlTls */,
                        brokerServiceUrl, null /* brokerServiceUrlTls */);
                this.getGlobalZkCache().getZooKeeper().create(
                        AdminResource.path("clusters", cluster),
                        ObjectMapperFactory.getThreadLocal().writeValueAsBytes(clusterData),
                        ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                LOG.info("Created cluster {} for function worker", cluster);
            } catch (KeeperException.NodeExistsException e) {
                LOG.debug("Failed to create already existing cluster {} for function worker service", cluster, e);
            } catch (IllegalArgumentException e) {
                LOG.error("Failed to create cluster with invalid name {} for function worker service", cluster, e);
                throw e;
            } catch (Exception e) {
                LOG.error("Failed to create cluster {} for function worker service", cluster, e);
                throw e;
            }

            // create namespace for function worker service
            try {
                Policies policies = new Policies();
                policies.retention_policies = new RetentionPolicies(-1, -1);
                policies.replication_clusters = Collections.singleton(functionWorkerService.get().getWorkerConfig().getPulsarFunctionsCluster());
                int defaultNumberOfBundles = this.getConfiguration().getDefaultNumberOfNamespaceBundles();
                policies.bundles = getBundles(defaultNumberOfBundles);

                this.getConfigurationCache().policiesCache().invalidate(AdminResource.path(POLICIES, namespace));
                ZkUtils.createFullPathOptimistic(this.getGlobalZkCache().getZooKeeper(),
                        AdminResource.path(POLICIES, namespace),
                        ObjectMapperFactory.getThreadLocal().writeValueAsBytes(policies),
                        ZooDefs.Ids.OPEN_ACL_UNSAFE,
                        CreateMode.PERSISTENT);
                LOG.info("Created namespace {} for function worker service", namespace);
            } catch (KeeperException.NodeExistsException e) {
                LOG.debug("Failed to create already existing namespace {} for function worker service", namespace);
            } catch (Exception e) {
                LOG.error("Failed to create namespace {}", namespace, e);
                throw e;
            }

            InternalConfigurationData internalConf = new InternalConfigurationData(
                    this.getConfiguration().getZookeeperServers(),
                    this.getConfiguration().getConfigurationStoreServers(),
                    new ClientConfiguration().getZkLedgersRootPath());

            URI dlogURI;
            try {
                // initializing dlog namespace for function worker
                dlogURI = Utils.initializeDlogNamespace(
                        internalConf.getZookeeperServers(),
                        internalConf.getLedgersRootPath());
            } catch (IOException ioe) {
                LOG.error("Failed to initialize dlog namespace at zookeeper {} for storing function packages",
                        internalConf.getZookeeperServers(), ioe);
                throw ioe;
            }
            LOG.info("Function worker service setup completed");
            functionWorkerService.get().start(dlogURI);
            LOG.info("Function worker service started");
        }
    }
}
