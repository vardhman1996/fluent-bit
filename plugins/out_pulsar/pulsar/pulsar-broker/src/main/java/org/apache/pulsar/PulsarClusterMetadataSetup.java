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
package org.apache.pulsar;

import static org.apache.pulsar.broker.cache.ConfigurationCacheService.POLICIES_ROOT;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.google.common.collect.Lists;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import org.apache.bookkeeper.client.BookKeeperAdmin;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.meta.HierarchicalLedgerManagerFactory;
import org.apache.bookkeeper.util.ZkUtils;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.BundlesData;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.Policies;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.apache.pulsar.common.util.ObjectMapperFactory;
import org.apache.pulsar.zookeeper.ZkBookieRackAffinityMapping;
import org.apache.pulsar.zookeeper.ZooKeeperClientFactory;
import org.apache.pulsar.zookeeper.ZooKeeperClientFactory.SessionType;
import org.apache.pulsar.zookeeper.ZookeeperClientFactoryImpl;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException.NodeExistsException;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Setup the metadata for a new Pulsar cluster
 */
public class PulsarClusterMetadataSetup {

    private static class Arguments {
        @Parameter(names = { "-c", "--cluster" }, description = "Cluster name", required = true)
        private String cluster;

        @Parameter(names = { "-uw",
                "--web-service-url" }, description = "Web-service URL for new cluster", required = true)
        private String clusterWebServiceUrl;

        @Parameter(names = { "-tw",
                "--web-service-url-tls" }, description = "Web-service URL for new cluster with TLS encryption", required = false)
        private String clusterWebServiceUrlTls;

        @Parameter(names = { "-ub",
                "--broker-service-url" }, description = "Broker-service URL for new cluster", required = false)
        private String clusterBrokerServiceUrl;

        @Parameter(names = { "-tb",
                "--broker-service-url-tls" }, description = "Broker-service URL for new cluster with TLS encryption", required = false)
        private String clusterBrokerServiceUrlTls;

        @Parameter(names = { "-zk",
                "--zookeeper" }, description = "Local ZooKeeper quorum connection string", required = true)
        private String zookeeper;

        @Parameter(names = { "-gzk",
                "--global-zookeeper" }, description = "Global ZooKeeper quorum connection string", required = false, hidden = true)
        private String globalZookeeper;

        @Parameter(names = { "-cs",
            "--configuration-store" }, description = "Configuration Store connection string", required = false)
        private String configurationStore;

        @Parameter(names = { "-h", "--help" }, description = "Show this help message")
        private boolean help = false;
    }

    public static void main(String[] args) throws Exception {
        Arguments arguments = new Arguments();
        JCommander jcommander = new JCommander();
        try {
            jcommander.addObject(arguments);
            jcommander.parse(args);
            if (arguments.help) {
                jcommander.usage();
                return;
            }
        } catch (Exception e) {
            jcommander.usage();
            throw e;
        }

        if (arguments.configurationStore == null && arguments.globalZookeeper == null) {
            System.err.println("Configuration store address argument is required (--configuration-store)");
            jcommander.usage();
            System.exit(1);
        }

        if (arguments.configurationStore != null && arguments.globalZookeeper != null) {
            System.err.println("Configuration store argument (--configuration-store) supercedes the deprecated (--global-zookeeper) argument");
            jcommander.usage();
            System.exit(1);
        }

        if (arguments.configurationStore == null) {
            arguments.configurationStore = arguments.globalZookeeper;
        }

        log.info("Setting up cluster {} with zk={} configuration-store ={}", arguments.cluster, arguments.zookeeper,
                arguments.configurationStore);
        ZooKeeperClientFactory zkfactory = new ZookeeperClientFactoryImpl();
        ZooKeeper localZk = zkfactory.create(arguments.zookeeper, SessionType.ReadWrite, 30000).get();
        ZooKeeper configStoreZk = zkfactory.create(arguments.configurationStore, SessionType.ReadWrite, 30000).get();

        // Format BookKeeper metadata
        ServerConfiguration bkConf = new ServerConfiguration();
        bkConf.setLedgerManagerFactoryClass(HierarchicalLedgerManagerFactory.class);
        bkConf.setZkServers(arguments.zookeeper);
        if (localZk.exists("/ledgers", false) == null // only format if /ledgers doesn't exist
                && !BookKeeperAdmin.format(bkConf, false /* interactive */, false /* force */)) {
            throw new IOException("Failed to initialize BookKeeper metadata");
        }

        if (localZk.exists(ZkBookieRackAffinityMapping.BOOKIE_INFO_ROOT_PATH, false) == null) {
            try {
                localZk.create(ZkBookieRackAffinityMapping.BOOKIE_INFO_ROOT_PATH, "{}".getBytes(), Ids.OPEN_ACL_UNSAFE,
                        CreateMode.PERSISTENT);
            } catch (NodeExistsException e) {
                // Ignore
            }
        }

        try {
            localZk.create("/managed-ledgers", new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        } catch (NodeExistsException e) {
            // Ignore
        }

        localZk.create("/namespace", new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);

        try {
            ZkUtils.createFullPathOptimistic(configStoreZk, POLICIES_ROOT, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE,
                    CreateMode.PERSISTENT);
        } catch (NodeExistsException e) {
            // Ignore
        }

        try {
            ZkUtils.createFullPathOptimistic(configStoreZk, "/admin/clusters", new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE,
                    CreateMode.PERSISTENT);
        } catch (NodeExistsException e) {
            // Ignore
        }

        ClusterData clusterData = new ClusterData(arguments.clusterWebServiceUrl, arguments.clusterWebServiceUrlTls,
                arguments.clusterBrokerServiceUrl, arguments.clusterBrokerServiceUrlTls);
        byte[] clusterDataJson = ObjectMapperFactory.getThreadLocal().writeValueAsBytes(clusterData);

        configStoreZk.create("/admin/clusters/" + arguments.cluster, clusterDataJson, ZooDefs.Ids.OPEN_ACL_UNSAFE,
                CreateMode.PERSISTENT);

        // Create marker for "global" cluster
        ClusterData globalClusterData = new ClusterData(null, null);
        byte[] globalClusterDataJson = ObjectMapperFactory.getThreadLocal().writeValueAsBytes(globalClusterData);

        try {
            configStoreZk.create("/admin/clusters/global", globalClusterDataJson, ZooDefs.Ids.OPEN_ACL_UNSAFE,
                    CreateMode.PERSISTENT);
        } catch (NodeExistsException e) {
            // Ignore
        }

        // Create public tenant, whitelisted to use the this same cluster, along with other clusters
        String publicTenantPath = POLICIES_ROOT + "/" + TopicName.PUBLIC_TENANT;

        Stat stat = configStoreZk.exists(publicTenantPath, false);
        if (stat == null) {
            TenantInfo publicTenant = new TenantInfo(Collections.emptySet(), Collections.singleton(arguments.cluster));

            try {
                ZkUtils.createFullPathOptimistic(configStoreZk, publicTenantPath,
                        ObjectMapperFactory.getThreadLocal().writeValueAsBytes(publicTenant),
                        ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            } catch (NodeExistsException e) {
                // Ignore
            }
        } else {
            // Update existing public tenant with new cluster
            byte[] content = configStoreZk.getData(publicTenantPath, false, null);
            TenantInfo publicTenant = ObjectMapperFactory.getThreadLocal().readValue(content, TenantInfo.class);

            // Only update z-node if the list of clusters should be modified
            if (!publicTenant.getAllowedClusters().contains(arguments.cluster)) {
                publicTenant.getAllowedClusters().add(arguments.cluster);

                configStoreZk.setData(publicTenantPath, ObjectMapperFactory.getThreadLocal().writeValueAsBytes(publicTenant),
                        stat.getVersion());
            }
        }

        // Create default namespace
        String defaultNamespacePath = POLICIES_ROOT + "/" + TopicName.PUBLIC_TENANT + "/" + TopicName.DEFAULT_NAMESPACE;
        Policies policies;

        stat = configStoreZk.exists(defaultNamespacePath, false);
        if (stat == null) {
            policies = new Policies();
            policies.bundles = getBundles(16);
            policies.replication_clusters = Collections.singleton(arguments.cluster);

            try {
                ZkUtils.createFullPathOptimistic(
                    configStoreZk,
                    defaultNamespacePath,
                    ObjectMapperFactory.getThreadLocal().writeValueAsBytes(policies),
                    ZooDefs.Ids.OPEN_ACL_UNSAFE,
                    CreateMode.PERSISTENT);
            } catch (NodeExistsException e) {
                // Ignore
            }
        } else {
            byte[] content = configStoreZk.getData(defaultNamespacePath, false, null);
            policies = ObjectMapperFactory.getThreadLocal().readValue(content, Policies.class);

            // Only update z-node if the list of clusters should be modified
            if (!policies.replication_clusters.contains(arguments.cluster)) {
                policies.replication_clusters.add(arguments.cluster);

                configStoreZk.setData(defaultNamespacePath, ObjectMapperFactory.getThreadLocal().writeValueAsBytes(policies),
                        stat.getVersion());
            }
        }

        log.info("Cluster metadata for '{}' setup correctly", arguments.cluster);
    }

    private static BundlesData getBundles(int numBundles) {
        Long maxVal = ((long) 1) << 32;
        Long segSize = maxVal / numBundles;
        List<String> partitions = Lists.newArrayList();
        partitions.add(String.format("0x%08x", 0l));
        Long curPartition = segSize;
        for (int i = 0; i < numBundles; i++) {
            if (i != numBundles - 1) {
                partitions.add(String.format("0x%08x", curPartition));
            } else {
                partitions.add(String.format("0x%08x", maxVal - 1));
            }
            curPartition += segSize;
        }
        return new BundlesData(partitions);
    }

    private static final Logger log = LoggerFactory.getLogger(PulsarClusterMetadataSetup.class);
}
