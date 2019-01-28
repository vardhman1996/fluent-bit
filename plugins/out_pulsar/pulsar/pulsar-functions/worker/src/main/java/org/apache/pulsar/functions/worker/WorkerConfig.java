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
package org.apache.pulsar.functions.worker;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.google.common.collect.Sets;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.common.configuration.PulsarConfiguration;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import lombok.experimental.Accessors;

@Data
@Setter
@Getter
@EqualsAndHashCode
@ToString
@Accessors(chain = true)
public class WorkerConfig implements Serializable, PulsarConfiguration {

    private static final long serialVersionUID = 1L;

    private String workerId;
    private String workerHostname;
    private int workerPort;
    private int workerPortTls;
    private String connectorsDirectory = "./connectors";
    private String functionMetadataTopicName;
    private String functionWebServiceUrl;
    private String pulsarServiceUrl;
    private String pulsarWebServiceUrl;
    private String clusterCoordinationTopicName;
    private String pulsarFunctionsNamespace;
    private String pulsarFunctionsCluster;
    private int numFunctionPackageReplicas;
    private String downloadDirectory;
    private String stateStorageServiceUrl;
    private String functionAssignmentTopicName;
    private String schedulerClassName;
    private long failureCheckFreqMs;
    private long rescheduleTimeoutMs;
    private int initialBrokerReconnectMaxRetries;
    private int assignmentWriteMaxRetries;
    private long instanceLivenessCheckFreqMs;
    private String clientAuthenticationPlugin;
    private String clientAuthenticationParameters;
    // Frequency how often worker performs compaction on function-topics
    private long topicCompactionFrequencySec = 30 * 60; // 30 minutes
    private int metricsSamplingPeriodSec = 60;
    /***** --- TLS --- ****/
    // Enable TLS
    private boolean tlsEnabled = false;
    // Path for the TLS certificate file
    private String tlsCertificateFilePath;
    // Path for the TLS private key file
    private String tlsKeyFilePath;
    // Path for the trusted TLS certificate file
    private String tlsTrustCertsFilePath = "";
    // Accept untrusted TLS certificate from client
    private boolean tlsAllowInsecureConnection = false;
    private boolean tlsRequireTrustedClientCertOnConnect = false;
    private boolean useTls = false;
    private boolean tlsHostnameVerificationEnable = false;
    // Enforce authentication
    private boolean authenticationEnabled = false;
    // Autentication provider name list, which is a list of class names
    private Set<String> authenticationProviders = Sets.newTreeSet();
    // Enforce authorization on accessing functions admin-api
    private boolean authorizationEnabled = false;
    // Role names that are treated as "super-user", meaning they will be able to access any admin-api
    private Set<String> superUserRoles = Sets.newTreeSet();
    
    private Properties properties = new Properties();


    @Data
    @Setter
    @Getter
    @EqualsAndHashCode
    @ToString
    public static class ThreadContainerFactory {
        private String threadGroupName;
    }
    private ThreadContainerFactory threadContainerFactory;

    @Data
    @Setter
    @Getter
    @EqualsAndHashCode
    @ToString
    public static class ProcessContainerFactory {
        private String javaInstanceJarLocation;
        private String pythonInstanceLocation;
        private String logDirectory;
    }
    private ProcessContainerFactory processContainerFactory;

    @Data
    @Setter
    @Getter
    @EqualsAndHashCode
    @ToString
    public static class KubernetesContainerFactory {
        private String k8Uri;
        private String jobNamespace;
        private String pulsarDockerImageName;
        private String pulsarRootDir;
        private Boolean submittingInsidePod;
        private String pulsarServiceUrl;
        private String pulsarAdminUrl;
        private Boolean installUserCodeDependencies;
        private Map<String, String> customLabels;
        private Integer expectedMetricsCollectionInterval;
    }
    private KubernetesContainerFactory kubernetesContainerFactory;

    public String getFunctionMetadataTopic() {
        return String.format("persistent://%s/%s", pulsarFunctionsNamespace, functionMetadataTopicName);
    }

    public String getClusterCoordinationTopic() {
        return String.format("persistent://%s/%s", pulsarFunctionsNamespace, clusterCoordinationTopicName);
    }

    public String getFunctionAssignmentTopic() {
        return String.format("persistent://%s/%s", pulsarFunctionsNamespace, functionAssignmentTopicName);
    }

    public static WorkerConfig load(String yamlFile) throws IOException {
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        return mapper.readValue(new File(yamlFile), WorkerConfig.class);
    }

    public String getWorkerId() {
        if (StringUtils.isBlank(this.workerId)) {
            this.workerId = String.format("%s-%s", this.getWorkerHostname(), this.getWorkerPort());
        }
        return this.workerId;
    }

    public String getWorkerHostname() {
        if (StringUtils.isBlank(this.workerHostname)) {
            this.workerHostname = unsafeLocalhostResolve();
        }
        return this.workerHostname;
    }

    public String getWorkerWebAddress() {
        return String.format("http://%s:%d", this.getWorkerHostname(), this.getWorkerPort());
    }

    public static String unsafeLocalhostResolve() {
        try {
            return InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException ex) {
            throw new IllegalStateException("Failed to resolve localhost name.", ex);
        }
    }
  
    @Override
    public void setProperties(Properties properties) {
        this.properties = properties;
    }
}