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
package org.apache.pulsar.io;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import java.lang.reflect.Method;
import java.net.URL;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.bookkeeper.test.PortManager;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.ServiceConfigurationUtils;
import org.apache.pulsar.broker.authentication.AuthenticationProviderTls;
import org.apache.pulsar.broker.authentication.AuthenticationService;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.Tenants;
import org.apache.pulsar.client.api.Authentication;
import org.apache.pulsar.client.impl.auth.AuthenticationTls;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.apache.pulsar.functions.proto.Function.FunctionDetails;
import org.apache.pulsar.functions.sink.PulsarSink;
import org.apache.pulsar.functions.utils.Utils;
import org.apache.pulsar.functions.worker.FunctionMetaDataManager;
import org.apache.pulsar.functions.worker.WorkerConfig;
import org.apache.pulsar.functions.worker.WorkerService;
import org.apache.pulsar.functions.worker.rest.WorkerServer;
import org.apache.pulsar.zookeeper.LocalBookkeeperEnsemble;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import org.apache.pulsar.client.admin.PulsarAdminException;

import com.google.common.collect.Sets;

/**
 * Test Pulsar function TLS authentication
 *
 */
public class PulsarFunctionTlsTest {
    LocalBookkeeperEnsemble bkEnsemble;

    ServiceConfiguration config;
    WorkerConfig workerConfig;
    URL urlTls;
    WorkerService functionsWorkerService;
    final String tenant = "external-repl-prop";
    String pulsarFunctionsNamespace = tenant + "/use/pulsar-function-admin";
    String workerId;
    WorkerServer workerServer;
    PulsarAdmin functionAdmin;
    private final int ZOOKEEPER_PORT = PortManager.nextFreePort();
    private final int workerServicePort = PortManager.nextFreePort();
    private final int workerServicePortTls = PortManager.nextFreePort();

    private final String TLS_SERVER_CERT_FILE_PATH = "./src/test/resources/authentication/tls/broker-cert.pem";
    private final String TLS_SERVER_KEY_FILE_PATH = "./src/test/resources/authentication/tls/broker-key.pem";
    private final String TLS_CLIENT_CERT_FILE_PATH = "./src/test/resources/authentication/tls/client-cert.pem";
    private final String TLS_CLIENT_KEY_FILE_PATH = "./src/test/resources/authentication/tls/client-key.pem";

    private static final Logger log = LoggerFactory.getLogger(PulsarFunctionTlsTest.class);

    @BeforeMethod
    void setup(Method method) throws Exception {

        log.info("--- Setting up method {} ---", method.getName());

        // Start local bookkeeper ensemble
        bkEnsemble = new LocalBookkeeperEnsemble(3, ZOOKEEPER_PORT, () -> PortManager.nextFreePort());
        bkEnsemble.start();

        config = spy(new ServiceConfiguration());
        config.setClusterName("use");
        Set<String> superUsers = Sets.newHashSet("superUser");
        config.setSuperUserRoles(superUsers);
        config.setZookeeperServers("127.0.0.1" + ":" + ZOOKEEPER_PORT);
        Set<String> providers = new HashSet<>();
        providers.add(AuthenticationProviderTls.class.getName());
        config.setAuthenticationEnabled(true);
        config.setAuthenticationProviders(providers);
        config.setTlsEnabled(true);
        config.setTlsCertificateFilePath(TLS_SERVER_CERT_FILE_PATH);
        config.setTlsKeyFilePath(TLS_SERVER_KEY_FILE_PATH);
        config.setTlsAllowInsecureConnection(true);
        functionsWorkerService = spy(createPulsarFunctionWorker(config));
        AuthenticationService authenticationService = new AuthenticationService(config);
        when(functionsWorkerService.getAuthenticationService()).thenReturn(authenticationService);
        when(functionsWorkerService.isInitialized()).thenReturn(true);

        PulsarAdmin admin = mock(PulsarAdmin.class);
        Tenants tenants = mock(Tenants.class);
        when(admin.tenants()).thenReturn(tenants);
        when(functionsWorkerService.getBrokerAdmin()).thenReturn(admin);
        Set<String> admins = Sets.newHashSet("superUser");
        TenantInfo tenantInfo = new TenantInfo(admins, null);
        when(tenants.getTenantInfo(any())).thenReturn(tenantInfo);

        // mock: once authentication passes, function should return response: function already exist
        FunctionMetaDataManager dataManager = mock(FunctionMetaDataManager.class);
        when(dataManager.containsFunction(any(), any(), any())).thenReturn(true);
        when(functionsWorkerService.getFunctionMetaDataManager()).thenReturn(dataManager);

        workerServer = new WorkerServer(functionsWorkerService);
        workerServer.start();
        Thread.sleep(2000);
        String functionTlsUrl = String.format("https://%s:%s",
                functionsWorkerService.getWorkerConfig().getWorkerHostname(), workerServicePortTls);

        Map<String, String> authParams = new HashMap<>();
        authParams.put("tlsCertFile", TLS_CLIENT_CERT_FILE_PATH);
        authParams.put("tlsKeyFile", TLS_CLIENT_KEY_FILE_PATH);
        Authentication authTls = new AuthenticationTls();
        authTls.configure(authParams);

        functionAdmin = PulsarAdmin.builder().serviceHttpUrl(functionTlsUrl)
                .tlsTrustCertsFilePath(TLS_CLIENT_CERT_FILE_PATH).allowTlsInsecureConnection(true)
                .authentication(authTls).build();

        Thread.sleep(100);
    }

    @AfterMethod
    void shutdown() throws Exception {
        log.info("--- Shutting down ---");
        functionAdmin.close();
        bkEnsemble.stop();
        workerServer.stop();
        functionsWorkerService.stop();
    }

    private WorkerService createPulsarFunctionWorker(ServiceConfiguration config) {
        workerConfig = new WorkerConfig();
        workerConfig.setPulsarFunctionsNamespace(pulsarFunctionsNamespace);
        workerConfig.setSchedulerClassName(
                org.apache.pulsar.functions.worker.scheduler.RoundRobinScheduler.class.getName());
        workerConfig.setThreadContainerFactory(new WorkerConfig.ThreadContainerFactory().setThreadGroupName("use"));
        // worker talks to local broker
        workerConfig.setPulsarServiceUrl("pulsar://127.0.0.1:" + config.getBrokerServicePortTls());
        workerConfig.setPulsarWebServiceUrl("https://127.0.0.1:" + config.getWebServicePortTls());
        workerConfig.setFailureCheckFreqMs(100);
        workerConfig.setNumFunctionPackageReplicas(1);
        workerConfig.setClusterCoordinationTopicName("coordinate");
        workerConfig.setFunctionAssignmentTopicName("assignment");
        workerConfig.setFunctionMetadataTopicName("metadata");
        workerConfig.setInstanceLivenessCheckFreqMs(100);
        workerConfig.setWorkerPort(workerServicePort);
        workerConfig.setPulsarFunctionsCluster(config.getClusterName());
        String hostname = ServiceConfigurationUtils.getDefaultOrConfiguredAddress(config.getAdvertisedAddress());
        this.workerId = "c-" + config.getClusterName() + "-fw-" + hostname + "-" + workerConfig.getWorkerPort();
        workerConfig.setWorkerHostname(hostname);
        workerConfig.setWorkerId(workerId);

        workerConfig.setClientAuthenticationPlugin(AuthenticationTls.class.getName());
        workerConfig.setClientAuthenticationParameters(
                String.format("tlsCertFile:%s,tlsKeyFile:%s", TLS_CLIENT_CERT_FILE_PATH, TLS_CLIENT_KEY_FILE_PATH));
        workerConfig.setUseTls(true);
        workerConfig.setTlsAllowInsecureConnection(true);
        workerConfig.setTlsTrustCertsFilePath(TLS_CLIENT_CERT_FILE_PATH);

        workerConfig.setWorkerPortTls(workerServicePortTls);
        workerConfig.setTlsEnabled(true);
        workerConfig.setTlsCertificateFilePath(TLS_SERVER_CERT_FILE_PATH);
        workerConfig.setTlsKeyFilePath(TLS_SERVER_KEY_FILE_PATH);

        workerConfig.setAuthenticationEnabled(true);
        workerConfig.setAuthorizationEnabled(true);

        return new WorkerService(workerConfig);
    }

    @Test
    public void testAuthorization() throws Exception {

        final String namespacePortion = "io";
        final String replNamespace = tenant + "/" + namespacePortion;
        final String sinkTopic = "persistent://" + replNamespace + "/output";
        final String functionName = "PulsarSink-test";
        final String subscriptionName = "test-sub";

        String jarFilePathUrl = String.format("%s:%s", Utils.FILE,
                PulsarSink.class.getProtectionDomain().getCodeSource().getLocation().getPath());
        FunctionDetails functionDetails = PulsarSinkE2ETest.createSinkConfig(jarFilePathUrl, tenant, namespacePortion,
                functionName, "my.*", sinkTopic, subscriptionName);

        try {
            functionAdmin.functions().createFunctionWithUrl(functionDetails, jarFilePathUrl);
            fail("Authentication should pass but call should fail with function already exist");
        } catch (PulsarAdminException e) {
            assertTrue(e.getMessage().contains("already exists"));
        }

    }

}