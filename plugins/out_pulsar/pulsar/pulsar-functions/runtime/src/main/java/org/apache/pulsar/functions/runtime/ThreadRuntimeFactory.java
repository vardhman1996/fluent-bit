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

package org.apache.pulsar.functions.runtime;

import com.google.common.annotations.VisibleForTesting;

import lombok.extern.slf4j.Slf4j;

import static org.apache.commons.lang3.StringUtils.isNotBlank;

import org.apache.pulsar.client.api.ClientBuilder;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.PulsarClientException.UnsupportedAuthenticationException;
import org.apache.pulsar.functions.instance.AuthenticationConfig;
import org.apache.pulsar.functions.instance.InstanceConfig;
import org.apache.pulsar.functions.utils.functioncache.FunctionCacheManager;
import org.apache.pulsar.functions.utils.functioncache.FunctionCacheManagerImpl;

/**
 * Thread based function container factory implementation.
 */
@Slf4j
public class ThreadRuntimeFactory implements RuntimeFactory {

    private final ThreadGroup threadGroup;
    private final FunctionCacheManager fnCache;
    private final PulsarClient pulsarClient;
    private final String storageServiceUrl;
    private volatile boolean closed;

    public ThreadRuntimeFactory(String threadGroupName, String pulsarServiceUrl, String storageServiceUrl,
            AuthenticationConfig authConfig) throws Exception {
        this(threadGroupName, createPulsarClient(pulsarServiceUrl, authConfig), storageServiceUrl);
    }

    @VisibleForTesting
    public ThreadRuntimeFactory(String threadGroupName, PulsarClient pulsarClient, String storageServiceUrl) {
        this.fnCache = new FunctionCacheManagerImpl();
        this.threadGroup = new ThreadGroup(threadGroupName);
        this.pulsarClient = pulsarClient;
        this.storageServiceUrl = storageServiceUrl;
    }

    private static PulsarClient createPulsarClient(String pulsarServiceUrl, AuthenticationConfig authConfig)
            throws PulsarClientException {
        ClientBuilder clientBuilder = null;
        if (isNotBlank(pulsarServiceUrl)) {
            clientBuilder = PulsarClient.builder().serviceUrl(pulsarServiceUrl);
            if (authConfig != null) {
                if (isNotBlank(authConfig.getClientAuthenticationPlugin())
                        && isNotBlank(authConfig.getClientAuthenticationParameters())) {
                    clientBuilder.authentication(authConfig.getClientAuthenticationPlugin(),
                            authConfig.getClientAuthenticationParameters());
                }
                clientBuilder.enableTls(authConfig.isUseTls());
                clientBuilder.allowTlsInsecureConnection(authConfig.isTlsAllowInsecureConnection());
                clientBuilder.enableTlsHostnameVerification(authConfig.isTlsHostnameVerificationEnable());
                clientBuilder.tlsTrustCertsFilePath(authConfig.getTlsTrustCertsFilePath());
            }
            return clientBuilder.build();
        }
        return null;
    }
    
    @Override
    public ThreadRuntime createContainer(InstanceConfig instanceConfig, String jarFile,
                                         String originalCodeFileName,
                                         Long expectedHealthCheckInterval) {
        return new ThreadRuntime(
            instanceConfig,
            fnCache,
            threadGroup,
            jarFile,
            pulsarClient,
            storageServiceUrl);
    }

    @Override
    public void close() {
        if (closed) {
            return;
        }
        closed = true;

        threadGroup.interrupt();
        fnCache.close();
        try {
            pulsarClient.close();
        } catch (PulsarClientException e) {
            log.warn("Failed to close pulsar client when closing function container factory", e);
        }
    }
}
