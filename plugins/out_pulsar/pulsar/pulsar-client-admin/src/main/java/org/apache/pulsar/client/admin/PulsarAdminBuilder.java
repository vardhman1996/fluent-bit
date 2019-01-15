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
package org.apache.pulsar.client.admin;

import java.util.Map;

import org.apache.pulsar.client.api.Authentication;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.PulsarClientException.UnsupportedAuthenticationException;

/**
 * Builder class for a {@link PulsarAdmin} instance.
 *
 */
public interface PulsarAdminBuilder {

    /**
     * @return the new {@link PulsarAdmin} instance
     */
    PulsarAdmin build() throws PulsarClientException;

    /**
     * Create a copy of the current client builder.
     * <p>
     * Cloning the builder can be used to share an incomplete configuration and specialize it multiple times. For
     * example:
     *
     * <pre>
     * PulsarAdminBuilder builder = PulsarAdmin.builder().allowTlsInsecureConnection(false);
     *
     * PulsarAdmin client1 = builder.clone().serviceHttpUrl(URL_1).build();
     * PulsarAdmin client2 = builder.clone().serviceHttpUrl(URL_2).build();
     * </pre>
     */
    PulsarAdminBuilder clone();

    /**
     * Set the Pulsar service HTTP URL for the admin endpoint (eg. "http://my-broker.example.com:8080", or
     * "https://my-broker.example.com:8443" for TLS)
     */
    PulsarAdminBuilder serviceHttpUrl(String serviceHttpUrl);

    /**
     * Set the authentication provider to use in the Pulsar client instance.
     * <p>
     * Example:
     * <p>
     *
     * <pre>
     * <code>
     * String AUTH_CLASS = "org.apache.pulsar.client.impl.auth.AuthenticationTls";
     * String AUTH_PARAMS = "tlsCertFile:/my/cert/file,tlsKeyFile:/my/key/file";
     *
     * PulsarAdmin client = PulsarAdmin.builder()
     *          .serviceHttpUrl(SERVICE_HTTP_URL)
     *          .authentication(AUTH_CLASS, AUTH_PARAMS)
     *          .build();
     * ....
     * </code>
     * </pre>
     *
     * @param authPluginClassName
     *            name of the Authentication-Plugin you want to use
     * @param authParamsString
     *            string which represents parameters for the Authentication-Plugin, e.g., "key1:val1,key2:val2"
     * @throws UnsupportedAuthenticationException
     *             failed to instantiate specified Authentication-Plugin
     */
    PulsarAdminBuilder authentication(String authPluginClassName, String authParamsString)
            throws UnsupportedAuthenticationException;

    /**
     * Set the authentication provider to use in the Pulsar client instance.
     * <p>
     * Example:
     * <p>
     *
     * <pre>
     * <code>
     * String AUTH_CLASS = "org.apache.pulsar.client.impl.auth.AuthenticationTls";
     *
     * Map<String, String> conf = new TreeMap<>();
     * conf.put("tlsCertFile", "/my/cert/file");
     * conf.put("tlsKeyFile", "/my/key/file");
     *
     * PulsarAdmin client = PulsarAdmin.builder()
     *          .serviceHttpUrl(SERVICE_HTTP_URL)
     *          .authentication(AUTH_CLASS, conf)
     *          .build();
     * ....
     * </code>
     *
     * @param authPluginClassName
     *            name of the Authentication-Plugin you want to use
     * @param authParams
     *            map which represents parameters for the Authentication-Plugin
     * @throws UnsupportedAuthenticationException
     *             failed to instantiate specified Authentication-Plugin
     */
    PulsarAdminBuilder authentication(String authPluginClassName, Map<String, String> authParams)
            throws UnsupportedAuthenticationException;

    /**
     * Set the authentication provider to use in the Pulsar admin instance.
     * <p>
     * Example:
     * <p>
     *
     * <pre>
     * <code>
     * String AUTH_CLASS = "org.apache.pulsar.client.impl.auth.AuthenticationTls";
     *
     * Map<String, String> conf = new TreeMap<>();
     * conf.put("tlsCertFile", "/my/cert/file");
     * conf.put("tlsKeyFile", "/my/key/file");
     *
     * Authentication auth = AuthenticationFactor.create(AUTH_CLASS, conf);
     *
     * PulsarAdmin admin = PulsarAdmin.builder()
     *          .serviceHttpUrl(SERVICE_URL)
     *          .authentication(auth)
     *          .build();
     * ....
     * </code>
     * </pre>
     *
     * @param authentication
     *            an instance of the {@link Authentication} provider already constructed
     */
    PulsarAdminBuilder authentication(Authentication authentication);

    /**
     * Set the path to the trusted TLS certificate file
     *
     * @param tlsTrustCertsFilePath
     */
    PulsarAdminBuilder tlsTrustCertsFilePath(String tlsTrustCertsFilePath);

    /**
     * Configure whether the Pulsar admin client accept untrusted TLS certificate from broker <i>(default: false)</i>
     *
     * @param allowTlsInsecureConnection
     */
    PulsarAdminBuilder allowTlsInsecureConnection(boolean allowTlsInsecureConnection);

    /**
     * It allows to validate hostname verification when client connects to broker over TLS. It validates incoming x509
     * certificate and matches provided hostname(CN/SAN) with expected broker's host name. It follows RFC 2818, 3.1.
     * Server Identity hostname verification.
     *
     * @see <a href="https://tools.ietf.org/html/rfc2818">rfc2818</a>
     *
     * @param enableTlsHostnameVerification
     */
    PulsarAdminBuilder enableTlsHostnameVerification(boolean enableTlsHostnameVerification);
}
