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
package org.apache.pulsar.proxy.server;

import com.fasterxml.jackson.jaxrs.json.JacksonJaxbJsonProvider;
import com.google.common.collect.Lists;

import io.netty.util.concurrent.DefaultThreadFactory;

import java.io.IOException;
import java.net.URI;
import java.security.GeneralSecurityException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.TimeZone;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import javax.servlet.DispatcherType;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.pulsar.broker.authentication.AuthenticationService;
import org.apache.pulsar.broker.web.AuthenticationFilter;
import org.apache.pulsar.common.util.ObjectMapperFactory;
import org.apache.pulsar.common.util.SecurityUtility;
import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.server.Slf4jRequestLog;
import org.eclipse.jetty.server.handler.ContextHandlerCollection;
import org.eclipse.jetty.server.handler.DefaultHandler;
import org.eclipse.jetty.server.handler.HandlerCollection;
import org.eclipse.jetty.server.handler.RequestLogHandler;
import org.eclipse.jetty.servlet.FilterHolder;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.eclipse.jetty.util.thread.ExecutorThreadPool;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.servlet.ServletContainer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Manages web-service startup/stop on jetty server.
 *
 */
public class WebServer {
    private static final String MATCH_ALL = "/*";

    private final Server server;
    private final ExecutorService webServiceExecutor;
    private final AuthenticationService authenticationService;
    private final List<Handler> handlers = Lists.newArrayList();
    private final ProxyConfiguration config;
    protected final int externalServicePort;

    public WebServer(ProxyConfiguration config, AuthenticationService authenticationService) {
        this.webServiceExecutor = Executors.newFixedThreadPool(32, new DefaultThreadFactory("pulsar-external-web"));
        this.server = new Server(new ExecutorThreadPool(webServiceExecutor));
        this.externalServicePort = config.getWebServicePort();
        this.authenticationService = authenticationService;
        this.config = config;

        List<ServerConnector> connectors = Lists.newArrayList();

        ServerConnector connector = new ServerConnector(server, 1, 1);
        connector.setPort(externalServicePort);
        connectors.add(connector);

        if (config.isTlsEnabledInProxy()) {
            try {
                SslContextFactory sslCtxFactory = SecurityUtility.createSslContextFactory(
                        config.isTlsAllowInsecureConnection(),
                        config.getTlsTrustCertsFilePath(),
                        config.getTlsCertificateFilePath(),
                        config.getTlsKeyFilePath(),
                        config.getTlsRequireTrustedClientCertOnConnect());
                ServerConnector tlsConnector = new ServerConnector(server, 1, 1, sslCtxFactory);
                tlsConnector.setPort(config.getWebServicePortTls());
                connectors.add(tlsConnector);
            } catch (GeneralSecurityException e) {
                throw new RuntimeException(e);
            }
        }

        // Limit number of concurrent HTTP connections to avoid getting out of file descriptors
        connectors.stream().forEach(c -> c.setAcceptQueueSize(1024 / connectors.size()));
        server.setConnectors(connectors.toArray(new ServerConnector[connectors.size()]));
    }

    public URI getServiceUri() {
        return this.server.getURI();
    }

    public void addServlet(String basePath, ServletHolder servletHolder) {
        addServlet(basePath, servletHolder, Collections.emptyList());
    }

    public void addServlet(String basePath, ServletHolder servletHolder, List<Pair<String, Object>> attributes) {
        ServletContextHandler context = new ServletContextHandler(ServletContextHandler.SESSIONS);
        context.setContextPath(basePath);
        context.addServlet(servletHolder, "/*");
        for (Pair<String, Object> attribute : attributes) {
            context.setAttribute(attribute.getLeft(), attribute.getRight());
        }
        if (config.isAuthenticationEnabled()) {
            FilterHolder filter = new FilterHolder(new AuthenticationFilter(authenticationService));
            context.addFilter(filter, MATCH_ALL, EnumSet.allOf(DispatcherType.class));
        }

        handlers.add(context);
    }

    public void addRestResources(String basePath, String javaPackages, String attribute, Object attributeValue) {
        JacksonJaxbJsonProvider provider = new JacksonJaxbJsonProvider();
        provider.setMapper(ObjectMapperFactory.create());
        ResourceConfig config = new ResourceConfig();
        config.packages("jersey.config.server.provider.packages", javaPackages);
        config.register(provider);
        ServletHolder servletHolder = new ServletHolder(new ServletContainer(config));
        servletHolder.setAsyncSupported(true);
        ServletContextHandler context = new ServletContextHandler(ServletContextHandler.SESSIONS);
        context.setContextPath(basePath);
        context.addServlet(servletHolder, "/*");
        context.setAttribute(attribute, attributeValue);
        handlers.add(context);
    }

    public int getExternalServicePort() {
        return externalServicePort;
    }

    public void start() throws Exception {
        RequestLogHandler requestLogHandler = new RequestLogHandler();
        Slf4jRequestLog requestLog = new Slf4jRequestLog();
        requestLog.setExtended(true);
        requestLog.setLogTimeZone(TimeZone.getDefault().getID());
        requestLog.setLogLatency(true);
        requestLogHandler.setRequestLog(requestLog);
        handlers.add(0, new ContextHandlerCollection());
        handlers.add(requestLogHandler);

        ContextHandlerCollection contexts = new ContextHandlerCollection();
        contexts.setHandlers(handlers.toArray(new Handler[handlers.size()]));

        HandlerCollection handlerCollection = new HandlerCollection();
        handlerCollection.setHandlers(new Handler[] { contexts, new DefaultHandler(), requestLogHandler });
        server.setHandler(handlerCollection);

        try {
            server.start();
        } catch (Exception e) {
            List<Integer> ports = new ArrayList<>();
            for (Connector c : server.getConnectors()) {
                if (c instanceof ServerConnector) {
                    ports.add(((ServerConnector) c).getPort());
                }
            }
            throw new IOException("Failed to start HTTP server on ports " + ports, e);
        }

        log.info("Server started at end point {}", getServiceUri());
    }

    public void stop() throws Exception {
        server.stop();
        webServiceExecutor.shutdown();
        log.info("Server stopped successfully");
    }

    public boolean isStarted() {
        return server.isStarted();
    }

    private static final Logger log = LoggerFactory.getLogger(WebServer.class);
}
