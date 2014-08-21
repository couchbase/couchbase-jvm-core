/**
 * Copyright (C) 2014 Couchbase, Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALING
 * IN THE SOFTWARE.
 */
package com.couchbase.client.core.config;

import com.couchbase.client.core.CouchbaseException;
import com.couchbase.client.core.service.ServiceType;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.net.InetAddress;
import java.net.URI;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;

/**
 * Default implementation of {@link NodeInfo}.
 *
 * @author Michael Nitschinger
 * @since 1.0
 */
public class DefaultNodeInfo implements NodeInfo {

    private final String viewUri;
    private final InetAddress hostname;
    private int configPort;
    private final Map<ServiceType, Integer> directServices;
    private final Map<ServiceType, Integer> sslServices;

    @JsonCreator
    public DefaultNodeInfo(
        @JsonProperty("couchApiBase") String viewUri,
        @JsonProperty("hostname") String hostname,
        @JsonProperty("ports") Map<String, Integer> ports) {
        this.viewUri = viewUri;
        try {
            this.hostname = InetAddress.getByName(trimPort(hostname));
        } catch (UnknownHostException e) {
            throw new CouchbaseException("Could not analyze hostname from config.", e);
        }
        this.directServices = parseDirectServices(ports);
        this.sslServices = new HashMap<ServiceType, Integer>();
    }

    public DefaultNodeInfo(String viewUri, InetAddress hostname, Map<ServiceType, Integer> direct, Map<ServiceType, Integer> ssl) {
        this.viewUri = viewUri;
        this.hostname = hostname;
        this.directServices = direct;
        this.sslServices = ssl;
    }

    @Override
    public String viewUri() {
        return viewUri;
    }

    @Override
    public InetAddress hostname() {
        return hostname;
    }

    @Override
    public Map<ServiceType, Integer> services() {
        return directServices;
    }

    @Override
    public Map<ServiceType, Integer> sslServices() {
        return sslServices;
    }

    private Map<ServiceType, Integer> parseDirectServices(final Map<String, Integer> input) {
        Map<ServiceType, Integer> services = new HashMap<ServiceType, Integer>();
        for (Map.Entry<String, Integer> entry : input.entrySet()) {
            String type = entry.getKey();
            Integer port = entry.getValue();
            if (type.equals("direct")) {
                services.put(ServiceType.BINARY, port);
            }
        }
        services.put(ServiceType.CONFIG, configPort);
        services.put(ServiceType.VIEW, URI.create(viewUri).getPort());
        return services;
    }

    private String trimPort(String hostname) {
        String[] parts =  hostname.split(":");
        configPort = Integer.parseInt(parts[1]);
        return parts[0];
    }

    @Override
    public String toString() {
        return "NodeInfo{" + "viewUri='" + viewUri + '\'' + ", hostname=" + hostname + ", configPort="
            + configPort + ", directServices=" + directServices + ", sslServices=" + sslServices + '}';
    }
}
