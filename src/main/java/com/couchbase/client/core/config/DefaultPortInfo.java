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
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;

public class DefaultPortInfo implements PortInfo {

    private final Map<ServiceType, Integer> ports;
    private final Map<ServiceType, Integer> sslPorts;
    private final InetAddress hostname;

    /**
     * Creates a new {@link DefaultPortInfo}.
     *
     * Note that if the hostname is null (not provided by the server), it is explicitly set to null because otherwise
     * the loaded InetAddress would point to localhost.
     *
     * @param services the list of services mapping to ports.
     */
    @JsonCreator
    public DefaultPortInfo(
        @JsonProperty("services") Map<String, Integer> services,
        @JsonProperty("hostname") String hostname
    ) {
        ports = new HashMap<ServiceType, Integer>();
        sslPorts = new HashMap<ServiceType, Integer>();
        try {
            this.hostname = hostname == null ? null : InetAddress.getByName(hostname);
        } catch (UnknownHostException e) {
            throw new CouchbaseException("Could not analyze hostname from config.", e);
        }

        for (Map.Entry<String, Integer> entry : services.entrySet()) {
            String service = entry.getKey();
            int port = entry.getValue();
            if (service.equals("mgmt")) {
                ports.put(ServiceType.CONFIG, port);
            } else if (service.equals("capi")) {
                ports.put(ServiceType.VIEW, port);
            } else if (service.equals("kv")) {
                ports.put(ServiceType.BINARY, port);
            } else if (service.equals("kvSSL")) {
                sslPorts.put(ServiceType.BINARY, port);
            } else if (service.equals("capiSSL")) {
                sslPorts.put(ServiceType.VIEW, port);
            } else if (service.equals("mgmtSSL")) {
                sslPorts.put(ServiceType.CONFIG, port);
            } else if (service.equals("n1ql")) {
                ports.put(ServiceType.QUERY, port);
            } else if (service.equals("n1qlSSL")) {
                sslPorts.put(ServiceType.QUERY, port);
            } else if (service.equals("fts")) {
                ports.put(ServiceType.SEARCH, port);
            }
        }
    }

    @Override
    public Map<ServiceType, Integer> ports() {
        return ports;
    }

    @Override
    public Map<ServiceType, Integer> sslPorts() {
        return sslPorts;
    }

    @Override
    public InetAddress hostname() {
        return hostname;
    }

    @Override
    public String toString() {
        return "DefaultPortInfo{"
            + "ports=" + ports
            + ", sslPorts=" + sslPorts
            + ", hostname='" + hostname
            + '\'' + '}';
    }
}
