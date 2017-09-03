/*
 * Copyright (c) 2016 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.couchbase.client.core.config;

import com.couchbase.client.core.CouchbaseException;
import com.couchbase.client.core.service.ServiceType;
import com.couchbase.client.core.utils.NetworkAddress;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.HashMap;
import java.util.Map;

public class DefaultPortInfo implements PortInfo {

    private final Map<ServiceType, Integer> ports;
    private final Map<ServiceType, Integer> sslPorts;
    private final NetworkAddress hostname;

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
            this.hostname = hostname == null ? null : NetworkAddress.create(hostname);
        } catch (Exception e) {
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
            } else if (service.equals("cbas")) {
                ports.put(ServiceType.ANALYTICS, port);
            } else if (service.equals("cbasSSL")) {
                sslPorts.put(ServiceType.ANALYTICS, port);
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
    public NetworkAddress hostname() {
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
