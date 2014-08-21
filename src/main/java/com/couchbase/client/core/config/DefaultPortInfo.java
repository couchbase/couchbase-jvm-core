package com.couchbase.client.core.config;

import com.couchbase.client.core.service.ServiceType;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.HashMap;
import java.util.Map;

/**
 * .
 *
 * @author Michael Nitschinger
 */
public class DefaultPortInfo implements PortInfo {

    private final Map<ServiceType, Integer> ports;
    private final Map<ServiceType, Integer> sslPorts;

    @JsonCreator
    public DefaultPortInfo(@JsonProperty("services") Map<String, Integer> services) {
        ports = new HashMap<ServiceType, Integer>();
        sslPorts = new HashMap<ServiceType, Integer>();

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
    public String toString() {
        return "DefaultPortInfo{" + "ports=" + ports + ", sslPorts=" + sslPorts + '}';
    }
}
