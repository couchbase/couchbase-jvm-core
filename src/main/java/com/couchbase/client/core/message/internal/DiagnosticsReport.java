/*
 * Copyright (c) 2017 Couchbase, Inc.
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
package com.couchbase.client.core.message.internal;

import com.couchbase.client.core.annotations.InterfaceAudience;
import com.couchbase.client.core.annotations.InterfaceStability;
import com.couchbase.client.core.endpoint.Endpoint;
import com.couchbase.client.core.service.ServiceType;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Aggregates the health of all {@link Endpoint}s.
 *
 * @author Michael Nitschinger
 * @since 1.5.0
 */
@InterfaceAudience.Public
@InterfaceStability.Experimental
public class DiagnosticsReport {

    private static final ObjectMapper JACKSON = new ObjectMapper();

    private static final int VERSION = 1;

    private final int version;
    private final List<EndpointHealth> endpoints;
    private final String sdk;
    private final String id;

    public DiagnosticsReport(List<EndpointHealth> endpoints, String sdk, String id) {
        this.id = id == null ? UUID.randomUUID().toString() : id;
        this.endpoints = endpoints;
        this.version = VERSION;
        this.sdk = sdk;
    }

    public String id() {
        return id;
    }

    public String sdk() {
        return sdk;
    }

    public List<EndpointHealth> endpoints() {
        return endpoints;
    }

    public List<EndpointHealth> endpoints(final ServiceType type) {
        List<EndpointHealth> filtered = new ArrayList<EndpointHealth>(endpoints.size());
        for (EndpointHealth h : endpoints) {
            if (h.type().equals(type)) {
                filtered.add(h);
            }
        }
        return filtered;
    }

    /**
     * Exports this report into the standard JSON format which is consistent
     * across different language SDKs.
     *
     * @return the encoded JSON string.
     */
    public String exportToJson() {
        Map<String, Object> result = new HashMap<String, Object>();
        Map<String, List<Map<String, Object>>> services = new HashMap<String, List<Map<String, Object>>>();

        for (EndpointHealth h : endpoints) {
            String type = serviceTypeFromEnum(h.type());
            if (!services.containsKey(type)) {
                services.put(type, new ArrayList<Map<String, Object>>());
            }
            List<Map<String, Object>> eps = services.get(type);
            eps.add(h.toMap());
        }

        result.put("version", version);
        result.put("services", services);
        result.put("sdk", sdk);
        result.put("id", id);

        try {
            return JACKSON.writeValueAsString(result);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Could not encode as JSON string.", e);
        }
    }

    private static String serviceTypeFromEnum(ServiceType type) {
        switch(type) {
            case VIEW:
                return "view";
            case BINARY:
                return "kv";
            case QUERY:
                return "n1ql";
            case CONFIG:
                return "mgmt";
            case SEARCH:
                return "fts";
            case ANALYTICS:
                return "cbas";
            default:
                throw new IllegalArgumentException();
        }
    }

    @Override
    public String toString() {
        return "ServicesHealth{" +
            "version=" + version +
            ", endpoints=" + endpoints +
            '}';
    }
}
