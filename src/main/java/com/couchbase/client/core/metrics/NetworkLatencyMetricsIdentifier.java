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
package com.couchbase.client.core.metrics;

/**
 * The unique identifier for a network latency metric.
 *
 * <p>This identifier represents the hierachy of a composed request alongside its response status. The hierarchy is as
 * follows: host -&gt; service -&gt; request -&gt; status. As an example, a real identifier might look like:
 * vnode4/192.168.56.104:11210-&gt;BINARY-&gt;UpsertRequest-&gt;SUCCESS.</p>
 *
 * @author Michael Nitschinger
 * @since 1.2.0
 */
public class NetworkLatencyMetricsIdentifier implements LatencyMetricsIdentifier {

    private final String host;
    private final String service;
    private final String request;
    private final String status;

    public NetworkLatencyMetricsIdentifier(String host, String service, String request, String status) {
        this.host = host;
        this.service = service;
        this.request = request;
        this.status = status;
    }

    public String host() {
        return host;
    }

    public String service() {
        return service;
    }

    public String request() {
        return request;
    }

    public String status() {
        return status;
    }

    @Override
    public int compareTo(LatencyMetricsIdentifier o) {
        return toString().compareTo(o.toString());
    }

    @Override
    public String toString() {
        return host + "->" + service + "->" + request + "->" + status;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        NetworkLatencyMetricsIdentifier that = (NetworkLatencyMetricsIdentifier) o;

        if (host != null ? !host.equals(that.host) : that.host != null) return false;
        if (service != null ? !service.equals(that.service) : that.service != null) return false;
        if (request != null ? !request.equals(that.request) : that.request != null) return false;
        return !(status != null ? !status.equals(that.status) : that.status != null);

    }

    @Override
    public int hashCode() {
        int result = host != null ? host.hashCode() : 0;
        result = 31 * result + (service != null ? service.hashCode() : 0);
        result = 31 * result + (request != null ? request.hashCode() : 0);
        result = 31 * result + (status != null ? status.hashCode() : 0);
        return result;
    }
}
