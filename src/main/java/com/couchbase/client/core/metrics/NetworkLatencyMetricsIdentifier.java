/**
 * Copyright (c) 2015 Couchbase, Inc.
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
package com.couchbase.client.core.metrics;

/**
 * The unique identifier for a network latency metric.
 *
 * This identifier represents the hierachy of a composed request alongside its response status. The hierachy is as
 * follows: host -> service -> request -> status. As an example, a real identifier might look like:
 * vnode4/192.168.56.104:11210->BINARY->UpsertRequest->SUCCESS.
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
