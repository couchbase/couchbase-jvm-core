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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Map;

/**
 * Default implementation of {@link NodeInfo}.
 *
 * @author Michael Nitschinger
 * @since 1.0
 */
public class DefaultNodeInfo implements NodeInfo {

    private final String viewUri;
    private final String hostname;
    private final Map<String, Integer> ports;

    @JsonCreator
    public DefaultNodeInfo(
        @JsonProperty("couchApiBase") String viewUri,
        @JsonProperty("hostname") String hostname,
        @JsonProperty("ports") Map<String, Integer> ports) {
        this.viewUri = viewUri;
        this.hostname = hostname;
        this.ports = ports;
    }

    @Override
    public String viewUri() {
        return viewUri;
    }

    @Override
    public String hostname() {
        return hostname;
    }

    @Override
    public Map<String, Integer> ports() {
        return ports;
    }
}
