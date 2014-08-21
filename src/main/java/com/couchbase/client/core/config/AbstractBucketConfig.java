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

import java.util.ArrayList;
import java.util.List;

public abstract class AbstractBucketConfig implements BucketConfig {

    private final String name;
    private String password;
    private final BucketNodeLocator locator;
    private final String uri;
    private final String streamingUri;
    private final List<NodeInfo> nodeInfo;

    protected AbstractBucketConfig(String name, BucketNodeLocator locator, String uri, String streamingUri,
        List<NodeInfo> nodeInfos, List<PortInfo> portInfos) {
        this.name = name;
        this.locator = locator;
        this.uri = uri;
        this.streamingUri = streamingUri;
        if (portInfos == null) {
            this.nodeInfo = nodeInfos;
        } else {
            List<NodeInfo> modified = new ArrayList<NodeInfo>();
            for (int i = 0; i < nodeInfos.size(); i++) {
                modified.add(new DefaultNodeInfo(nodeInfos.get(i).viewUri(), nodeInfos.get(i).hostname(), portInfos.get(i).ports(), portInfos.get(i).sslPorts()));
            }
            this.nodeInfo = modified;
        }
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public BucketNodeLocator locator() {
        return locator;
    }

    @Override
    public String uri() {
        return uri;
    }

    @Override
    public String streamingUri() {
        return streamingUri;
    }

    @Override
    public List<NodeInfo> nodes() {
        return nodeInfo;
    }

    @Override
    public String password() {
        return password;
    }

    @Override
    public BucketConfig password(final String password) {
        this.password = password;
        return this;
    }

}
