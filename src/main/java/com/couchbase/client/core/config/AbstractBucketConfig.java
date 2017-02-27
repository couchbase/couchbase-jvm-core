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

import com.couchbase.client.core.service.ServiceType;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public abstract class AbstractBucketConfig implements BucketConfig {

    private final String name;
    private String password;
    private final BucketNodeLocator locator;
    private final String uri;
    private final String streamingUri;
    private final List<NodeInfo> nodeInfo;
    private final int enabledServices;
    private final List<BucketCapabilities> bucketCapabilities;

    protected AbstractBucketConfig(String name, BucketNodeLocator locator, String uri, String streamingUri,
        List<NodeInfo> nodeInfos, List<PortInfo> portInfos, List<BucketCapabilities> bucketCapabilities) {
        this.name = name;
        this.locator = locator;
        this.uri = uri;
        this.streamingUri = streamingUri;
        this.bucketCapabilities = bucketCapabilities;
        this.nodeInfo = portInfos == null ? nodeInfos : nodeInfoFromExtended(portInfos, nodeInfos);
        int es = 0;
        for (NodeInfo info : nodeInfo) {
            for (ServiceType type : info.services().keySet()) {
                es |= 1 << type.ordinal();
            }
            for (ServiceType type : info.sslServices().keySet()) {
                es |= 1 << type.ordinal();
            }
        }
        this.enabledServices = es;
    }

    /**
     * Helper method to create the {@link NodeInfo}s from from the extended node information.
     *
     * In older server versions (< 3.0.2) the nodesExt part does not carry a hostname, so as a fallback the hostname
     * is loaded from the node info if needed.
     *
     * @param nodesExt the extended information.
     * @return the generated node infos.
     */
    private List<NodeInfo> nodeInfoFromExtended(final List<PortInfo> nodesExt, final List<NodeInfo> nodeInfos) {
        List<NodeInfo> converted = new ArrayList<NodeInfo>(nodesExt.size());
        for (int i = 0; i < nodesExt.size(); i++) {
            InetAddress hostname = nodesExt.get(i).hostname();
            if (hostname == null) {
                hostname = nodeInfos.get(i).hostname();
            }
            Map<ServiceType, Integer> ports = nodesExt.get(i).ports();
            Map<ServiceType, Integer> sslPorts = nodesExt.get(i).sslPorts();

            // this is an ephemeral bucket (not supporting views), don't enable views!
            if (!bucketCapabilities.contains(BucketCapabilities.COUCHAPI)) {
                ports.remove(ServiceType.VIEW);
                sslPorts.remove(ServiceType.VIEW);
            }

            converted.add(new DefaultNodeInfo(hostname, ports, sslPorts));
        }
        return converted;
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

    @Override
    public boolean serviceEnabled(ServiceType type) {
        return (enabledServices & (1 << type.ordinal())) != 0;
    }
}
