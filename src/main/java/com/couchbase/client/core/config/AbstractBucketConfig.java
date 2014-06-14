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
