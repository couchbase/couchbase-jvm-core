package com.couchbase.client.core.config;

import java.util.List;

public abstract class AbstractBucketConfig implements BucketConfig {

    private final String name;
    private final BucketNodeLocator locator;
    private final String uri;
    private final String streamingUri;
    private final List<NodeInfo> nodeInfo;

    protected AbstractBucketConfig(String name, BucketNodeLocator locator, String uri, String streamingUri,
        List<NodeInfo> nodeInfo) {
        this.name = name;
        this.locator = locator;
        this.uri = uri;
        this.streamingUri = streamingUri;
        this.nodeInfo = nodeInfo;
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
}
