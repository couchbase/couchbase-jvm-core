package com.couchbase.client.core.config;

public class AbstractBucketConfig implements BucketConfig {

    private final String name;
    private final BucketNodeLocator locator;
    private final String uri;
    private final String streamingUri;

    protected AbstractBucketConfig(String name, BucketNodeLocator locator, String uri, String streamingUri) {
        this.name = name;
        this.locator = locator;
        this.uri = uri;
        this.streamingUri = streamingUri;
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
}
