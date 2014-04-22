package com.couchbase.client.core.config;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

/**
 * Represents a Couchbase Bucket Configuration.
 */
@JsonTypeInfo(
    use = JsonTypeInfo.Id.NAME,
    include = JsonTypeInfo.As.PROPERTY,
    property = "nodeLocator"
)
@JsonSubTypes({
    @JsonSubTypes.Type(value = CouchbaseBucketConfig.class, name = "vbucket"),
    @JsonSubTypes.Type(value = MemcacheBucketConfig.class, name = "ketama")
})
public interface BucketConfig {

    /**
     * The name of the bucket.
     *
     * @return name of the bucket.
     */
    String name();

    /**
     * The type of node locator in use for this bucket.
     *
     * @return the node locator type.
     */
    BucketNodeLocator locator();

    /**
     * The HTTP Uri for this bucket configuration.
     *
     * @return the uri.
     */
    String uri();

    /**
     * The HTTP Streaming URI for this bucket.
     *
     * @return the streaming uri.
     */
    String streamingUri();

}
