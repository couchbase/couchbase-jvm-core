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

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

import java.util.List;

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
    @JsonSubTypes.Type(value = MemcachedBucketConfig.class, name = "ketama")
})
public interface BucketConfig {

    /**
     * The name of the bucket.
     *
     * @return name of the bucket.
     */
    String name();

    /**
     * The password of the bucket.
     *
     * @return the password of the bucket.
     */
    String password();

    /**
     * Setter to inject the password manually into the config.
     *
     * @param pasword
     * @return the config for proper chaining.
     */
    BucketConfig password(String pasword);

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

    /**
     * The list of nodes associated with this bucket.
     *
     * @return the list of nodes.
     */
    List<NodeInfo> nodes();

    /**
     * If a config is marked as tainted.
     *
     * @return true if tainted.
     */
    boolean tainted();

    /**
     * Revision number (optional) for that configuration.
     *
     * @return the rev number, might be 0.
     */
    long rev();

    /**
     * The bucket type.
     *
     * @return the bucket type.
     */
    BucketType type();

}
