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
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

import java.util.List;

/**
 * Represents a Couchbase Bucket Configuration.
 */
@JsonTypeInfo(
    use = JsonTypeInfo.Id.NAME,
    include = JsonTypeInfo.As.PROPERTY,
    property = "nodeLocator")
@JsonSubTypes({
    @JsonSubTypes.Type(value = CouchbaseBucketConfig.class, name = "vbucket"),
    @JsonSubTypes.Type(value = MemcachedBucketConfig.class, name = "ketama")})

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
     * @param password the password of the bucket to inject.
     * @return the config for proper chaining.
     */
    BucketConfig password(String password);

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

    /**
     * Check if the service is enabled on the bucket.
     *
     * @param type the type to check.
     * @return true if it is, false otherwise.
     */
    boolean serviceEnabled(ServiceType type);

}
