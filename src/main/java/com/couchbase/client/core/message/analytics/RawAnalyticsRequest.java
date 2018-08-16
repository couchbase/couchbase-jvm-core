/*
 * Copyright (c) 2017 Couchbase, Inc.
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
package com.couchbase.client.core.message.analytics;

import com.couchbase.client.core.annotations.InterfaceAudience;
import com.couchbase.client.core.annotations.InterfaceStability;
import com.couchbase.client.core.message.PrelocatedRequest;
import com.couchbase.client.core.message.query.QueryRequest;

import java.net.InetAddress;

/**
 * A {@link AnalyticsRequest} that can also be {@link PrelocatedRequest prelocated}, but expects a
 * response with the uninterpreted JSON payload returned by the query service.
 *
 * @author Simon Baslé
 * @since 1.4.3
 */
@InterfaceStability.Uncommitted
@InterfaceAudience.Public
public class RawAnalyticsRequest extends GenericAnalyticsRequest {

    private RawAnalyticsRequest(String jsonQuery, String bucket, String username, String password, InetAddress targetNode,
                                int priority) {
        super(jsonQuery, true, bucket, username, password, targetNode, priority);
    }

    /**
     * Create a {@link RawAnalyticsRequest} containing a full Analytics query in Json form (including additional
     * query parameters).
     *
     * The simplest form of such a query is a single statement encapsulated in a json query object:
     * <pre>{"statement":"SELECT * FROM default"}</pre>.
     *
     * @param jsonQuery the Analytics query in json form.
     * @param bucket the bucket on which to perform the query.
     * @param password the password for the target bucket.
     * @return a {@link RawAnalyticsRequest} for this full query.
     */
    public static RawAnalyticsRequest jsonQuery(String jsonQuery, String bucket, String password) {
        return new RawAnalyticsRequest(jsonQuery, bucket, bucket, password, null, GenericAnalyticsRequest.NO_PRIORITY);
    }

    /**
     * Create a {@link RawAnalyticsRequest} containing a full Analytics query in Json form (including additional
     * query parameters).
     *
     * The simplest form of such a query is a single statement encapsulated in a json query object:
     * <pre>{"statement":"SELECT * FROM default"}</pre>.
     *
     * @param jsonQuery the Analytics query in json form.
     * @param bucket the bucket on which to perform the query.
     * @param username the user authorized for bucket access.
     * @param password the password for the user.
     * @return a {@link RawAnalyticsRequest} for this full query.
     */
    public static RawAnalyticsRequest jsonQuery(String jsonQuery, String bucket, String username, String password) {
        return new RawAnalyticsRequest(jsonQuery, bucket, username, password, null, GenericAnalyticsRequest.NO_PRIORITY);
    }

    /**
     * Create a {@link RawAnalyticsRequest} containing a full Analytics query in Json form (including additional
     * query parameters).
     *
     * The simplest form of such a query is a single statement encapsulated in a json query object:
     * <pre>{"statement":"SELECT * FROM default"}</pre>.
     *
     * @param jsonQuery the Analytics query in json form.
     * @param bucket the bucket on which to perform the query.
     * @param password the password for the target bucket.
     * @param targetNode the node on which to execute this request (or null to let the core locate and choose one).
     * @return a {@link RawAnalyticsRequest} for this full query.
     */
    public static RawAnalyticsRequest jsonQuery(String jsonQuery, String bucket, String password, InetAddress targetNode) {
        return new RawAnalyticsRequest(jsonQuery, bucket, bucket, password, targetNode, GenericAnalyticsRequest.NO_PRIORITY);
    }

    /**
     * Create a {@link RawAnalyticsRequest} containing a full Analytics query in Json form (including additional
     * query parameters).
     *
     * The simplest form of such a query is a single statement encapsulated in a json query object:
     * <pre>{"statement":"SELECT * FROM default"}</pre>.
     *
     * @param jsonQuery the Analytics query in json form.
     * @param bucket the bucket on which to perform the query.
     * @param username the user authorized for bucket access.
     * @param password the password for the target bucket.
     * @param targetNode the node on which to execute this request (or null to let the core locate and choose one).
     * @return a {@link RawAnalyticsRequest} for this full query.
     */
    public static RawAnalyticsRequest jsonQuery(String jsonQuery, String bucket, String username, String password, InetAddress targetNode) {
        return new RawAnalyticsRequest(jsonQuery, bucket, username, password, targetNode, GenericAnalyticsRequest.NO_PRIORITY);
    }
}