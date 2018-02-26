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

package com.couchbase.client.core.message.query;

import com.couchbase.client.core.annotations.InterfaceAudience;
import com.couchbase.client.core.annotations.InterfaceStability;
import com.couchbase.client.core.message.AbstractCouchbaseRequest;
import com.couchbase.client.core.message.PrelocatedRequest;

import java.net.InetAddress;

/**
 * A {@link QueryRequest} that can also be {@link PrelocatedRequest prelocated}, but expects a
 * response with the uninterpreted JSON payload returned by the query service.
 *
 * @author Simon Baslé
 * @since 1.3
 */
@InterfaceStability.Uncommitted
@InterfaceAudience.Public
public class RawQueryRequest extends GenericQueryRequest {

    private RawQueryRequest(String jsonQuery, String bucket, String username, String password, InetAddress targetNode, String contextId) {
        super(jsonQuery, true, bucket, username, password, targetNode, contextId);
    }

    /**
     * Create a {@link RawQueryRequest} containing a full N1QL query in Json form (including additional
     * query parameters like named arguments, etc...).
     *
     * The simplest form of such a query is a single statement encapsulated in a json query object:
     * <pre>{"statement":"SELECT * FROM default"}</pre>.
     *
     * @param jsonQuery the N1QL query in json form.
     * @param bucket the bucket on which to perform the query.
     * @param password the password for the target bucket.
     * @param contextId the context id to store and use for tracing purposes.
     * @return a {@link RawQueryRequest} for this full query.
     */
    public static RawQueryRequest jsonQuery(String jsonQuery, String bucket, String password, String contextId) {
        return new RawQueryRequest(jsonQuery, bucket, bucket, password, null, contextId);
    }

    /**
     * Create a {@link RawQueryRequest} containing a full N1QL query in Json form (including additional
     * query parameters like named arguments, etc...).
     *
     * The simplest form of such a query is a single statement encapsulated in a json query object:
     * <pre>{"statement":"SELECT * FROM default"}</pre>.
     *
     * @param jsonQuery the N1QL query in json form.
     * @param bucket the bucket on which to perform the query.
     * @param username the user authorized for bucket access.
     * @param password the password for the user.
     * @param contextId the context id to store and use for tracing purposes.
     * @return a {@link RawQueryRequest} for this full query.
     */
    public static RawQueryRequest jsonQuery(String jsonQuery, String bucket, String username, String password, String contextId) {
        return new RawQueryRequest(jsonQuery, bucket, username, password, null, contextId);
    }

    /**
     * Create a {@link RawQueryRequest} containing a full N1QL query in Json form (including additional
     * query parameters like named arguments, etc...).
     *
     * The simplest form of such a query is a single statement encapsulated in a json query object:
     * <pre>{"statement":"SELECT * FROM default"}</pre>.
     *
     * @param jsonQuery the N1QL query in json form.
     * @param bucket the bucket on which to perform the query.
     * @param password the password for the target bucket.
     * @param targetNode the node on which to execute this request (or null to let the core locate and choose one).
     * @param contextId the context id to store and use for tracing purposes.
     * @return a {@link RawQueryRequest} for this full query.
     */
    public static RawQueryRequest jsonQuery(String jsonQuery, String bucket, String password, InetAddress targetNode, String contextId) {
        return new RawQueryRequest(jsonQuery, bucket, bucket, password, targetNode, contextId);
    }

    /**
     * Create a {@link RawQueryRequest} containing a full N1QL query in Json form (including additional
     * query parameters like named arguments, etc...).
     *
     * The simplest form of such a query is a single statement encapsulated in a json query object:
     * <pre>{"statement":"SELECT * FROM default"}</pre>.
     *
     * @param jsonQuery the N1QL query in json form.
     * @param bucket the bucket on which to perform the query.
     * @param username the user authorized for bucket access.
     * @param password the password for the user.
     * @param targetNode the node on which to execute this request (or null to let the core locate and choose one).
     * @param contextId the context id to store and use for tracing purposes.
     * @return a {@link RawQueryRequest} for this full query.
     */
    public static RawQueryRequest jsonQuery(String jsonQuery, String bucket, String username, String password, InetAddress targetNode, String contextId) {
        return new RawQueryRequest(jsonQuery, bucket, username, password, targetNode, contextId);
    }
}