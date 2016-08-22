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

    private RawQueryRequest(String jsonQuery, String bucket, String password, InetAddress targetNode) {
        super(jsonQuery, true, bucket, password, targetNode);
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
     * @return a {@link RawQueryRequest} for this full query.
     */
    public static RawQueryRequest jsonQuery(String jsonQuery, String bucket, String password) {
        return new RawQueryRequest(jsonQuery, bucket, password, null);
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
     * @return a {@link RawQueryRequest} for this full query.
     */
    public static RawQueryRequest jsonQuery(String jsonQuery, String bucket, String password, InetAddress targetNode) {
        return new RawQueryRequest(jsonQuery, bucket, password, targetNode);
    }
}