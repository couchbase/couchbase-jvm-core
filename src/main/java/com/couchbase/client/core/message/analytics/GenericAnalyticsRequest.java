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
import com.couchbase.client.core.message.AbstractCouchbaseRequest;
import com.couchbase.client.core.message.PrelocatedRequest;
import com.couchbase.client.core.message.query.QueryRequest;
import com.couchbase.client.core.tracing.ThresholdLogReporter;
import io.opentracing.Span;
import io.opentracing.tag.Tags;

import java.net.InetAddress;

/**
 * For the lack of a better name, a analytics request against a analytics server.
 *
 * @author Michael Nitschinger
 * @since 1.4.3
 */
@InterfaceStability.Uncommitted
@InterfaceAudience.Public
public class GenericAnalyticsRequest extends AbstractCouchbaseRequest implements AnalyticsRequest, PrelocatedRequest {

    public static final int NO_PRIORITY = 0;

    private final String query;
    private final boolean jsonFormat;
    private final InetAddress targetNode;
    private final int priority;

    protected GenericAnalyticsRequest(String query, boolean jsonFormat, String bucket, String username, String password,
        InetAddress targetNode, int priority) {
        super(bucket, username, password);
        this.query = query;
        this.jsonFormat = jsonFormat;
        this.targetNode = targetNode;
        this.priority = priority;
    }

    @Override
    protected void afterSpanSet(Span span) {
        span.setTag(Tags.PEER_SERVICE.getKey(), ThresholdLogReporter.SERVICE_ANALYTICS);
    }

    public String query() {
        return query;
    }

    public int priority() {
        return priority;
    }

    public boolean isJsonFormat() {
        return jsonFormat;
    }

    @Override
    public InetAddress sendTo() {
        return targetNode;
    }

    /**
     * Creates a {@link GenericAnalyticsRequest} and mark it as containing a single simple statement
     * (e.g. "SELECT * FROM default").
     *
     * @param statement the Analytics query statement to perform.
     * @param bucket the bucket on which to search.
     * @param password the password for the target bucket.
     * @return a {@link GenericAnalyticsRequest} for this simple statement.
     */
    public static GenericAnalyticsRequest simpleStatement(String statement, String bucket, String password) {
        return new GenericAnalyticsRequest(statement, false, bucket, bucket, password, null, NO_PRIORITY);
    }

    /**
     * Creates a {@link GenericAnalyticsRequest} and mark it as containing a single simple statement
     * (e.g. "SELECT * FROM default").
     *
     * @param statement the Analytics query statement to perform.
     * @param bucket the bucket on which to search.
     * @param username the user authorized for bucket access.
     * @param password the password for the user.
     * @return a {@link GenericAnalyticsRequest} for this simple statement.
     */
    public static GenericAnalyticsRequest simpleStatement(String statement, String bucket, String username, String password) {
        return new GenericAnalyticsRequest(statement, false, bucket, username, password, null, NO_PRIORITY);
    }

    /**
     * Create a {@link GenericAnalyticsRequest} and mark it as containing a full Analytics query in Json form
     * (including additional query parameters).
     *
     * The simplest form of such a query is a single statement encapsulated in a json query object:
     * <pre>{"statement":"SELECT * FROM default"}</pre>.
     *
     * @param jsonQuery the Analytics query in json form.
     * @param bucket the bucket on which to perform the query.
     * @param password the password for the target bucket.
     * @return a {@link GenericAnalyticsRequest} for this full query.
     */
    public static GenericAnalyticsRequest jsonQuery(String jsonQuery, String bucket, String username, String password, int priority) {
        return new GenericAnalyticsRequest(jsonQuery, true, bucket, username, password, null, priority);
    }

    /**
     * Create a {@link GenericAnalyticsRequest} and mark it as containing a full Analytics query in Json form
     * (including additional query parameters).
     *
     * The simplest form of such a query is a single statement encapsulated in a json query object:
     * <pre>{"statement":"SELECT * FROM default"}</pre>.
     *
     * @param jsonQuery the Analytics query in json form.
     * @param bucket the bucket on which to perform the query.
     * @param password the password for the target bucket.
     * @return a {@link GenericAnalyticsRequest} for this full query.
     */
    public static GenericAnalyticsRequest jsonQuery(String jsonQuery, String bucket, String username, String password) {
        return jsonQuery(jsonQuery, bucket, username, password, NO_PRIORITY);
    }

    /**
     * Create a {@link GenericAnalyticsRequest} and mark it as containing a full Analytics query in Json form
     * (including additional query parameters).
     *
     * The simplest form of such a query is a single statement encapsulated in a json query object:
     * <pre>{"statement":"SELECT * FROM default"}</pre>.
     *
     * @param jsonQuery the Analytics query in json form.
     * @param bucket the bucket on which to perform the query.
     * @param username the username authorized for bucket access.
     * @param password the password for the user.
     * @param targetNode the node on which to execute this request (or null to let the core locate and choose one).
     * @return a {@link GenericAnalyticsRequest} for this full query.
     */
    public static GenericAnalyticsRequest jsonQuery(String jsonQuery, String bucket, String username, String password, InetAddress targetNode) {
        return new GenericAnalyticsRequest(jsonQuery, true, bucket, username, password, targetNode, NO_PRIORITY);
    }

    @Override
    public String path() {
        return "/analytics/service";
    }
}
