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

import com.couchbase.client.core.logging.CouchbaseLoggerFactory;
import com.couchbase.client.core.logging.RedactionLevel;
import com.couchbase.client.core.message.AbstractCouchbaseRequest;
import com.couchbase.client.core.message.PrelocatedRequest;
import com.couchbase.client.core.tracing.ThresholdLogReporter;
import io.opentracing.Span;
import io.opentracing.tag.Tags;

import java.net.InetAddress;

/**
 * For the lack of a better name, a query request against a query server.
 *
 * @author Michael Nitschinger
 * @since 1.0
 */
public class GenericQueryRequest extends AbstractCouchbaseRequest implements QueryRequest, PrelocatedRequest {

    /**
     * Cache the redaction level for performance.
     */
    private static final RedactionLevel REDACTION_LEVEL = CouchbaseLoggerFactory.getRedactionLevel();

    private final String query;
    private final boolean jsonFormat;
    private final InetAddress targetNode;
    private final String contextId;
    private final String statement;

    protected GenericQueryRequest(String query, boolean jsonFormat, String bucket, String username, String password, InetAddress targetNode, String contextId, String statement) {
        super(bucket, username, password);
        this.query = query;
        this.jsonFormat = jsonFormat;
        this.targetNode = targetNode;
        this.contextId = contextId;
        this.statement = statement;
    }

    @Override
    protected void afterSpanSet(Span span) {
        span.setTag(Tags.PEER_SERVICE.getKey(), ThresholdLogReporter.SERVICE_N1QL);
        if (statement != null && !statement.isEmpty() && REDACTION_LEVEL == RedactionLevel.NONE) {
            span.setTag(Tags.DB_STATEMENT.getKey(), statement);
        }
    }

    @Override
    public String operationId() {
        return contextId;
    }

    public String query() {
        return query;
    }

    public boolean isJsonFormat() {
        return jsonFormat;
    }

    @Override
    public InetAddress sendTo() {
        return targetNode;
    }

    /**
     * Creates a {@link GenericQueryRequest} and mark it as containing a single simple statement
     * (e.g. "SELECT * FROM default").
     *
     * @param statement the N1QL query statement to perform.
     * @param bucket the bucket on which to search.
     * @param password the password for the target bucket.
     * @return a {@link GenericQueryRequest} for this simple statement.
     */
    public static GenericQueryRequest simpleStatement(String statement, String bucket, String password) {
        return new GenericQueryRequest(statement, false, bucket, bucket, password, null,
            null, null);
    }

    /**
     * Creates a {@link GenericQueryRequest} and mark it as containing a single simple statement
     * (e.g. "SELECT * FROM default").
     *
     * @param statement the N1QL query statement to perform.
     * @param bucket the bucket on which to search.
     * @param username the user authorized for bucket access.
     * @param password the password for the user.
     * @return a {@link GenericQueryRequest} for this simple statement.
     */
    public static GenericQueryRequest simpleStatement(String statement, String bucket, String username, String password) {
        return new GenericQueryRequest(statement, false, bucket, username, password, null,
            null, null);
    }

    /**
     * Create a {@link GenericQueryRequest} and mark it as containing a full N1QL query in Json form
     * (including additional query parameters like named arguments, etc...).
     *
     * The simplest form of such a query is a single statement encapsulated in a json query object:
     * <pre>{"statement":"SELECT * FROM default"}</pre>.
     *
     * @param jsonQuery the N1QL query in json form.
     * @param bucket the bucket on which to perform the query.
     * @param password the password for the target bucket.
     * @param contextId the context id to store and use for tracing purposes.
     * @return a {@link GenericQueryRequest} for this full query.
     */
    public static GenericQueryRequest jsonQuery(String jsonQuery, String bucket, String password, String contextId) {
        return new GenericQueryRequest(jsonQuery, true, bucket, bucket, password, null,
            contextId, null);
    }

    /**
     * Create a {@link GenericQueryRequest} and mark it as containing a full N1QL query in Json form
     * (including additional query parameters like named arguments, etc...).
     *
     * The simplest form of such a query is a single statement encapsulated in a json query object:
     * <pre>{"statement":"SELECT * FROM default"}</pre>.
     *
     * @param jsonQuery the N1QL query in json form.
     * @param bucket the bucket on which to perform the query.
     * @param username the user authorized for bucket access.
     * @param password the password for the user.
     * @param contextId the context id to store and use for tracing purposes.
     * @return a {@link GenericQueryRequest} for this full query.
     */
    public static GenericQueryRequest jsonQuery(String jsonQuery, String bucket, String username, String password, String contextId, String statement) {
        return new GenericQueryRequest(jsonQuery, true, bucket, username, password, null,
            contextId, statement);
    }


    /**
     * Create a {@link GenericQueryRequest} and mark it as containing a full N1QL query in Json form
     * (including additional query parameters like named arguments, etc...).
     *
     * The simplest form of such a query is a single statement encapsulated in a json query object:
     * <pre>{"statement":"SELECT * FROM default"}</pre>.
     *
     * @param jsonQuery the N1QL query in json form.
     * @param bucket the bucket on which to perform the query.
     * @param password the password for the target bucket.
     * @param targetNode the node on which to execute this request (or null to let the core locate and choose one).
     * @param contextId the context id to store and use for tracing purposes.
     * @return a {@link GenericQueryRequest} for this full query.
     */
    public static GenericQueryRequest jsonQuery(String jsonQuery, String bucket, String password, InetAddress targetNode, String contextId) {
        return new GenericQueryRequest(jsonQuery, true, bucket, bucket, password, targetNode, contextId,
            null);
    }

    /**
     * Create a {@link GenericQueryRequest} and mark it as containing a full N1QL query in Json form
     * (including additional query parameters like named arguments, etc...).
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
     * @param statement the statement string.
     * @return a {@link GenericQueryRequest} for this full query.
     */
    public static GenericQueryRequest jsonQuery(String jsonQuery, String bucket, String username, String password, InetAddress targetNode, String contextId, String statement) {
        return new GenericQueryRequest(jsonQuery, true, bucket, username, password, targetNode, contextId,
            statement);
    }
}
