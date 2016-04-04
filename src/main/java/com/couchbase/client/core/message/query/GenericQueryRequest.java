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
package com.couchbase.client.core.message.query;

import com.couchbase.client.core.message.AbstractCouchbaseRequest;
import com.couchbase.client.core.message.PrelocatedRequest;

import java.net.InetAddress;

/**
 * For the lack of a better name, a query request against a query server.
 *
 * @author Michael Nitschinger
 * @since 1.0
 */
public class GenericQueryRequest extends AbstractCouchbaseRequest implements QueryRequest, PrelocatedRequest {

    private final String query;
    private final boolean jsonFormat;
    private final InetAddress targetNode;

    private GenericQueryRequest(String query, boolean jsonFormat, String bucket, String password, InetAddress targetNode) {
        super(bucket, password);
        this.query = query;
        this.jsonFormat = jsonFormat;
        this.targetNode = targetNode;
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
        return new GenericQueryRequest(statement, false, bucket, password, null);
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
     * @return a {@link GenericQueryRequest} for this full query.
     */
    public static GenericQueryRequest jsonQuery(String jsonQuery, String bucket, String password) {
        return new GenericQueryRequest(jsonQuery, true, bucket, password, null);
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
     * @return a {@link GenericQueryRequest} for this full query.
     */
    public static GenericQueryRequest jsonQuery(String jsonQuery, String bucket, String password, InetAddress targetNode) {
        return new GenericQueryRequest(jsonQuery, true, bucket, password, targetNode);
    }
}
