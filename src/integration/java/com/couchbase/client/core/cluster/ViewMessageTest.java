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
package com.couchbase.client.core.cluster;

import com.couchbase.client.core.message.cluster.OpenBucketRequest;
import com.couchbase.client.core.message.cluster.OpenBucketResponse;
import com.couchbase.client.core.message.cluster.SeedNodesRequest;
import com.couchbase.client.core.message.cluster.SeedNodesResponse;
import com.couchbase.client.core.util.TestProperties;
import org.junit.BeforeClass;
import rx.Observable;
import rx.functions.Func1;

/**
 * Verifies basic functionality of view operations.
 *
 * @author Michael Nitschinger
 * @since 1.0
 */
public class ViewMessageTest {

    private static final String seedNode = TestProperties.seedNode();
    private static final String bucket = TestProperties.bucket();
    private static final String password = TestProperties.password();

    private static Cluster cluster;

    @BeforeClass
    public static void connect() {
        cluster = new CouchbaseCluster();
        cluster.<SeedNodesResponse>send(new SeedNodesRequest(seedNode)).flatMap(
            new Func1<SeedNodesResponse, Observable<OpenBucketResponse>>() {
                @Override
                public Observable<OpenBucketResponse> call(SeedNodesResponse response) {
                    return cluster.send(new OpenBucketRequest(bucket, password));
                }
            }
        ).toBlockingObservable().single();
    }

    /*@Test
    public void shoudQueryNonExistentView() {
        ViewQueryResponse single = cluster
            .<ViewQueryResponse>send(new ViewQueryRequest("design", "view", false, bucket, password))
            .toBlockingObservable()
            .single();

        String expected = "{\"error\":\"not_found\",\"reason\":\"Design document _design/design not found\"}\n";
        assertEquals(expected, single.content().toString(CharsetUtil.UTF_8));
    }*/

    /*@Test
    public void shouldQueryExistingView() {
        List<ViewQueryResponse> responses = cluster
            .<ViewQueryResponse>send(new ViewQueryRequest("foo", "bar", false, bucket, password))
            .toList()
            .toBlockingObservable()
            .single();

        System.out.println(responses);
    }*/
}
