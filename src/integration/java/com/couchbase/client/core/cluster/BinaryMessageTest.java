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

import com.couchbase.client.core.message.CouchbaseResponse;
import com.couchbase.client.core.message.ResponseStatus;
import com.couchbase.client.core.message.binary.*;
import com.couchbase.client.core.message.cluster.OpenBucketRequest;
import com.couchbase.client.core.message.cluster.OpenBucketResponse;
import com.couchbase.client.core.message.cluster.SeedNodesRequest;
import com.couchbase.client.core.message.cluster.SeedNodesResponse;
import com.couchbase.client.core.util.TestProperties;
import io.netty.buffer.Unpooled;
import io.netty.util.CharsetUtil;
import org.junit.BeforeClass;
import org.junit.Test;
import rx.Observable;
import rx.functions.Func1;

import static org.junit.Assert.assertEquals;

/**
 * Verifies basic functionality of binary operations.
 *
 * @author Michael Nitschinger
 * @since 1.0
 */
public class BinaryMessageTest {

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

    @Test
    public void shouldUpsertAndGetDocument() {
        String key = "upsert-key";
        String content = "Hello World!";
        UpsertRequest upsert = new UpsertRequest(key, Unpooled.copiedBuffer(content, CharsetUtil.UTF_8), bucket);
        cluster.<UpsertResponse>send(upsert).toBlockingObservable().single();

        GetRequest request = new GetRequest(key, bucket);
        assertEquals(content, cluster.<GetResponse>send(request).toBlockingObservable().single().content());
    }

    @Test
    public void shouldUpsertWithExpiration() throws Exception {
        String key = "upsert-key-vanish";
        String content = "Hello World!";
        UpsertRequest upsert = new UpsertRequest(key, Unpooled.copiedBuffer(content, CharsetUtil.UTF_8), 1, 0, bucket);
        cluster.<UpsertResponse>send(upsert).toBlockingObservable().single();

        Thread.sleep(2000);

        GetRequest request = new GetRequest(key, bucket);
        assertEquals("Not found", cluster.<GetResponse>send(request).toBlockingObservable().single().content());
    }

    @Test
    public void shouldHandleDoubleInsert() {
        String key = "insert-key";
        String content = "Hello World!";
        InsertRequest insert = new InsertRequest(key, Unpooled.copiedBuffer(content, CharsetUtil.UTF_8), bucket);
        assertEquals(ResponseStatus.OK, cluster.<InsertResponse>send(insert).toBlockingObservable().single().status());

        insert = new InsertRequest(key, Unpooled.copiedBuffer(content, CharsetUtil.UTF_8), bucket);
        assertEquals(ResponseStatus.EXISTS, cluster.<InsertResponse>send(insert).toBlockingObservable().single().status());
    }

    @Test
    public void shouldReplaceWithoutCAS() {
        final String key = "replace-key";
        final String content = "replace content";

        ReplaceRequest insert = new ReplaceRequest(key, Unpooled.copiedBuffer(content, CharsetUtil.UTF_8), bucket);
        assertEquals(ResponseStatus.NOT_EXISTS, cluster.<ReplaceResponse>send(insert).toBlockingObservable().single().status());

        UpsertRequest upsert = new UpsertRequest(key, Unpooled.copiedBuffer("insert content", CharsetUtil.UTF_8), bucket);
        ReplaceResponse response = cluster.<UpsertResponse>send(upsert)
            .flatMap(new Func1<UpsertResponse, Observable<ReplaceResponse>>() {
                @Override
                public Observable<ReplaceResponse> call(UpsertResponse response) {
                    return cluster.send(new ReplaceRequest(key, Unpooled.copiedBuffer(content, CharsetUtil.UTF_8), bucket));
                }
            }
        ).toBlockingObservable().single();

        assertEquals(ResponseStatus.OK, response.status());
    }

    @Test
    public void shouldReplaceWithCAS() {
        final String key = "replace-key-cas";
        final String content = "replace content";

        ReplaceRequest insert = new ReplaceRequest(key, Unpooled.copiedBuffer(content, CharsetUtil.UTF_8), bucket);
        assertEquals(ResponseStatus.NOT_EXISTS, cluster.<ReplaceResponse>send(insert).toBlockingObservable().single().status());

        UpsertRequest upsert = new UpsertRequest(key, Unpooled.copiedBuffer("insert content", CharsetUtil.UTF_8), bucket);
        ReplaceResponse response = cluster.<UpsertResponse>send(upsert)
                .flatMap(new Func1<UpsertResponse, Observable<ReplaceResponse>>() {
                             @Override
                             public Observable<ReplaceResponse> call(UpsertResponse response) {
                                 return cluster.send(new ReplaceRequest(key, Unpooled.copiedBuffer(content, CharsetUtil.UTF_8), 24234234L, bucket));
                             }
                         }
                ).toBlockingObservable().single();

        assertEquals(ResponseStatus.EXISTS, response.status());

        // TODO: add CAS() response to all binary responses (get, upsert, insert, replace)
        // TODO: implement remove
    }

}
