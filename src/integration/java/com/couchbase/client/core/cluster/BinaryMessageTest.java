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

import com.couchbase.client.core.message.ResponseStatus;
import com.couchbase.client.core.message.binary.GetRequest;
import com.couchbase.client.core.message.binary.GetResponse;
import com.couchbase.client.core.message.binary.InsertRequest;
import com.couchbase.client.core.message.binary.InsertResponse;
import com.couchbase.client.core.message.binary.RemoveRequest;
import com.couchbase.client.core.message.binary.RemoveResponse;
import com.couchbase.client.core.message.binary.ReplaceRequest;
import com.couchbase.client.core.message.binary.ReplaceResponse;
import com.couchbase.client.core.message.binary.UpsertRequest;
import com.couchbase.client.core.message.binary.UpsertResponse;
import com.couchbase.client.core.message.cluster.DisconnectRequest;
import com.couchbase.client.core.message.cluster.OpenBucketRequest;
import com.couchbase.client.core.message.cluster.OpenBucketResponse;
import com.couchbase.client.core.message.cluster.SeedNodesRequest;
import com.couchbase.client.core.message.cluster.SeedNodesResponse;
import com.couchbase.client.core.util.TestProperties;
import io.netty.buffer.Unpooled;
import io.netty.util.CharsetUtil;
import org.junit.AfterClass;
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

    @AfterClass
    public static void disconnect() throws InterruptedException {
        cluster.send(new DisconnectRequest()).toBlockingObservable().first();
    }

    @Test
    public void shouldUpsertAndGetDocument() {
        String key = "upsert-key";
        String content = "Hello World!";
        UpsertRequest upsert = new UpsertRequest(key, Unpooled.copiedBuffer(content, CharsetUtil.UTF_8), bucket);
        cluster.<UpsertResponse>send(upsert).toBlockingObservable().single();

        GetRequest request = new GetRequest(key, bucket);
        assertEquals(content, cluster.<GetResponse>send(request).toBlockingObservable().single().content()
            .toString(CharsetUtil.UTF_8));
    }

    @Test
    public void shouldUpsertWithExpiration() throws Exception {
        String key = "upsert-key-vanish";
        String content = "Hello World!";
        UpsertRequest upsert = new UpsertRequest(key, Unpooled.copiedBuffer(content, CharsetUtil.UTF_8), 1, 0, bucket);
        cluster.<UpsertResponse>send(upsert).toBlockingObservable().single();

        Thread.sleep(2000);

        GetRequest request = new GetRequest(key, bucket);
        assertEquals(ResponseStatus.NOT_EXISTS, cluster.<GetResponse>send(request).toBlockingObservable().single().status());
    }

    @Test
    public void shouldHandleDoubleInsert() {
        String key = "insert-key";
        String content = "Hello World!";
        InsertRequest insert = new InsertRequest(key, Unpooled.copiedBuffer(content, CharsetUtil.UTF_8), bucket);
        assertEquals(ResponseStatus.SUCCESS, cluster.<InsertResponse>send(insert).toBlockingObservable().single().status());

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

        assertEquals(ResponseStatus.SUCCESS, response.status());
    }

    @Test
    public void shouldReplaceWithFailingCAS() {
        final String key = "replace-key-cas-fail";
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
            }).toBlockingObservable().single();

        assertEquals(ResponseStatus.EXISTS, response.status());
    }

    @Test
    public void shouldReplaceWithMatchingCAS() {
        final String key = "replace-key-cas-match";
        final String content = "replace content";

        ReplaceRequest insert = new ReplaceRequest(key, Unpooled.copiedBuffer(content, CharsetUtil.UTF_8), bucket);
        assertEquals(ResponseStatus.NOT_EXISTS, cluster.<ReplaceResponse>send(insert).toBlockingObservable().single().status());

        UpsertRequest upsert = new UpsertRequest(key, Unpooled.copiedBuffer("insert content", CharsetUtil.UTF_8), bucket);
        ReplaceResponse response = cluster.<UpsertResponse>send(upsert)
            .flatMap(new Func1<UpsertResponse, Observable<ReplaceResponse>>() {
                @Override
                public Observable<ReplaceResponse> call(UpsertResponse response) {
                    return cluster.send(new ReplaceRequest(key, Unpooled.copiedBuffer(content, CharsetUtil.UTF_8), response.cas(), bucket));
                }
            }).toBlockingObservable().single();

        assertEquals(ResponseStatus.SUCCESS, response.status());
    }

    @Test
    public void shouldRemoveDocumentWithoutCAS() {
        String key = "remove-key";
        String content = "Hello World!";
        UpsertRequest upsert = new UpsertRequest(key, Unpooled.copiedBuffer(content, CharsetUtil.UTF_8), bucket);
        assertEquals(ResponseStatus.SUCCESS, cluster.<UpsertResponse>send(upsert).toBlockingObservable().single().status());

        RemoveRequest remove = new RemoveRequest(key, bucket);
        assertEquals(ResponseStatus.SUCCESS, cluster.<RemoveResponse>send(remove).toBlockingObservable().single().status());
        GetRequest get = new GetRequest(key, bucket);
        assertEquals(ResponseStatus.NOT_EXISTS, cluster.<GetResponse>send(get).toBlockingObservable().single().status());
    }

    @Test
    public void shouldRemoveDocumentWithCAS() {
        String key = "remove-key-cas";
        String content = "Hello World!";
        UpsertRequest upsert = new UpsertRequest(key, Unpooled.copiedBuffer(content, CharsetUtil.UTF_8), bucket);
        UpsertResponse upsertResponse = cluster.<UpsertResponse>send(upsert).toBlockingObservable().single();
        assertEquals(ResponseStatus.SUCCESS, upsertResponse.status());

        RemoveRequest remove = new RemoveRequest(key, 1233443, bucket);
        assertEquals(ResponseStatus.EXISTS, cluster.<RemoveResponse>send(remove).toBlockingObservable().single().status());
        remove = new RemoveRequest(key, upsertResponse.cas(), bucket);
        assertEquals(ResponseStatus.SUCCESS, cluster.<RemoveResponse>send(remove).toBlockingObservable().single().status());
    }

}
