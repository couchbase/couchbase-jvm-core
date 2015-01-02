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
import com.couchbase.client.core.message.kv.CounterRequest;
import com.couchbase.client.core.message.kv.CounterResponse;
import com.couchbase.client.core.message.kv.GetRequest;
import com.couchbase.client.core.message.kv.GetResponse;
import com.couchbase.client.core.message.kv.InsertRequest;
import com.couchbase.client.core.message.kv.InsertResponse;
import com.couchbase.client.core.message.kv.RemoveRequest;
import com.couchbase.client.core.message.kv.RemoveResponse;
import com.couchbase.client.core.message.kv.ReplaceRequest;
import com.couchbase.client.core.message.kv.ReplaceResponse;
import com.couchbase.client.core.message.kv.TouchRequest;
import com.couchbase.client.core.message.kv.TouchResponse;
import com.couchbase.client.core.message.kv.UnlockRequest;
import com.couchbase.client.core.message.kv.UnlockResponse;
import com.couchbase.client.core.message.kv.UpsertRequest;
import com.couchbase.client.core.message.kv.UpsertResponse;
import com.couchbase.client.core.util.ClusterDependentTest;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.util.CharsetUtil;
import io.netty.util.ReferenceCountUtil;
import org.junit.Test;
import rx.Observable;
import rx.functions.Func1;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Verifies basic functionality of binary operations.
 *
 * @author Michael Nitschinger
 * @since 1.0
 */
public class KeyValueMessageTest extends ClusterDependentTest {

    @Test
    public void shouldUpsertAndGetDocument() throws Exception {
        String key = "upsert-key";
        String content = "Hello World!";
        UpsertRequest upsert = new UpsertRequest(key, Unpooled.copiedBuffer(content, CharsetUtil.UTF_8), bucket());
        UpsertResponse response = cluster().<UpsertResponse>send(upsert).toBlocking().single();
        response.content().release();

        GetRequest request = new GetRequest(key, bucket());
        GetResponse getResponse = cluster().<GetResponse>send(request).toBlocking().single();
        assertEquals(content, getResponse.content().toString(CharsetUtil.UTF_8));
        getResponse.content().release();
    }

    @Test
    public void shouldUpsertWithExpiration() throws Exception {
        String key = "upsert-key-vanish";
        String content = "Hello World!";
        UpsertRequest upsert = new UpsertRequest(key, Unpooled.copiedBuffer(content, CharsetUtil.UTF_8), 1, 0, bucket());
        UpsertResponse response = cluster().<UpsertResponse>send(upsert).toBlocking().single();
        response.content().release();

        Thread.sleep(2000);

        GetRequest request = new GetRequest(key, bucket());
        GetResponse getResponse = cluster().<GetResponse>send(request).toBlocking().single();
        assertEquals(ResponseStatus.NOT_EXISTS, getResponse.status());
        getResponse.content().release();
    }

    @Test
    public void shouldHandleDoubleInsert() {
        String key = "insert-key";
        String content = "Hello World!";
        InsertRequest insert = new InsertRequest(key, Unpooled.copiedBuffer(content, CharsetUtil.UTF_8), bucket());
        InsertResponse insertResponse = cluster().<InsertResponse>send(insert).toBlocking().single();
        assertEquals(ResponseStatus.SUCCESS, insertResponse.status());
        insertResponse.content().release();

        insert = new InsertRequest(key, Unpooled.copiedBuffer(content, CharsetUtil.UTF_8), bucket());
        insertResponse = cluster().<InsertResponse>send(insert).toBlocking().single();
        assertEquals(ResponseStatus.EXISTS, insertResponse.status());
    }

    @Test
    public void shouldReplaceWithoutCAS() {
        final String key = "replace-key";
        final String content = "replace content";

        ReplaceRequest insert = new ReplaceRequest(key, Unpooled.copiedBuffer(content, CharsetUtil.UTF_8), bucket());
        ReplaceResponse response = cluster().<ReplaceResponse>send(insert).toBlocking().single();
        assertEquals(ResponseStatus.NOT_EXISTS, response.status());
        response.content().release();

        UpsertRequest upsert = new UpsertRequest(key, Unpooled.copiedBuffer("insert content", CharsetUtil.UTF_8), bucket());
        response = cluster().<UpsertResponse>send(upsert)
            .flatMap(new Func1<UpsertResponse, Observable<ReplaceResponse>>() {
                @Override
                public Observable<ReplaceResponse> call(UpsertResponse response) {
                    response.content().release();
                    return cluster().send(new ReplaceRequest(key, Unpooled.copiedBuffer(content, CharsetUtil.UTF_8), bucket()));
                }
            }
        ).toBlocking().single();
        response.content().release();

        assertEquals(ResponseStatus.SUCCESS, response.status());
    }

    @Test
    public void shouldReplaceWithFailingCAS() {
        final String key = "replace-key-cas-fail";
        final String content = "replace content";

        ReplaceRequest insert = new ReplaceRequest(key, Unpooled.copiedBuffer(content, CharsetUtil.UTF_8), bucket());
        ReplaceResponse response = cluster().<ReplaceResponse>send(insert).toBlocking().single();
        assertEquals(ResponseStatus.NOT_EXISTS, response.status());
        response.content().release();

        UpsertRequest upsert = new UpsertRequest(key, Unpooled.copiedBuffer("insert content", CharsetUtil.UTF_8), bucket());
        response = cluster().<UpsertResponse>send(upsert)
            .flatMap(new Func1<UpsertResponse, Observable<ReplaceResponse>>() {
                @Override
                public Observable<ReplaceResponse> call(UpsertResponse response) {
                    response.content().release();
                 return cluster().send(new ReplaceRequest(key, Unpooled.copiedBuffer(content, CharsetUtil.UTF_8), 24234234L, bucket()));
                }
            }).toBlocking().single();
        response.content().release();

        assertEquals(ResponseStatus.EXISTS, response.status());
    }

    @Test
    public void shouldReplaceWithMatchingCAS() {
        final String key = "replace-key-cas-match";
        final String content = "replace content";

        ReplaceRequest insert = new ReplaceRequest(key, Unpooled.copiedBuffer(content, CharsetUtil.UTF_8), bucket());
        ReplaceResponse response = cluster().<ReplaceResponse>send(insert).toBlocking().single();
        assertEquals(ResponseStatus.NOT_EXISTS, response.status());
        response.content().release();

        UpsertRequest upsert = new UpsertRequest(key, Unpooled.copiedBuffer("insert content", CharsetUtil.UTF_8), bucket());
        response = cluster().<UpsertResponse>send(upsert)
            .flatMap(new Func1<UpsertResponse, Observable<ReplaceResponse>>() {
                @Override
                public Observable<ReplaceResponse> call(UpsertResponse response) {
                    response.content().release();
                    return cluster().send(new ReplaceRequest(key, Unpooled.copiedBuffer(content, CharsetUtil.UTF_8), response.cas(), bucket()));
                }
            }).toBlocking().single();
        response.content().release();

        assertEquals(ResponseStatus.SUCCESS, response.status());
    }

    @Test
    public void shouldRemoveDocumentWithoutCAS() {
        String key = "remove-key";
        String content = "Hello World!";
        UpsertRequest upsert = new UpsertRequest(key, Unpooled.copiedBuffer(content, CharsetUtil.UTF_8), bucket());
        UpsertResponse upsertResponse = cluster().<UpsertResponse>send(upsert).toBlocking().single();
        assertEquals(ResponseStatus.SUCCESS, upsertResponse.status());
        upsertResponse.content().release();

        RemoveRequest remove = new RemoveRequest(key, bucket());
        RemoveResponse response = cluster().<RemoveResponse>send(remove).toBlocking().single();
        assertEquals(ResponseStatus.SUCCESS, response.status());
        assertTrue(response.cas() != 0);
        response.content().release();

        GetRequest get = new GetRequest(key, bucket());
        GetResponse getResponse = cluster().<GetResponse>send(get).toBlocking().single();
        assertEquals(ResponseStatus.NOT_EXISTS, getResponse.status());
        getResponse.content().release();
    }

    @Test
    public void shouldRemoveDocumentWithCAS() {
        String key = "remove-key-cas";
        String content = "Hello World!";
        UpsertRequest upsert = new UpsertRequest(key, Unpooled.copiedBuffer(content, CharsetUtil.UTF_8), bucket());
        UpsertResponse upsertResponse = cluster().<UpsertResponse>send(upsert).toBlocking().single();
        assertEquals(ResponseStatus.SUCCESS, upsertResponse.status());
        upsertResponse.content().release();

        RemoveRequest remove = new RemoveRequest(key, 1233443, bucket());
        RemoveResponse response = cluster().<RemoveResponse>send(remove).toBlocking().single();
        assertEquals(ResponseStatus.EXISTS, response.status());
        response.content().release();
        remove = new RemoveRequest(key, upsertResponse.cas(), bucket());
        response = cluster().<RemoveResponse>send(remove).toBlocking().single();
        assertEquals(ResponseStatus.SUCCESS, response.status());
        assertTrue(response.cas() != 0);
        response.content().release();
    }

    @Test
    public void shouldIncrementFromCounter() {
        String key = "counter-incr";

        CounterResponse response1 = cluster().<CounterResponse>send(new CounterRequest(key, 0, 10, 0, bucket())).toBlocking().single();
        assertEquals(0, response1.value());

        CounterResponse response2 = cluster().<CounterResponse>send(new CounterRequest(key, 0, 10, 0, bucket())).toBlocking().single();
        assertEquals(10, response2.value());

        CounterResponse response3 = cluster().<CounterResponse>send(new CounterRequest(key, 0, 10, 0, bucket())).toBlocking().single();
        assertEquals(20, response3.value());

        assertTrue(response1.cas() != response2.cas());
        assertTrue(response2.cas() != response3.cas());
    }

    @Test
    public void shouldDecrementFromCounter() {
        String key = "counter-decr";

        CounterResponse response1 = cluster().<CounterResponse>send(new CounterRequest(key, 100, -10, 0, bucket())).toBlocking().single();
        assertEquals(100, response1.value());

        CounterResponse response2 = cluster().<CounterResponse>send(new CounterRequest(key, 100, -10, 0, bucket())).toBlocking().single();
        assertEquals(90, response2.value());

        CounterResponse response3 = cluster().<CounterResponse>send(new CounterRequest(key, 100, -10, 0, bucket())).toBlocking().single();
        assertEquals(80, response3.value());

        assertTrue(response1.cas() != response2.cas());
        assertTrue(response2.cas() != response3.cas());
    }

    @Test
    public void shouldGetAndTouch() throws Exception {
        String key = "get-and-touch";

        UpsertRequest request = new UpsertRequest(key, Unpooled.copiedBuffer("content", CharsetUtil.UTF_8), 3, 0, bucket());
        UpsertResponse response = cluster().<UpsertResponse>send(request).toBlocking().single();
        assertEquals(ResponseStatus.SUCCESS, response.status());
        response.content().release();

        Thread.sleep(2000);

        GetResponse getResponse = cluster().<GetResponse>send(new GetRequest(key, bucket(), false, true, 3)).toBlocking().single();
        assertEquals(ResponseStatus.SUCCESS, getResponse.status());
        assertEquals("content", getResponse.content().toString(CharsetUtil.UTF_8));
        getResponse.content().release();

        Thread.sleep(2000);

        getResponse = cluster().<GetResponse>send(new GetRequest(key, bucket(), false, true, 3)).toBlocking().single();
        assertEquals(ResponseStatus.SUCCESS, getResponse.status());
        assertEquals("content", getResponse.content().toString(CharsetUtil.UTF_8));
        getResponse.content().release();
    }

    @Test
    public void shouldGetAndLock() throws Exception {
        String key = "get-and-lock";

        UpsertRequest request = new UpsertRequest(key, Unpooled.copiedBuffer("content", CharsetUtil.UTF_8), bucket());
        UpsertResponse response = cluster().<UpsertResponse>send(request).toBlocking().single();
        assertEquals(ResponseStatus.SUCCESS, response.status());
        response.content().release();

        GetResponse getResponse = cluster().<GetResponse>send(new GetRequest(key, bucket(), true, false, 2)).toBlocking().single();
        assertEquals(ResponseStatus.SUCCESS, getResponse.status());
        assertEquals("content", getResponse.content().toString(CharsetUtil.UTF_8));
        getResponse.content().release();

        request = new UpsertRequest(key, Unpooled.copiedBuffer("content", CharsetUtil.UTF_8), bucket());
        response = cluster().<UpsertResponse>send(request).toBlocking().single();
        assertEquals(ResponseStatus.EXISTS, response.status());
        response.content().release();

        Thread.sleep(3000);

        request = new UpsertRequest(key, Unpooled.copiedBuffer("content", CharsetUtil.UTF_8), bucket());
        response = cluster().<UpsertResponse>send(request).toBlocking().single();
        response.content().release();
        assertEquals(ResponseStatus.SUCCESS, response.status());
    }

    @Test
    public void shouldTouch() throws Exception {
        String key = "touch";

        UpsertRequest request = new UpsertRequest(key, Unpooled.copiedBuffer("content", CharsetUtil.UTF_8), 3, 0, bucket());
        UpsertResponse response = cluster().<UpsertResponse>send(request).toBlocking().single();
        assertEquals(ResponseStatus.SUCCESS, response.status());
        response.content().release();

        Thread.sleep(2000);

        TouchResponse touchResponse = cluster().<TouchResponse>send(new TouchRequest(key, 3, bucket())).toBlocking().single();
        assertEquals(ResponseStatus.SUCCESS, touchResponse.status());
        touchResponse.content().release();

        Thread.sleep(2000);

        GetResponse getResponse = cluster().<GetResponse>send(new GetRequest(key, bucket())).toBlocking().single();
        assertEquals(ResponseStatus.SUCCESS, getResponse.status());
        assertEquals("content", getResponse.content().toString(CharsetUtil.UTF_8));
        getResponse.content().release();
    }

    @Test
    public void shouldUnlock() throws Exception {
        String key = "unlock";

        UpsertRequest request = new UpsertRequest(key, Unpooled.copiedBuffer("content", CharsetUtil.UTF_8), bucket());
        UpsertResponse response = cluster().<UpsertResponse>send(request).toBlocking().single();
        assertEquals(ResponseStatus.SUCCESS, response.status());
        response.content().release();

        GetResponse getResponse = cluster().<GetResponse>send(new GetRequest(key, bucket(), true, false, 15)).toBlocking().single();
        assertEquals(ResponseStatus.SUCCESS, getResponse.status());
        assertEquals("content", getResponse.content().toString(CharsetUtil.UTF_8));
        getResponse.content().release();

        request = new UpsertRequest(key, Unpooled.copiedBuffer("content", CharsetUtil.UTF_8), bucket());
        response = cluster().<UpsertResponse>send(request).toBlocking().single();
        assertEquals(ResponseStatus.EXISTS, response.status());
        response.content().release();

        UnlockRequest unlockRequest = new UnlockRequest(key, getResponse.cas(), bucket());
        UnlockResponse unlockResponse = cluster().<UnlockResponse>send(unlockRequest).toBlocking().single();
        assertEquals(ResponseStatus.SUCCESS, unlockResponse.status());
        unlockResponse.content().release();

        request = new UpsertRequest(key, Unpooled.copiedBuffer("content", CharsetUtil.UTF_8), bucket());
        response = cluster().<UpsertResponse>send(request).toBlocking().single();
        assertEquals(ResponseStatus.SUCCESS, response.status());
        response.content().release();
    }

}
