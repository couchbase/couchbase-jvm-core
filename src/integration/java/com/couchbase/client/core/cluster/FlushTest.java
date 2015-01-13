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
import com.couchbase.client.core.message.config.FlushRequest;
import com.couchbase.client.core.message.config.FlushResponse;
import com.couchbase.client.core.message.kv.GetRequest;
import com.couchbase.client.core.message.kv.GetResponse;
import com.couchbase.client.core.message.kv.UpsertRequest;
import com.couchbase.client.core.message.kv.UpsertResponse;
import com.couchbase.client.core.util.ClusterDependentTest;
import io.netty.buffer.Unpooled;
import io.netty.util.CharsetUtil;
import io.netty.util.ReferenceCountUtil;
import org.junit.Test;
import rx.Observable;
import rx.functions.Action1;
import rx.functions.Func1;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 * Verifies the functionality of Flush in various scenarios.
 *
 * @author Michael Nitschinger
 * @since 1.0
 */
public class FlushTest extends ClusterDependentTest {

    @Test
    public void shouldFlush() {
        List<String> keys = Arrays.asList("key1", "key2", "key3");

        Observable
                .from(keys)
                .flatMap(new Func1<String, Observable<UpsertResponse>>() {
                    @Override
                    public Observable<UpsertResponse> call(String key) {
                        return cluster().send(new UpsertRequest(key, Unpooled.copiedBuffer("Content", CharsetUtil.UTF_8), bucket()));
                    }
                })
                .doOnNext(new Action1<UpsertResponse>() {
                    @Override
                    public void call(UpsertResponse upsertResponse) {
                        ReferenceCountUtil.releaseLater(upsertResponse.content());
                    }
                })
                .toBlocking()
                .last();

        FlushResponse response = cluster().<FlushResponse>send(new FlushRequest(bucket(), password())).toBlocking().single();
        assertEquals(ResponseStatus.SUCCESS, response.status());
        ReferenceCountUtil.releaseLater(response.content());

        List<GetResponse> responses = Observable
            .from(keys)
            .flatMap(new Func1<String, Observable<GetResponse>>() {
                @Override
                public Observable<GetResponse> call(String key) {
                    return cluster().send(new GetRequest(key, bucket()));
                }
            }).toList().toBlocking().single();

        assertEquals(keys.size(), responses.size());
        for (GetResponse get : responses) {
            assertEquals(ResponseStatus.NOT_EXISTS, get.status());
            ReferenceCountUtil.releaseLater(get.content());
        }
    }

}
