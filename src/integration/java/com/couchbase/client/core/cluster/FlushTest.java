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
