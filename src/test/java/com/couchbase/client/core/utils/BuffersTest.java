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
package com.couchbase.client.core.utils;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.Test;
import rx.Observable;
import rx.functions.Action1;
import rx.subjects.ReplaySubject;

import static org.junit.Assert.assertEquals;

/**
 * Verifies the functionality of
 */
public class BuffersTest {

    @Test
    public void shouldReleaseIfUnsubscribed() {
        ReplaySubject<ByteBuf> source = ReplaySubject.create();
        Observable<ByteBuf> wrapped = Buffers.wrapColdWithAutoRelease(source);

        for (int i = 0; i < 5; i++) {
            source.onNext(Unpooled.buffer());
        }
        source.onCompleted();

        source.toBlocking().forEach(new Action1<ByteBuf>() {
            @Override
            public void call(ByteBuf byteBuf) {
                assertEquals(1, byteBuf.refCnt());
            }
        });

        wrapped
            .take(2)
            .toBlocking()
            .forEach(new Action1<ByteBuf>() {
                @Override
                public void call(ByteBuf byteBuf) {
                    byteBuf.release();
                }
            });

        source.toBlocking().forEach(new Action1<ByteBuf>() {
            @Override
            public void call(ByteBuf byteBuf) {
                assertEquals(0, byteBuf.refCnt());
            }
        });

    }

}