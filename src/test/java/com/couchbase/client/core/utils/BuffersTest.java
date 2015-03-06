/**
 * Copyright (c) 2015 Couchbase, Inc.
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