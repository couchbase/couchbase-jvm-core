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
import io.netty.util.ReferenceCountUtil;
import rx.Observable;
import rx.Subscriber;
import rx.functions.Action1;

/**
 * Collection of utilities around {@link ByteBuf}.
 *
 * @author Simon Baslé
 * @author Michael Nitschinger
 * @since 1.1
 */
public class Buffers {

    /**
     * An rx {@link Action1} that releases (once) a non-null {@link ByteBuf} provided its refCnt is &gt; 0.
     */
    public static final Action1 BYTE_BUF_RELEASER = new Action1<ByteBuf>() {
        @Override
        public void call(ByteBuf byteBuf) {
            if (byteBuf != null && byteBuf.refCnt() > 0) {
                byteBuf.release();
            }
        }
    };

    /**
     * Wrap an observable and free a reference counted item if unsubscribed in the meantime.
     *
     * This can and should be used if a hot observable is used as the source but it is not guaranteed that
     * there will always be a subscriber that consumes the reference counted item. If an item is emitted
     * by the source observable and no subscriber is attached (because it unsubscribed) the item will
     * be freed.
     *
     * Note that also non reference counted items can be passed in, but there is no impact other than
     * making it cold (in which case defer could be used).
     *
     * It is very important that if subscribed, the caller needs to release the reference counted item.
     * It wil only be released on behalf of the caller when unsubscribed.
     *
     * @param source the source observable to wrap.
     * @return the wrapped cold observable with refcnt release logic.
     */
    public static <T> Observable<T> wrapColdWithAutoRelease(final Observable<T> source) {
        return Observable.create(new Observable.OnSubscribe<T>() {
            @Override
            public void call(final Subscriber<? super T> subscriber) {
                source.subscribe(new Subscriber<T>() {
                    @Override
                    public void onCompleted() {
                        if (!subscriber.isUnsubscribed()) {
                            subscriber.onCompleted();
                        }
                    }

                    @Override
                    public void onError(Throwable e) {
                        if(!subscriber.isUnsubscribed()) {
                            subscriber.onError(e);
                        }
                    }

                    @Override
                    public void onNext(T t) {
                        if (!subscriber.isUnsubscribed()) {
                            subscriber.onNext(t);
                        } else {
                            ReferenceCountUtil.release(t);
                        }
                    }
                });
            }
        });
    }
}
