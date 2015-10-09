/*
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

package com.couchbase.client.core.env.resources;

import io.netty.util.ThreadDeathWatcher;
import rx.Observable;
import rx.Subscriber;

import java.util.concurrent.TimeUnit;

/**
 * {@link ShutdownHook} hook that attempts to terminate Netty threads gracefully.
 * It won't report a failure if said threads cannot be terminated right away though.
 *
 * @author Simon Baslé
 * @since 2.2
 */
public class NettyShutdownHook implements ShutdownHook {

    private volatile boolean isReallyShutdown = false;

    @Override
    public Observable<Boolean> shutdown() {
        return Observable.create(new Observable.OnSubscribe<Boolean>() {
            @Override
            public void call(final Subscriber<? super Boolean> subscriber) {
                new Thread(new Runnable() {
                    @Override
                    public void run() {
                        try {
                            isReallyShutdown = ThreadDeathWatcher.awaitInactivity(3, TimeUnit.SECONDS);
                            if (!subscriber.isUnsubscribed()) {
                                subscriber.onNext(isReallyShutdown);
                                subscriber.onCompleted();
                            }
                        } catch (Throwable e) {
                            if (!subscriber.isUnsubscribed()) {
                                subscriber.onError(e);
                            }
                        }
                    }
                }).start();
            }
        });
    }

    @Override
    public boolean isShutdown() {
        return isReallyShutdown;
    }

}
