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
