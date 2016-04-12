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

import io.netty.channel.EventLoopGroup;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;
import rx.Observable;
import rx.Subscriber;

import java.util.concurrent.TimeUnit;

/**
 * {@link ShutdownHook} hook for an {@link EventLoopGroup}.
 *
 * @author Simon Baslé
 * @since 2.2
 */
public class IoPoolShutdownHook implements ShutdownHook {

    private final EventLoopGroup ioPool;
    private volatile boolean shutdown;

    public IoPoolShutdownHook(EventLoopGroup ioPool) {
        this.ioPool = ioPool;
        this.shutdown = false;
    }

    public Observable<Boolean> shutdown() {
        return Observable.create(new Observable.OnSubscribe<Boolean>() {
            @Override
            public void call(final Subscriber<? super Boolean> subscriber) {
                ioPool.shutdownGracefully(0, 10, TimeUnit.MILLISECONDS)
                    .addListener(new GenericFutureListener() {
                    @Override
                    public void operationComplete(final Future future) throws Exception {
                        if (!subscriber.isUnsubscribed()) {
                            try {
                                if (future.isSuccess()) {
                                    subscriber.onNext(true);
                                    shutdown = true;
                                    subscriber.onCompleted();
                                } else {
                                    subscriber.onError(future.cause());
                                }
                            } catch (Exception ex) {
                                subscriber.onError(ex);
                            }
                        }
                    }
                });
            }
        });
    }

    @Override
    public boolean isShutdown() {
        return shutdown;
    }
}
