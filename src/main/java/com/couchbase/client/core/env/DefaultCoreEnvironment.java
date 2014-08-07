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
package com.couchbase.client.core.env;

import com.couchbase.client.core.logging.InternalLogger;
import com.couchbase.client.core.logging.InternalLoggerFactory;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;
import rx.Observable;
import rx.Scheduler;
import rx.Subscriber;

public class DefaultCoreEnvironment implements CoreEnvironment {

    /**
     * The logger used.
     */
    private static final InternalLogger LOGGER = InternalLoggerFactory.getInstance(CoreEnvironment.class);
    private static final int MAX_ALLOWED_INSTANCES = 1;
    private static volatile int instanceCounter = 0;

    private final CoreProperties properties;
    private final EventLoopGroup ioPool;
    private final Scheduler coreScheduler;
    private volatile boolean shutdown;

    protected DefaultCoreEnvironment(final CoreProperties properties) {
        if (++instanceCounter > MAX_ALLOWED_INSTANCES) {
            LOGGER.warn("More than " + MAX_ALLOWED_INSTANCES + " Couchbase Environments found, " +
                "this can have severe impact on performance and stability. Reuse environments!");
        }
        this.properties = properties;
        this.ioPool = new NioEventLoopGroup(properties().ioPoolSize());
        this.coreScheduler = new CoreScheduler(properties().computationPoolSize());
        this.shutdown = false;
    }

    public static DefaultCoreEnvironment create() {
        return create(DynamicCoreProperties.create());
    }

    public static DefaultCoreEnvironment create(final CoreProperties properties) {
        return new DefaultCoreEnvironment(properties);
    }

    @Override
    public EventLoopGroup ioPool() {
        return ioPool;
    }

    @Override
    @SuppressWarnings("unchecked")
    public Observable<Boolean> shutdown() {
        if (shutdown) {
            return Observable.just(true);
        }

        return Observable.create(new Observable.OnSubscribe<Boolean>() {
            @Override
            public void call(final Subscriber<? super Boolean> subscriber) {
                if (shutdown) {
                    subscriber.onNext(true);
                    subscriber.onCompleted();
                }

                ioPool.shutdownGracefully().addListener(new GenericFutureListener() {
                    @Override
                    public void operationComplete(final Future future) throws Exception {
                        if (!subscriber.isUnsubscribed()) {
                            if (future.isSuccess()) {
                                subscriber.onNext(future.isSuccess());
                                subscriber.onCompleted();
                            } else {
                                subscriber.onError(future.cause());
                            }
                        }
                    }
                });
            }
        });
    }

    @Override
    public CoreProperties properties() {
        return properties;
    }

    @Override
    public Scheduler scheduler() {
        return coreScheduler;
    }
}
