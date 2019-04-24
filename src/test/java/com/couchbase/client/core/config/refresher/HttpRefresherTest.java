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
package com.couchbase.client.core.config.refresher;

import com.couchbase.client.core.ClusterFacade;
import com.couchbase.client.core.config.BucketConfig;
import com.couchbase.client.core.config.ProposedBucketConfigContext;
import com.couchbase.client.core.config.parser.BucketConfigParser;
import com.couchbase.client.core.env.CoreEnvironment;
import com.couchbase.client.core.env.DefaultCoreEnvironment;
import com.couchbase.client.core.message.CouchbaseResponse;
import com.couchbase.client.core.message.ResponseStatus;
import com.couchbase.client.core.message.config.BucketStreamingRequest;
import com.couchbase.client.core.message.config.BucketStreamingResponse;
import com.couchbase.client.core.util.Resources;
import org.junit.Test;
import rx.Observable;
import rx.functions.Action1;
import rx.functions.Func1;
import rx.schedulers.Schedulers;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.internal.verification.VerificationModeFactory.atLeast;

/**
 * Verifies the correct functionality of the {@link HttpRefresher}.
 *
 * @author Michael Nitschinger
 * @since 1.0
 */
public class HttpRefresherTest {

    private static final CoreEnvironment environment = DefaultCoreEnvironment.create();

    @Test
    public void shouldPublishNewBucketConfiguration() throws Exception {
        ClusterFacade cluster = mock(ClusterFacade.class);

        Observable<String> configStream = Observable.just(
            Resources.read("stream1.json", this.getClass()),
            Resources.read("stream2.json", this.getClass()),
            Resources.read("stream3.json", this.getClass())
        ).observeOn(Schedulers.computation());

        Observable<CouchbaseResponse> response = Observable.just((CouchbaseResponse)
            new BucketStreamingResponse(configStream, "", ResponseStatus.SUCCESS, null)
        );
        when(cluster.send(isA(BucketStreamingRequest.class))).thenReturn(response);

        HttpRefresher refresher = new HttpRefresher(environment, cluster);

        final CountDownLatch latch = new CountDownLatch(3);
        refresher.configs()
            .map(new Func1<ProposedBucketConfigContext, BucketConfig>() {
                @Override
                public BucketConfig call(ProposedBucketConfigContext ctx) {
                    return BucketConfigParser.parse(ctx.config(), mock(CoreEnvironment.class), null);
                }
            })
            .subscribe(new Action1<BucketConfig>() {
                @Override
                public void call(BucketConfig bucketConfig) {
                    assertEquals("default", bucketConfig.name());
                    latch.countDown();
                }
            });

        Observable<Boolean> observable = refresher.registerBucket("default", "");
        assertTrue(observable.toBlocking().single());
        assertTrue(latch.await(3, TimeUnit.SECONDS));

        refresher.deregisterBucket("default");
    }

    @Test
    @SuppressWarnings("unchecked")
    public void shouldNotFallbackToVerboseIfTerseFails() {
        ClusterFacade cluster = mock(ClusterFacade.class);

        Observable<String> configStream = Observable.just(
            Resources.read("stream1.json", this.getClass()),
            Resources.read("stream2.json", this.getClass()),
            Resources.read("stream3.json", this.getClass())
        ).observeOn(Schedulers.computation());

        Observable<CouchbaseResponse> failingResponse = Observable.error(new Exception("failed"));
        Observable<CouchbaseResponse> successResponse = Observable.just((CouchbaseResponse)
                new BucketStreamingResponse(configStream, "", ResponseStatus.SUCCESS, null)
        );
        when(cluster.send(isA(BucketStreamingRequest.class))).thenReturn(failingResponse, successResponse);

        HttpRefresher refresher = new HttpRefresher(environment, cluster);

        final AtomicInteger count = new AtomicInteger(0);
        refresher.configs()
            .map(new Func1<ProposedBucketConfigContext, BucketConfig>() {
                @Override
                public BucketConfig call(ProposedBucketConfigContext ctx) {
                    return BucketConfigParser.parse(ctx.config(), mock(CoreEnvironment.class), null);
                }
            })
            .subscribe(new Action1<BucketConfig>() {
                @Override
                public void call(BucketConfig bucketConfig) {
                    count.incrementAndGet();
                }
            });

        Observable<Boolean> observable = refresher.registerBucket("default", "");
        assertTrue(observable.toBlocking().single());
        assertEquals(0, count.get());

        refresher.deregisterBucket("default");
    }

    @Test
    @SuppressWarnings("unchecked")
    public void shouldFallbackToVerboseIfTerseIsNotExists() throws Exception {
        ClusterFacade cluster = mock(ClusterFacade.class);

        Observable<String> configStream = Observable.just(
            Resources.read("stream1.json", this.getClass()),
            Resources.read("stream2.json", this.getClass()),
            Resources.read("stream3.json", this.getClass())
        ).observeOn(Schedulers.computation());

        Observable<CouchbaseResponse> failingResponse = Observable.just((CouchbaseResponse)
                        new BucketStreamingResponse(null, "", ResponseStatus.NOT_EXISTS, null)
        );
        Observable<CouchbaseResponse> successResponse = Observable.just((CouchbaseResponse)
                new BucketStreamingResponse(configStream, "", ResponseStatus.SUCCESS, null)
        );

        when(cluster.send(isA(BucketStreamingRequest.class))).thenReturn(failingResponse, successResponse);

        HttpRefresher refresher = new HttpRefresher(environment, cluster);

        final CountDownLatch latch = new CountDownLatch(3);
        refresher.configs()
            .map(new Func1<ProposedBucketConfigContext, BucketConfig>() {
                @Override
                public BucketConfig call(ProposedBucketConfigContext ctx) {
                    return BucketConfigParser.parse(ctx.config(), mock(CoreEnvironment.class), null);
                }
            })
            .subscribe(new Action1<BucketConfig>() {
                @Override
                public void call(BucketConfig bucketConfig) {
                    assertEquals("default", bucketConfig.name());
                    latch.countDown();
                }
            });

        Observable<Boolean> observable = refresher.registerBucket("default", "");
        assertTrue(observable.toBlocking().single());
        assertTrue(latch.await(3, TimeUnit.SECONDS));

        refresher.deregisterBucket("default");
    }

    @Test
    public void shouldResubscribeIfClosed() throws Exception {
        ClusterFacade cluster = mock(ClusterFacade.class);

        Observable<String> configStream = Observable.just(
            Resources.read("stream1.json", this.getClass()),
            Resources.read("stream2.json", this.getClass()),
            Resources.read("stream3.json", this.getClass())
        ).observeOn(Schedulers.computation());

        Observable<CouchbaseResponse> response = Observable.just((CouchbaseResponse)
            new BucketStreamingResponse(configStream, "", ResponseStatus.SUCCESS, null)
        );
        when(cluster.send(isA(BucketStreamingRequest.class))).thenReturn(response);

        HttpRefresher refresher = new HttpRefresher(environment, cluster);

        final CountDownLatch latch = new CountDownLatch(3);
        refresher.configs()
            .map(new Func1<ProposedBucketConfigContext, BucketConfig>() {
                @Override
                public BucketConfig call(ProposedBucketConfigContext ctx) {
                    return BucketConfigParser.parse(ctx.config(), mock(CoreEnvironment.class), null);
                }
            })
            .subscribe(new Action1<BucketConfig>() {
                @Override
                public void call(BucketConfig bucketConfig) {
                    assertEquals("default", bucketConfig.name());
                    latch.countDown();
                }
            });

        Observable<Boolean> observable = refresher.registerBucket("default", "");
        assertTrue(observable.toBlocking().single());
        assertTrue(latch.await(3, TimeUnit.SECONDS));

        refresher.deregisterBucket("default");
        verify(cluster, atLeast(2)).send(isA(BucketStreamingRequest.class));
    }

}