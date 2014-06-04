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
package com.couchbase.client.core.config.refresher;

import com.couchbase.client.core.ClusterFacade;
import com.couchbase.client.core.config.BucketConfig;
import com.couchbase.client.core.message.CouchbaseResponse;
import com.couchbase.client.core.message.ResponseStatus;
import com.couchbase.client.core.message.config.BucketStreamingRequest;
import com.couchbase.client.core.message.config.BucketStreamingResponse;
import com.couchbase.client.core.util.Resources;
import org.junit.Test;
import rx.Observable;
import rx.functions.Action1;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.isA;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Verifies the correct functionality of the {@link HttpRefresher}.
 *
 * @author Michael Nitschinger
 * @since 1.0
 */
public class HttpRefresherTest {

    @Test
    public void shouldPublishNewBucketConfiguration() throws Exception {
        ClusterFacade cluster = mock(ClusterFacade.class);

        Observable<String> configStream = Observable.from(
            Resources.read("stream1.json", this.getClass()),
            Resources.read("stream2.json", this.getClass()),
            Resources.read("stream3.json", this.getClass())
        );
        Observable<CouchbaseResponse> response = Observable.from((CouchbaseResponse)
            new BucketStreamingResponse(configStream, "", ResponseStatus.SUCCESS, null)
        );
        when(cluster.send(isA(BucketStreamingRequest.class))).thenReturn(response);

        HttpRefresher refresher = new HttpRefresher(cluster);

        final CountDownLatch latch = new CountDownLatch(3);
        refresher.configs().subscribe(new Action1<BucketConfig>() {
            @Override
            public void call(BucketConfig bucketConfig) {
                assertEquals("default", bucketConfig.name());
                latch.countDown();
            }
        });

        Observable<Boolean> observable = refresher.registerBucket("default", "");
        assertTrue(observable.toBlocking().single());
        assertTrue(latch.await(3, TimeUnit.SECONDS));
    }

    @Test
    public void shouldFallbackToVerboseIfTerseFails() throws Exception {
        ClusterFacade cluster = mock(ClusterFacade.class);

        Observable<String> configStream = Observable.from(
            Resources.read("stream1.json", this.getClass()),
            Resources.read("stream2.json", this.getClass()),
            Resources.read("stream3.json", this.getClass())
        );

        Observable<CouchbaseResponse> failingResponse = Observable.error(new Exception("failed"));
        Observable<CouchbaseResponse> successResponse = Observable.from((CouchbaseResponse)
                new BucketStreamingResponse(configStream, "", ResponseStatus.SUCCESS, null)
        );
        when(cluster.send(isA(BucketStreamingRequest.class))).thenReturn(failingResponse);
        when(cluster.send(isA(BucketStreamingRequest.class))).thenReturn(successResponse);

        HttpRefresher refresher = new HttpRefresher(cluster);

        final CountDownLatch latch = new CountDownLatch(3);
        refresher.configs().subscribe(new Action1<BucketConfig>() {
            @Override
            public void call(BucketConfig bucketConfig) {
                assertEquals("default", bucketConfig.name());
                latch.countDown();
            }
        });

        Observable<Boolean> observable = refresher.registerBucket("default", "");
        assertTrue(observable.toBlocking().single());
        assertTrue(latch.await(3, TimeUnit.SECONDS));
    }

}