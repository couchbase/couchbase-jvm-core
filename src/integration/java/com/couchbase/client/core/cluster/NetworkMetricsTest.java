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
package com.couchbase.client.core.cluster;

import com.couchbase.client.core.event.CouchbaseEvent;
import com.couchbase.client.core.event.metrics.LatencyMetric;
import com.couchbase.client.core.event.metrics.NetworkLatencyMetricsEvent;
import com.couchbase.client.core.message.kv.GetRequest;
import com.couchbase.client.core.message.kv.GetResponse;
import com.couchbase.client.core.message.kv.InsertRequest;
import com.couchbase.client.core.message.kv.InsertResponse;
import com.couchbase.client.core.message.kv.UpsertRequest;
import com.couchbase.client.core.message.kv.UpsertResponse;
import com.couchbase.client.core.metrics.NetworkLatencyMetricsIdentifier;
import com.couchbase.client.core.util.ClusterDependentTest;
import io.netty.buffer.Unpooled;
import io.netty.util.CharsetUtil;
import io.netty.util.ReferenceCountUtil;
import org.junit.Test;
import rx.Observable;
import rx.functions.Func1;
import rx.observers.TestSubscriber;

import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Verifies the functionality of the network metric collector.
 *
 * @author Michael Nitschinger
 * @since 1.2.0
 */
public class NetworkMetricsTest extends ClusterDependentTest {

    @Test
    public void shouldCapturePerformedOperations() throws Exception {
        Observable<CouchbaseEvent> eventBus = env().eventBus().get();
        TestSubscriber<CouchbaseEvent> eventSubscriber = new TestSubscriber<CouchbaseEvent>();
        eventBus
                .filter(new Func1<CouchbaseEvent, Boolean>() {
                    @Override
                    public Boolean call(CouchbaseEvent event) {
                        return event instanceof NetworkLatencyMetricsEvent;
                    }
                }).subscribe(eventSubscriber);

        assertTrue(eventSubscriber.getOnErrorEvents().isEmpty());
        assertTrue(eventSubscriber.getOnNextEvents().isEmpty());

        InsertResponse insertResponse = cluster()
                .<InsertResponse>send(new InsertRequest("perfIns", Unpooled.copiedBuffer("ins", CharsetUtil.UTF_8), bucket()))
                .toBlocking()
                .single();
        ReferenceCountUtil.release(insertResponse);

        UpsertResponse upsertResponse = cluster()
                .<UpsertResponse>send(new UpsertRequest("perfUps", Unpooled.copiedBuffer("ups", CharsetUtil.UTF_8), bucket()))
                .toBlocking()
                .single();
        ReferenceCountUtil.release(upsertResponse);

        for (int i = 0; i < 5; i++) {
            GetResponse getResponse = cluster().<GetResponse>send(new GetRequest("perfIns", bucket()))
                    .toBlocking()
                    .single();
            ReferenceCountUtil.release(getResponse);

        }

        env().networkLatencyMetricsCollector().triggerEmit();

        Thread.sleep(100);

        List<CouchbaseEvent> events = eventSubscriber.getOnNextEvents();
        assertEquals(1, events.size());
        NetworkLatencyMetricsEvent event = (NetworkLatencyMetricsEvent) events.get(0);

        boolean hasInsert = false;
        boolean hasUpsert = false;
        boolean hasGet = false;
        for (Map.Entry<NetworkLatencyMetricsIdentifier, LatencyMetric> metric : event.latencies().entrySet()) {
            if (metric.getKey().request().equals("InsertRequest")) {
                hasInsert = true;
                assertTrue(metric.getValue().count() >= 1);
            } else if (metric.getKey().request().equals("UpsertRequest")) {
                hasUpsert = true;
                assertTrue(metric.getValue().count() >= 1);
            } else if (metric.getKey().request().equals("GetRequest") && metric.getKey().status().equals("NOT_EXISTS")) {
                hasGet = true;
                assertTrue(metric.getValue().count() >= 5);
            }
            assertTrue(metric.getValue().max() > 0);
            assertTrue(metric.getValue().min() > 0);
            assertTrue(metric.getValue().count() > 0);

            long lastPercentile = 0;
            for (Map.Entry<Double, Long> percentile : metric.getValue().percentiles().entrySet()) {
                assertTrue(percentile.getValue() >= lastPercentile);
                lastPercentile = percentile.getValue();
            }
        }

        assertTrue(hasInsert);
        assertTrue(hasUpsert);
        assertTrue(hasGet);
    }
}
