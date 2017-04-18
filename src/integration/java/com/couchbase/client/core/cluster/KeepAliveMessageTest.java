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
package com.couchbase.client.core.cluster;

import com.couchbase.client.core.event.CouchbaseEvent;
import com.couchbase.client.core.event.metrics.LatencyMetric;
import com.couchbase.client.core.event.metrics.NetworkLatencyMetricsEvent;
import com.couchbase.client.core.metrics.NetworkLatencyMetricsIdentifier;
import com.couchbase.client.core.util.ClusterDependentTest;
import org.junit.BeforeClass;
import org.junit.Test;
import rx.Observable;
import rx.functions.Func1;
import rx.observers.TestSubscriber;

import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Verifies that KeepAlives are really sent across idle channels
 *
 * @author Michael Nitschinger
 * @since 1.3.0
 */
public class KeepAliveMessageTest extends ClusterDependentTest {

    @BeforeClass
    public static void setup() throws Exception {
        connect(false);
    }

    @Test
    public void shouldSendKeepAlives() throws Exception {
        Observable<CouchbaseEvent> eventBus = env().eventBus().get();
        TestSubscriber<CouchbaseEvent> eventSubscriber = new TestSubscriber<CouchbaseEvent>();
        eventBus
                .filter(new Func1<CouchbaseEvent, Boolean>() {
                    @Override
                    public Boolean call(CouchbaseEvent event) {
                        return event instanceof NetworkLatencyMetricsEvent;
                    }
                }).subscribe(eventSubscriber);

        Thread.sleep(ClusterDependentTest.KEEPALIVE_INTERVAL + 1000);

        env().networkLatencyMetricsCollector().triggerEmit();

        Thread.sleep(100);

        List<CouchbaseEvent> events = eventSubscriber.getOnNextEvents();
        assertEquals(1, events.size());
        NetworkLatencyMetricsEvent event = (NetworkLatencyMetricsEvent) events.get(0);

        int keepalivesFound = 0;
        for (Map.Entry<NetworkLatencyMetricsIdentifier, LatencyMetric> metric : event.latencies().entrySet()) {
            System.err.println(metric.getKey());
            if (metric.getKey().request().equals("KeepAliveRequest")) {
                keepalivesFound++;
            }
        }
        assertTrue(keepalivesFound > 0);
    }
}
