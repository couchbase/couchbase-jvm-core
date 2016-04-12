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
package com.couchbase.client.core.event;

import org.junit.Test;
import rx.observers.TestSubscriber;
import rx.schedulers.Schedulers;
import rx.schedulers.TestScheduler;

import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

/**
 * Verifies the functionality of the {@link DefaultEventBus}.
 *
 * @author Michael Nitschinger
 * @since 1.1.0
 */
public class DefaultEventBusTest {

    @Test
    public void shouldPublishToSubscriber() throws Exception {
        TestScheduler testScheduler = Schedulers.test();
        EventBus eventBus = new DefaultEventBus(testScheduler);

        TestSubscriber<CouchbaseEvent> subscriber = new TestSubscriber<CouchbaseEvent>();
        assertFalse(eventBus.hasSubscribers());
        eventBus.get().subscribe(subscriber);
        assertTrue(eventBus.hasSubscribers());

        CouchbaseEvent event1 = mock(CouchbaseEvent.class);
        CouchbaseEvent event2 = mock(CouchbaseEvent.class);

        eventBus.publish(event1);
        eventBus.publish(event2);

        testScheduler.advanceTimeBy(1, TimeUnit.SECONDS);

        assertEquals(2, subscriber.getOnNextEvents().size());
        assertEquals(0, subscriber.getOnErrorEvents().size());
        assertEquals(0, subscriber.getOnCompletedEvents().size());
    }

    @Test
    public void shouldNotBufferEventsBeforeSubscribe() throws Exception {
        TestScheduler testScheduler = Schedulers.test();
        EventBus eventBus = new DefaultEventBus(testScheduler);

        TestSubscriber<CouchbaseEvent> subscriber = new TestSubscriber<CouchbaseEvent>();


        CouchbaseEvent event1 = mock(CouchbaseEvent.class);
        CouchbaseEvent event2 = mock(CouchbaseEvent.class);

        eventBus.publish(event1);
        assertFalse(eventBus.hasSubscribers());
        eventBus.get().subscribe(subscriber);
        assertTrue(eventBus.hasSubscribers());
        eventBus.publish(event2);

        testScheduler.advanceTimeBy(1, TimeUnit.SECONDS);

        assertEquals(1, subscriber.getOnNextEvents().size());
        assertEquals(0, subscriber.getOnErrorEvents().size());
        assertEquals(0, subscriber.getOnCompletedEvents().size());
    }

}