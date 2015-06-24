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