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

package com.couchbase.client.core.utils;

import com.couchbase.client.core.CouchbaseException;
import com.couchbase.client.core.env.CoreScheduler;
import com.couchbase.client.core.message.CouchbaseResponse;
import org.junit.Test;
import rx.Scheduler;
import rx.observers.TestSubscriber;
import rx.schedulers.Schedulers;
import rx.subjects.AsyncSubject;
import rx.subjects.Subject;

import static org.junit.Assert.*;

/**
 * Verifies the functionality of the {@link Observables} util class.
 *
 * @author Michael Nitschinger
 * @since 1.4.0
 */
public class ObservablesTest {

    @Test
    public void shouldFailSafeOnCoreScheduler() {
        testFailSafe(new CoreScheduler(1), "cb-computations");
    }

    @Test
    public void shouldFailSafeOnCustomScheduler() {
        testFailSafe(Schedulers.computation(), "RxComputationScheduler");
    }

    @Test
    public void shouldFailSafeDirectly() {
        testFailSafe(null, "main");
    }

    /**
     * Heper method to test fail-safe functionality.
     *
     * @param scheduler the scheduler to test against. if null, it will be failed on the current thread.
     * @param threadName the part of a thread name to match against for additional verification.
     */
    private static void testFailSafe(final Scheduler scheduler, final String threadName) {
        Subject<CouchbaseResponse, CouchbaseResponse> subject = AsyncSubject.create();
        TestSubscriber<CouchbaseResponse> subscriber = TestSubscriber.create();
        subject.subscribe(subscriber);
        Exception failure = new CouchbaseException("Some Error");

        Observables.failSafe(scheduler, scheduler != null, subject, failure);

        subscriber.awaitTerminalEvent();
        subscriber.assertError(failure);
        assertTrue(subscriber.getLastSeenThread().getName().contains(threadName));
    }

}