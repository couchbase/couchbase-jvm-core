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

import com.couchbase.client.core.logging.CouchbaseLogger;
import com.couchbase.client.core.logging.CouchbaseLoggerFactory;
import rx.Observable;
import rx.Scheduler;
import rx.subjects.PublishSubject;
import rx.subjects.Subject;

/**
 * The default event bus implementation.
 *
 * @author Michael Nitschinger
 * @since 1.1.0
 */
public class DefaultEventBus implements EventBus {

    private static final CouchbaseLogger LOGGER = CouchbaseLoggerFactory.getInstance(DefaultEventBus.class);

    private final Subject<CouchbaseEvent, CouchbaseEvent> bus;
    private final Scheduler scheduler;

    public DefaultEventBus(final Scheduler scheduler) {
        bus = PublishSubject.<CouchbaseEvent>create().toSerialized();
        this.scheduler = scheduler;
    }

    @Override
    public Observable<CouchbaseEvent> get() {
        return bus.onBackpressureDrop().observeOn(scheduler);
    }

    @Override
    public void publish(final CouchbaseEvent event) {
        if (bus.hasObservers()) {
            try {
                bus.onNext(event);
            } catch (Exception ex) {
                LOGGER.warn("Caught exception during event emission, moving on.", ex);
            }
        }
    }

    @Override
    public boolean hasSubscribers() {
        return bus.hasObservers();
    }
}
