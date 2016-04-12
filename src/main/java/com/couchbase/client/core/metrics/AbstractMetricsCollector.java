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
package com.couchbase.client.core.metrics;

import com.couchbase.client.core.event.CouchbaseEvent;
import com.couchbase.client.core.event.EventBus;
import com.couchbase.client.core.logging.CouchbaseLogger;
import com.couchbase.client.core.logging.CouchbaseLoggerFactory;
import rx.Observable;
import rx.Scheduler;
import rx.Subscription;
import rx.functions.Action1;

/**
 * A common base class for all metrics collectors which emit events.
 *
 * @author Michael Nitschinger
 * @since 1.2.0
 */
public abstract class AbstractMetricsCollector implements MetricsCollector {

    private static final CouchbaseLogger LOGGER = CouchbaseLoggerFactory.getInstance(MetricsCollector.class);

    private final MetricsCollectorConfig config;
    private final Subscription subscription;
    private final boolean isEnabled;
    private final Scheduler scheduler;
    private final EventBus eventBus;

    protected AbstractMetricsCollector(final EventBus eventBus, Scheduler scheduler, MetricsCollectorConfig config) {
        this.config = config;
        this.scheduler = scheduler;
        this.eventBus = eventBus;
        isEnabled = config.emitFrequency() > 0;


        if (config.emitFrequency() > 0) {
            subscription = Observable
                    .interval(config.emitFrequency(), config.emitFrequencyUnit(), scheduler)
                    .subscribe(new Action1<Long>() {
                        @Override
                        public void call(Long ignored) {
                            CouchbaseEvent event = generateCouchbaseEvent();
                            if (LOGGER.isTraceEnabled()) {
                                LOGGER.trace("Emitting Metric to EventBus: {}", event);
                            }
                            eventBus.publish(event);
                        }
                    });
        } else {
            subscription = null;
        }
    }

    /**
     * Generate the actual {@link CouchbaseEvent} to emit on every interval.
     */
    protected abstract CouchbaseEvent generateCouchbaseEvent();

    @Override
    public MetricsCollectorConfig config() {
        return config;
    }

    @Override
    public boolean shutdown() {
        if (subscription == null) {
            return true;
        }

        if (!subscription.isUnsubscribed()) {
            subscription.unsubscribe();
        }

        return true;
    }

    @Override
    public boolean isEnabled() {
        return isEnabled;
    }

    @Override
    public void triggerEmit() {
        if (!isEnabled()) {
            return;
        }

        Observable
            .just(generateCouchbaseEvent())
            .subscribeOn(scheduler)
            .subscribe(new Action1<CouchbaseEvent>() {
                @Override
                public void call(CouchbaseEvent event) {
                    if (LOGGER.isDebugEnabled()) {
                        LOGGER.trace("Manually Triggering Metric Emit to EventBus: {}", event);
                    }
                    eventBus.publish(event);
                }
            });
    }
}
