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

import com.couchbase.client.core.env.CoreScheduler;
import com.couchbase.client.core.logging.CouchbaseLogger;
import com.couchbase.client.core.logging.CouchbaseLoggerFactory;
import com.couchbase.client.core.message.CouchbaseResponse;
import rx.Scheduler;
import rx.functions.Action0;
import rx.subjects.Subject;

/**
 * Various utilities to help with {@link Observables}.
 *
 * @author Michael Nitschinger
 * @since 1.4.0
 */
public enum Observables {
    ;

    /**
     * The logger used.
     */
    private static final CouchbaseLogger LOGGER = CouchbaseLoggerFactory.getInstance(Observables.class);

    /**
     * Helper method to fail a given observable while still making sure to move it onto the proper
     * scheduler for handling if needed (similar to onNext).
     */
    public static void failSafe(final Scheduler scheduler, final boolean moveOut,
        final Subject<CouchbaseResponse, CouchbaseResponse> observable, final Throwable err) {
        if (moveOut) {
            if (scheduler instanceof CoreScheduler) {
                ((CoreScheduler) scheduler).scheduleDirect(new Action0() {
                    @Override
                    public void call() {
                        try {
                            observable.onError(err);
                        } catch (Exception ex) {
                            LOGGER.warn("Caught exception while onError on observable", ex);
                        }
                    }
                });
            } else {
                final Scheduler.Worker worker = scheduler.createWorker();
                worker.schedule(new Action0() {
                    @Override
                    public void call() {
                        try {
                            observable.onError(err);
                        } catch (Exception ex) {
                            LOGGER.warn("Caught exception while onError on observable", ex);
                        } finally {
                            worker.unsubscribe();
                        }
                    }
                });
            }
        } else {
            observable.onError(err);
        }
    }

}
