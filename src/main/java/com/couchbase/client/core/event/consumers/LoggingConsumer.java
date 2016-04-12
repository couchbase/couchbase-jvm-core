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
package com.couchbase.client.core.event.consumers;

import com.couchbase.client.core.event.CouchbaseEvent;
import com.couchbase.client.core.logging.CouchbaseLogLevel;
import com.couchbase.client.core.logging.CouchbaseLogger;
import com.couchbase.client.core.logging.CouchbaseLoggerFactory;
import com.couchbase.client.core.utils.Events;
import rx.Subscriber;

/**
 * Consumes {@link CouchbaseEvent}s and logs them.
 *
 * @author Michael Nitschinger
 * @since 1.2.0
 */
public class LoggingConsumer extends Subscriber<CouchbaseEvent> {

    public static final OutputFormat DEFAULT_FORMAT = OutputFormat.JSON;
    private static final CouchbaseLogger LOGGER = CouchbaseLoggerFactory.getInstance(LoggingConsumer.class);

    private final CouchbaseLogLevel level;
    private final OutputFormat outputFormat;

    private LoggingConsumer(CouchbaseLogLevel level, OutputFormat outputFormat) {
        super();
        this.level = level;
        this.outputFormat = outputFormat;
    }

    public static LoggingConsumer create() {
        return create(CouchbaseLogLevel.INFO, DEFAULT_FORMAT);
    }

    public static LoggingConsumer create(CouchbaseLogLevel level, OutputFormat outputFormat) {
        return new LoggingConsumer(level, outputFormat);
    }

    @Override
    public void onCompleted() {
        LOGGER.trace("Event stream completed in logging consumer.");
    }

    @Override
    public void onError(Throwable ex) {
        LOGGER.warn("Received error in logging consumer.", ex);
    }

    @Override
    public void onNext(CouchbaseEvent event) {
        try {
            switch (outputFormat) {
                case JSON:
                    LOGGER.log(level, Events.toJson(event, false));
                    break;
                case JSON_PRETTY:
                    LOGGER.log(level, Events.toJson(event, true));
                    break;
                case TO_STRING:
                    LOGGER.log(level, event.toString());
                    break;
                default:
                    throw new IllegalArgumentException("Unsupported output format: " + outputFormat.toString());
            }
        } catch (Exception ex) {
            LOGGER.warn("Received error while logging event in logging consumer.", ex);
        }
    }

    /**
     * The target output format to log.
     */
    public enum OutputFormat {

        /**
         * Will log compact json.
         */
        JSON,

        /**
         * Will log pretty printed json.
         */
        JSON_PRETTY,

        /**
         * Will call {@link Object#toString()} and log the result.
         */
        TO_STRING
    }

}
