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

    private static final CouchbaseLogger LOGGER = CouchbaseLoggerFactory.getInstance(LoggingConsumer.class);

    private final CouchbaseLogLevel level;
    private final OutputFormat outputFormat;

    private LoggingConsumer(CouchbaseLogLevel level, OutputFormat outputFormat) {
        super();
        this.level = level;
        this.outputFormat = outputFormat;
    }

    public static LoggingConsumer create() {
        return create(CouchbaseLogLevel.INFO, OutputFormat.JSON_PRETTY);
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
