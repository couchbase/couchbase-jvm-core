/*
 * Copyright (c) 2018 Couchbase, Inc.
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

package com.couchbase.client.core.tracing;

import com.couchbase.client.core.logging.CouchbaseLogger;
import com.couchbase.client.core.logging.CouchbaseLoggerFactory;
import io.netty.util.internal.shaded.org.jctools.queues.MpscUnboundedArrayQueue;
import io.opentracing.Tracer;
import io.opentracing.tag.Tags;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static com.couchbase.client.core.logging.RedactableArgument.system;
import static com.couchbase.client.core.utils.DefaultObjectMapper.prettyWriter;
import static com.couchbase.client.core.utils.DefaultObjectMapper.writer;

/**
 * The {@link ThresholdLogReporter} retrieves spans from (usually) a {@link Tracer}
 * and stores them for threshold-based aggregation and logging.
 *
 * @author Michael Nitschinger
 * @since 1.6.0
 */
public class ThresholdLogReporter {

    /**
     * The Logger used.
     */
    private static final CouchbaseLogger LOGGER =
        CouchbaseLoggerFactory.getInstance(ThresholdLogReporter.class);

    private static final AtomicInteger REPORTER_ID = new AtomicInteger();

    private static final long MIN_LOG_INTERVAL = TimeUnit.SECONDS.toNanos(1);

    public static final String SERVICE_KV = "kv";
    public static final String SERVICE_N1QL = "n1ql";
    public static final String SERVICE_FTS = "search";
    public static final String SERVICE_VIEW = "view";
    public static final String SERVICE_ANALYTICS = "analytics";

    public static final String KEY_TOTAL_MICROS = "total_us";
    public static final String KEY_DISPATCH_MICROS = "last_dispatch_us";
    public static final String KEY_ENCODE_MICROS = "encode_us";
    public static final String KEY_DECODE_MICROS = "decode_us";
    public static final String KEY_SERVER_MICROS = "server_us";

    private final Queue<ThresholdLogSpan> overThresholdQueue;

    private final long kvThreshold;
    private final long n1qlThreshold;
    private final long viewThreshold;
    private final long ftsThreshold;
    private final long analyticsThreshold;
    private final long logIntervalNanos;
    private final int sampleSize;
    private final boolean pretty;

    private final Thread worker;

    private volatile boolean running;

    public static ThresholdLogReporter.Builder builder() {
        return new Builder();
    }

    public static ThresholdLogReporter disabled() {
        return builder().logInterval(0, TimeUnit.SECONDS).build();
    }

    public static ThresholdLogReporter create() {
        return builder().build();
    }

    /**
     * Creates the reporter based on its config.
     *
     * @param builder the builder for configuration.
     */
    ThresholdLogReporter(final Builder builder) {
        logIntervalNanos = builder.logIntervalUnit.toNanos(builder.logInterval);
        sampleSize = builder.sampleSize;
        if (logIntervalNanos > 0 && logIntervalNanos < minLogInterval()) {
            throw new IllegalArgumentException("The log interval needs to be either 0 or greater than "
                + MIN_LOG_INTERVAL + " micros");
        }

        overThresholdQueue = new MpscUnboundedArrayQueue<ThresholdLogSpan>(builder.spanQueueSize);
        kvThreshold = builder.kvThreshold;
        analyticsThreshold = builder.analyticsThreshold;
        ftsThreshold = builder.ftsThreshold;
        viewThreshold = builder.viewThreshold;
        n1qlThreshold = builder.n1qlThreshold;
        pretty = builder.pretty;
        running = true;

        if (logIntervalNanos > 0) {
            worker = new Thread(new Worker());
            worker.setDaemon(true);
            worker.start();
        } else {
            worker = null;
            LOGGER.debug("ThresholdLogReporter disabled via config.");
        }
    }

    /**
     * Returns the minimum log interval.
     *
     * This can be overridden by test impls if needed.
     */
    long minLogInterval() {
        return MIN_LOG_INTERVAL;
    }

    /**
     * Reports the given span, but it doesn't have to be a potential slow.
     *
     * This method, based on its configuration, will figure out if the given
     * span is indeed eligible for being part in the log.
     *
     * @param span the span to report.
     */
    public void report(final ThresholdLogSpan span) {
        if (isOverThreshold(span)) {
            if (!overThresholdQueue.offer(span)) {
                LOGGER.debug("Could not enqueue span {} for over threshold reporting, discarding.", span);
            }
        }
    }

    /**
     * Checks if the given span is over the threshold and eligible for being
     * reported.
     *
     * @param span the span to check.
     * @return true if it is, false otherwise.
     */
    private boolean isOverThreshold(final ThresholdLogSpan span) {
        String service = (String) span.tag(Tags.PEER_SERVICE.getKey());
        if (SERVICE_KV.equals(service)) {
            return span.durationMicros() >= kvThreshold;
        } else if (SERVICE_N1QL.equals(service)) {
            return span.durationMicros() >= n1qlThreshold;
        } else if (SERVICE_VIEW.equals(service)) {
            return span.durationMicros() >= viewThreshold;
        } else if (SERVICE_FTS.equals(service)) {
            return span.durationMicros() >= ftsThreshold;
        } else if (SERVICE_ANALYTICS.equals(service)) {
            return span.durationMicros() >= analyticsThreshold;
        } else {
            return false;
        }
    }

    /**
     * Stop the reporter and its associated threads.
     */
    public void shutdown() {
        running = false;
        if (worker != null) {
            worker.interrupt();
        }
    }

    /**
     * The builder used to configure the {@link ThresholdLogReporter}.
     */
    public static class Builder {

        private static final long DEFAULT_LOG_INTERVAL = 10;
        private static final TimeUnit DEFAULT_LOG_INTERVAL_UNIT = TimeUnit.SECONDS;
        private static final int DEFAULT_SPAN_QUEUE_SIZE = 1024;
        private static final long DEFAULT_KV_THRESHOLD = TimeUnit.MILLISECONDS.toMicros(500);
        private static final long DEFAULT_N1QL_THRESHOLD = TimeUnit.SECONDS.toMicros(1);
        private static final long DEFAULT_VIEW_THRESHOLD = TimeUnit.SECONDS.toMicros(1);
        private static final long DEFAULT_FTS_THRESHOLD = TimeUnit.SECONDS.toMicros(1);
        private static final long DEFAULT_ANALYTICS_THRESHOLD = TimeUnit.SECONDS.toMicros(1);
        private static final int DEFAULT_SAMPLE_SIZE = 10;
        private static final boolean DEFAULT_PRETTY = false;

        private long logInterval = DEFAULT_LOG_INTERVAL;
        private TimeUnit logIntervalUnit = DEFAULT_LOG_INTERVAL_UNIT;
        private int spanQueueSize = DEFAULT_SPAN_QUEUE_SIZE;
        private int sampleSize = DEFAULT_SAMPLE_SIZE;
        private boolean pretty = DEFAULT_PRETTY;

        private long kvThreshold = DEFAULT_KV_THRESHOLD;
        private long n1qlThreshold = DEFAULT_N1QL_THRESHOLD;
        private long viewThreshold = DEFAULT_VIEW_THRESHOLD;
        private long ftsThreshold = DEFAULT_FTS_THRESHOLD;
        private long analyticsThreshold = DEFAULT_ANALYTICS_THRESHOLD;

        public ThresholdLogReporter build() {
            return new ThresholdLogReporter(this);
        }

        /**
         * Allows to customize the log interval. if set to 0, disables it.
         *
         * @param interval the interval to use.
         * @param unit the time unit for the interval.
         * @return this builder for chaining.
         */
        public Builder logInterval(final long interval, final TimeUnit unit) {
            this.logInterval = interval;
            this.logIntervalUnit = unit;
            return this;
        }

        /**
         * Allows to configure the queue size for the individual span queues
         * used to track the spans over threshold.
         *
         * @param spanQueueSize the queue size to use.
         * @return this builder for chaining.
         */
        public Builder spanQueueSize(final int spanQueueSize) {
            this.spanQueueSize = spanQueueSize;
            return this;
        }

        /**
         * Allows to customize the kvThreshold.
         *
         * @param kvThreshold the threshold to set.
         * @return this builder for chaining.
         */
        public Builder kvThreshold(final long kvThreshold, final TimeUnit timeUnit) {
            this.kvThreshold = timeUnit.toMicros(kvThreshold);
            return this;
        }

        /**
         * Allows to customize the n1qlThreshold.
         *
         * @param n1qlThreshold the threshold to set.
         * @return this builder for chaining.
         */
        public Builder n1qlThreshold(final long n1qlThreshold, final TimeUnit timeUnit) {
            this.n1qlThreshold = timeUnit.toMicros(n1qlThreshold);
            return this;
        }

        /**
         * Allows to customize the viewThreshold.
         *
         * @param viewThreshold the threshold to set.
         * @return this builder for chaining.
         */
        public Builder viewThreshold(final long viewThreshold, final TimeUnit timeUnit) {
            this.viewThreshold = timeUnit.toMicros(viewThreshold);
            return this;
        }

        /**
         * Allows to customize the ftsThreshold.
         *
         * @param ftsThreshold the threshold to set.
         * @return this builder for chaining.
         */
        public Builder ftsThreshold(final long ftsThreshold, final TimeUnit timeUnit) {
            this.ftsThreshold = timeUnit.toMicros(ftsThreshold);
            return this;
        }

        /**
         * Allows to customize the analyticsThreshold.
         *
         * @param analyticsThreshold the threshold to set.
         * @return this builder for chaining.
         */
        public Builder analyticsThreshold(final long analyticsThreshold, final TimeUnit timeUnit) {
            this.analyticsThreshold = timeUnit.toMicros(analyticsThreshold);
            return this;
        }

        /**
         * Allows to customize the sample size per service.
         *
         * @param sampleSize the sample size to set.
         * @return this builder for chaining.
         */
        public Builder sampleSize(final int sampleSize) {
            this.sampleSize = sampleSize;
            return this;
        }

        /**
         * Allows to set the JSON output to pretty, making it more
         * readable but also more verbose. Helpful during debugging.
         *
         * @param pretty set to true, false by default
         * @return this builder for chaining.
         */
        public Builder pretty(final boolean pretty) {
            this.pretty = pretty;
            return this;
        }

    }

    /**
     * This worker consumes the queue in the given interval and logs what
     * is needed.
     */
    class Worker implements Runnable {

        /**
         * Time this worker spends between check cycles. 100ms should be granular enough
         * but making it configurable, who knows...
         */
        private final long workerSleepMs = Long.parseLong(
            System.getProperty("com.couchbase.thresholdLogReporterSleep", "100")
        );

        private final SortedSet<ThresholdLogSpan> kvThresholdSet = new TreeSet<ThresholdLogSpan>();
        private final SortedSet<ThresholdLogSpan> n1qlThresholdSet = new TreeSet<ThresholdLogSpan>();
        private final SortedSet<ThresholdLogSpan> viewThresholdSet = new TreeSet<ThresholdLogSpan>();
        private final SortedSet<ThresholdLogSpan> ftsThresholdSet = new TreeSet<ThresholdLogSpan>();
        private final SortedSet<ThresholdLogSpan> analyticsThresholdSet = new TreeSet<ThresholdLogSpan>();

        private int kvThresholdCount = 0;
        private int n1qlThresholdCount = 0;
        private int viewThresoldCount = 0;
        private int ftsThresholdCount = 0;
        private int analyticsThresholdCount = 0;

        private long lastThresholdLog;
        private boolean hasThresholdWritten;

        @Override
        public void run() {
            Thread.currentThread().setName("cb-tracing-" + REPORTER_ID.incrementAndGet());
            while (running) {
                try {
                    handleOverThresholdQueue();
                    Thread.sleep(workerSleepMs);
                } catch (final InterruptedException ex) {
                    if (!running) {
                        return;
                    } else {
                        Thread.currentThread().interrupt();
                    }
                } catch (final Exception ex) {
                    LOGGER.warn("Got exception on slow operation reporter, ignoring.", ex);
                }
            }
        }

        /**
         * Helper method which drains the queue, handles the sets and logs if needed.
         */
        private void handleOverThresholdQueue() {
            long now = System.nanoTime();
            if (now > (lastThresholdLog + logIntervalNanos)) {
                prepareAndlogOverThreshold();
                lastThresholdLog = now;
            }

            while (true) {
                ThresholdLogSpan span = overThresholdQueue.poll();
                if (span == null) {
                    return;
                }
                String service = (String) span.tag(Tags.PEER_SERVICE.getKey());
                if (SERVICE_KV.equals(service)) {
                    updateSet(kvThresholdSet, span);
                    kvThresholdCount += 1;
                } else if (SERVICE_N1QL.equals(service)) {
                    updateSet(n1qlThresholdSet, span);
                    n1qlThresholdCount += 1;
                } else if (SERVICE_VIEW.equals(service)) {
                    updateSet(viewThresholdSet, span);
                    viewThresoldCount += 1;
                } else if (SERVICE_FTS.equals(service)) {
                    updateSet(ftsThresholdSet, span);
                    ftsThresholdCount += 1;
                } else if (SERVICE_ANALYTICS.equals(service)) {
                    updateSet(analyticsThresholdSet, span);
                    analyticsThresholdCount += 1;
                } else {
                    LOGGER.warn("Unknown service in span {}", service);
                }
            }
        }

        /**
         * Logs the over threshold data and resets the sets.
         */
        private void prepareAndlogOverThreshold() {
            if (!hasThresholdWritten) {
                return;
            }
            hasThresholdWritten = false;

            List<Map<String, Object>> output = new ArrayList<Map<String, Object>>();

            if (!kvThresholdSet.isEmpty()) {
                output.add(convertThresholdSet(kvThresholdSet, kvThresholdCount, SERVICE_KV));
                kvThresholdSet.clear();
                kvThresholdCount = 0;
            }
            if (!n1qlThresholdSet.isEmpty()) {
                output.add(convertThresholdSet(n1qlThresholdSet, n1qlThresholdCount, SERVICE_N1QL));
                n1qlThresholdSet.clear();
                n1qlThresholdCount = 0;
            }
            if (!viewThresholdSet.isEmpty()) {
                output.add(convertThresholdSet(viewThresholdSet, viewThresoldCount, SERVICE_VIEW));
                viewThresholdSet.clear();
                viewThresoldCount = 0;
            }
            if (!ftsThresholdSet.isEmpty()) {
                output.add(convertThresholdSet(ftsThresholdSet, ftsThresholdCount, SERVICE_FTS));
                ftsThresholdSet.clear();
                ftsThresholdCount = 0;
            }
            if (!analyticsThresholdSet.isEmpty()) {
                output.add(convertThresholdSet(analyticsThresholdSet, analyticsThresholdCount, SERVICE_ANALYTICS));
                analyticsThresholdSet.clear();
                analyticsThresholdCount = 0;
            }
            logOverThreshold(output);
        }

        private Map<String, Object> convertThresholdSet(SortedSet<ThresholdLogSpan> set, int count, String ident) {
            Map<String, Object> output = new HashMap<String, Object>();
            List<Map<String, Object>> top = new ArrayList<Map<String, Object>>();
            for (ThresholdLogSpan span : set) {
                Map<String, Object> entry = new HashMap<String, Object>();
                entry.put(KEY_TOTAL_MICROS, span.durationMicros());

                String spanId = span.request().operationId();
                if (spanId != null) {
                    entry.put("last_operation_id", spanId);
                }

                String operationName = span.operationName();
                if (operationName != null) {
                    entry.put("operation_name", operationName);
                }

                String local = span.request().lastLocalSocket();
                String peer = span.request().lastRemoteSocket();
                if (local != null) {
                    entry.put("last_local_address", system(local).toString());
                }
                if (peer != null) {
                    entry.put("last_remote_address", system(peer).toString());
                }

                String localId = span.request().lastLocalId();
                if (localId != null) {
                    entry.put("last_local_id", system(localId).toString());
                }

                String decode_duration = span.getBaggageItem(KEY_DECODE_MICROS);
                if (decode_duration != null) {
                    entry.put(KEY_DECODE_MICROS, Long.parseLong(decode_duration));
                }

                String encode_duration = span.getBaggageItem(KEY_ENCODE_MICROS);
                if (encode_duration != null) {
                    entry.put(KEY_ENCODE_MICROS, Long.parseLong(encode_duration));
                }

                String dispatch_duration = span.getBaggageItem(KEY_DISPATCH_MICROS);
                if (dispatch_duration != null) {
                    entry.put(KEY_DISPATCH_MICROS, Long.parseLong(dispatch_duration));
                }

                String server_duration = span.getBaggageItem(KEY_SERVER_MICROS);
                if (server_duration != null) {
                    entry.put(KEY_SERVER_MICROS, Long.parseLong(server_duration));
                }
                top.add(entry);
            }
            output.put("service", ident);
            output.put("count", count);
            output.put("top", top);
            return output;
        }

        /**
         * Helper method which updates the set with the span and ensures that the sample
         * size is respected.
         *
         * @param set the set to work with.
         * @param span the span to store.
         */
        private void updateSet(final SortedSet<ThresholdLogSpan> set, final ThresholdLogSpan span) {
            set.add(span);
            while(set.size() > sampleSize) {
                set.remove(set.first());
            }
            hasThresholdWritten = true;
        }
    }

    /**
     * This method is intended to be overridden in test implementations
     * to assert against the output.
     */
    void logOverThreshold(final List<Map<String, Object>> toLog) {
        try {
            String result = pretty
                ? prettyWriter().writeValueAsString(toLog)
                : writer().writeValueAsString(toLog);
            LOGGER.warn("Operations over threshold: {}", result);
        } catch (Exception ex) {
            LOGGER.warn("Could not write threshold log.", ex);
        }
    }
}
