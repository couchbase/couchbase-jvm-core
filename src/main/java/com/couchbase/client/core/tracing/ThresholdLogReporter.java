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
import com.fasterxml.jackson.databind.ObjectMapper;
import io.netty.util.internal.shaded.org.jctools.queues.MpscUnboundedArrayQueue;
import io.opentracing.Tracer;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

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

    /**
     * TODO: refactor me away once the global jackson is merged
     */
    private static final ObjectMapper JACKSON = new ObjectMapper();

    private static final AtomicInteger REPORTER_ID = new AtomicInteger();

    private static final long MIN_LOG_INTERVAL = TimeUnit.SECONDS.toNanos(1);

    public static final String SERVICE_KV = "kv";
    public static final String SERVICE_N1QL = "n1ql";
    public static final String SERVICE_FTS = "search";
    public static final String SERVICE_VIEW = "view";
    public static final String SERVICE_ANALYTICS = "analytics";


    private final Queue<ThresholdLogSpan> overThresholdQueue;
    private final Queue<ThresholdLogSpan> zombieQueue;

    private final long kvThreshold;
    private final long n1qlThreshold;
    private final long viewThreshold;
    private final long ftsThreshold;
    private final long analyticsThreshold;
    private final long logIntervalNanos;
    private final int sampleSize;

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
        zombieQueue = new MpscUnboundedArrayQueue<ThresholdLogSpan>(builder.spanQueueSize);
        kvThreshold = builder.kvThreshold;
        analyticsThreshold = builder.analyticsThreshold;
        ftsThreshold = builder.ftsThreshold;
        viewThreshold = builder.viewThreshold;
        n1qlThreshold = builder.n1qlThreshold;
        running = true;

        if (logIntervalNanos > 0) {
            Thread worker = new Thread(new Worker());
            worker.setDaemon(true);
            worker.start();
        } else {
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
     * Reports the given span, but it doesn't have to be a potential slow or
     * zombie operation.
     *
     * This method, based on its configuration, will figure out if the given
     * span is indeed eligible for being part in the log.
     *
     * @param span the span to report.
     */
    public void report(final ThresholdLogSpan span) {
        if (isZombie(span)) {
            if (!zombieQueue.offer(span)) {
                LOGGER.debug("Could not enqueue span {} for zombie reporting, discarding.", span);
            }
        } else if (isOverThreshold(span)) {
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
        String service = (String) span.tag("couchbase.service");
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
            LOGGER.warn("Unknown service in span {}", span);
            return false;
        }
    }

    /**
     * Checks if the given span is a zombie span.
     *
     *      "It's not too late for you. My offer still stands: you can join us ... or you can die."
     *          -- Rick
     *
     * @param span the span to check.
     * @return true if it is, false otherwise.
     */
    private boolean isZombie(final ThresholdLogSpan span) {
        return span.getBaggageItem("couchbase.zombie").equals("true");
    }

    /**
     * Stop the reporter and its associated threads.
     */
    public void shutdown() {
        running = false;
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

        private long logInterval = DEFAULT_LOG_INTERVAL;
        private TimeUnit logIntervalUnit = DEFAULT_LOG_INTERVAL_UNIT;
        private int spanQueueSize = DEFAULT_SPAN_QUEUE_SIZE;
        private int sampleSize = DEFAULT_SAMPLE_SIZE;

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
         * used to track the spans over threshold/zombies.
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
        public Builder kvThreshold(final long kvThreshold) {
            this.kvThreshold = kvThreshold;
            return this;
        }

        /**
         * Allows to customize the n1qlThreshold.
         *
         * @param n1qlThreshold the threshold to set.
         * @return this builder for chaining.
         */
        public Builder n1qlThreshold(final long n1qlThreshold) {
            this.n1qlThreshold = n1qlThreshold;
            return this;
        }

        /**
         * Allows to customize the viewThreshold.
         *
         * @param viewThreshold the threshold to set.
         * @return this builder for chaining.
         */
        public Builder viewThreshold(final long viewThreshold) {
            this.viewThreshold = viewThreshold;
            return this;
        }

        /**
         * Allows to customize the ftsThreshold.
         *
         * @param ftsThreshold the threshold to set.
         * @return this builder for chaining.
         */
        public Builder ftsThreshold(final long ftsThreshold) {
            this.ftsThreshold = ftsThreshold;
            return this;
        }

        /**
         * Allows to customize the analyticsThreshold.
         *
         * @param analyticsThreshold the threshold to set.
         * @return this builder for chaining.
         */
        public Builder analyticsThreshold(final long analyticsThreshold) {
            this.analyticsThreshold = analyticsThreshold;
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

        private long lastThresholdLog;
        private boolean hasThresholdWritten;
        private final SortedSet<ThresholdLogSpan> kvThresholdSet;
        private final SortedSet<ThresholdLogSpan> n1qlThresholdSet;
        private final SortedSet<ThresholdLogSpan> viewThresholdSet;
        private final SortedSet<ThresholdLogSpan> ftsThresholdSet;
        private final SortedSet<ThresholdLogSpan> analyticsThresholdSet;

        private long lastZombieLog;
        private boolean hasZombieWritten;
        private final SortedSet<ThresholdLogSpan> kvZombieSet;
        private final SortedSet<ThresholdLogSpan> n1qlZombieSet;
        private final SortedSet<ThresholdLogSpan> viewZombieSet;
        private final SortedSet<ThresholdLogSpan> ftsZombieSet;
        private final SortedSet<ThresholdLogSpan> analyticsZombieSet;

        Worker() {
            kvThresholdSet = new TreeSet<ThresholdLogSpan>();
            n1qlThresholdSet = new TreeSet<ThresholdLogSpan>();
            viewThresholdSet = new TreeSet<ThresholdLogSpan>();
            ftsThresholdSet = new TreeSet<ThresholdLogSpan>();
            analyticsThresholdSet = new TreeSet<ThresholdLogSpan>();

            kvZombieSet = new TreeSet<ThresholdLogSpan>();
            n1qlZombieSet = new TreeSet<ThresholdLogSpan>();
            viewZombieSet = new TreeSet<ThresholdLogSpan>();
            ftsZombieSet = new TreeSet<ThresholdLogSpan>();
            analyticsZombieSet = new TreeSet<ThresholdLogSpan>();
        }

        @Override
        public void run() {
            Thread.currentThread().setName("cb-tracing-" + REPORTER_ID.incrementAndGet());
            while (running) {
                try {
                    handleOverThresholdQueue();
                    handlerZombieQueue();
                    Thread.sleep(workerSleepMs);
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
            if ((now - lastThresholdLog + logIntervalNanos) > 0) {
                prepareAndlogOverThreshold();
                lastThresholdLog = now;
            }

            while (true) {
                ThresholdLogSpan span = overThresholdQueue.poll();
                if (span == null) {
                    return;
                }
                String service = (String) span.tag("couchbase.service");
                if (SERVICE_KV.equals(service)) {
                    updateSet(kvThresholdSet, span);
                } else if (SERVICE_N1QL.equals(service)) {
                    updateSet(n1qlThresholdSet, span);
                } else if (SERVICE_VIEW.equals(service)) {
                    updateSet(viewThresholdSet, span);
                } else if (SERVICE_FTS.equals(service)) {
                    updateSet(ftsThresholdSet, span);
                } else if (SERVICE_ANALYTICS.equals(service)) {
                    updateSet(analyticsThresholdSet, span);
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
                output.add(convertThresholdSet(kvThresholdSet, SERVICE_KV));
                kvThresholdSet.clear();
            }
            if (!n1qlThresholdSet.isEmpty()) {
                output.add(convertThresholdSet(n1qlThresholdSet, SERVICE_N1QL));
                n1qlThresholdSet.clear();
            }
            if (!viewThresholdSet.isEmpty()) {
                output.add(convertThresholdSet(viewThresholdSet, SERVICE_VIEW));
                viewThresholdSet.clear();
            }
            if (!ftsThresholdSet.isEmpty()) {
                output.add(convertThresholdSet(ftsThresholdSet, SERVICE_FTS));
                ftsThresholdSet.clear();
            }
            if (!analyticsThresholdSet.isEmpty()) {
                output.add(convertThresholdSet(analyticsThresholdSet, SERVICE_ANALYTICS));
                analyticsThresholdSet.clear();
            }
            logOverThreshold(output);
        }

        private Map<String, Object> convertThresholdSet(SortedSet<ThresholdLogSpan> set, String ident) {
            Map<String, Object> output = new HashMap<String, Object>();
            List<Map<String, Object>> top = new ArrayList<Map<String, Object>>();
            for (ThresholdLogSpan span : set) {
                Map<String, Object> entry = new HashMap<String, Object>();
                // TODO: upcoming commits will flesh this out
                entry.put("total_duration_us", span.durationMicros());
                top.add(entry);
            }
            output.put("service", ident);
            output.put("count", set.size());
            output.put("top", top);
            return output;
        }

        /**
         * Helper method which drains the queue, handles the sets and logs if needed.
         */
        private void handlerZombieQueue() {
            long now = System.nanoTime();
            if ((now - lastZombieLog + logIntervalNanos) > 0) {
                prepareAndLogZombies();
                lastZombieLog = now;
            }

            while (true) {
                ThresholdLogSpan span = zombieQueue.poll();
                if (span == null) {
                    return;
                }
                String service = (String) span.tag("couchbase.service");
                if (SERVICE_KV.equals(service)) {
                    updateSet(kvZombieSet, span);
                } else if (SERVICE_N1QL.equals(service)) {
                    updateSet(n1qlZombieSet, span);
                } else if (SERVICE_VIEW.equals(service)) {
                    updateSet(viewZombieSet, span);
                } else if (SERVICE_FTS.equals(service)) {
                    updateSet(ftsZombieSet, span);
                } else if (SERVICE_ANALYTICS.equals(service)) {
                    updateSet(analyticsZombieSet, span);
                } else {
                    LOGGER.warn("Unknown service in span {}", service);
                }
            }
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
            hasZombieWritten = true;
        }

        /**
         * Logs the zombie data and resets the sets.
         */
        private void prepareAndLogZombies() {
            if (!hasZombieWritten) {
                return;
            }
            hasZombieWritten = false;

            List<Map<String, Object>> output = new ArrayList<Map<String, Object>>();

            if (!kvZombieSet.isEmpty()) {
                output.add(convertZombieSet(kvZombieSet, SERVICE_KV));
                kvZombieSet.clear();
            }
            if (!n1qlZombieSet.isEmpty()) {
                output.add(convertZombieSet(n1qlZombieSet, SERVICE_N1QL));
                n1qlZombieSet.clear();
            }
            if (!viewZombieSet.isEmpty()) {
                output.add(convertZombieSet(viewZombieSet, SERVICE_VIEW));
                viewZombieSet.clear();
            }
            if (!ftsZombieSet.isEmpty()) {
                output.add(convertZombieSet(ftsZombieSet, SERVICE_FTS));
                ftsZombieSet.clear();
            }
            if (!analyticsZombieSet.isEmpty()) {
                output.add(convertZombieSet(analyticsZombieSet, SERVICE_ANALYTICS));
                analyticsZombieSet.clear();
            }

            logZombies(output);
        }
    }

    /**
     * Helper method to conver the span set into the log output format.
     */
    private Map<String, Object> convertZombieSet(SortedSet<ThresholdLogSpan> set, String ident) {
        Map<String, Object> output = new HashMap<String, Object>();
        List<Map<String, Object>> top = new ArrayList<Map<String, Object>>();
        for (ThresholdLogSpan span : set) {
            Map<String, Object> entry = new HashMap<String, Object>();
            // TODO: upcoming commits will flesh this out
            entry.put("total_duration_us", span.durationMicros());
            top.add(entry);
        }
        output.put("service", ident);
        output.put("count", set.size());
        output.put("top", top);
        return output;
    }


    /**
     * This method is intended to be overridden in test implementations
     * to assert against the output.
     */
    void logOverThreshold(final List<Map<String, Object>> toLog) {
        try {
            LOGGER.warn("Operations over threshold: {}", JACKSON.writeValueAsString(toLog));
        } catch (Exception ex) {
            LOGGER.warn("Could not write threshold log.", ex);
        }
    }

    /**
     * This method is intended to be overridden in test implementations
     * to assert against the output.
     */
    void logZombies(final List<Map<String, Object>> toLog) {
        try {
            LOGGER.warn("Zombie responses observed: {}", JACKSON.writeValueAsString(toLog));
        } catch (Exception ex) {
            LOGGER.warn("Could not write zombie log.", ex);
        }
    }

}
