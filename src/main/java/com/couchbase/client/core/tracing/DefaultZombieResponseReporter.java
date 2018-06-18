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
import com.couchbase.client.core.message.CouchbaseRequest;
import com.couchbase.client.core.message.CouchbaseResponse;
import com.couchbase.client.core.message.analytics.AnalyticsRequest;
import com.couchbase.client.core.message.config.ConfigRequest;
import com.couchbase.client.core.message.kv.BinaryRequest;
import com.couchbase.client.core.message.kv.BinaryResponse;
import com.couchbase.client.core.message.query.QueryRequest;
import com.couchbase.client.core.message.search.SearchRequest;
import com.couchbase.client.core.message.view.ViewRequest;
import io.netty.util.internal.shaded.org.jctools.queues.MpscUnboundedArrayQueue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static com.couchbase.client.core.tracing.ThresholdLogReporter.SERVICE_ANALYTICS;
import static com.couchbase.client.core.tracing.ThresholdLogReporter.SERVICE_FTS;
import static com.couchbase.client.core.tracing.ThresholdLogReporter.SERVICE_KV;
import static com.couchbase.client.core.tracing.ThresholdLogReporter.SERVICE_N1QL;
import static com.couchbase.client.core.tracing.ThresholdLogReporter.SERVICE_VIEW;
import static com.couchbase.client.core.utils.DefaultObjectMapper.prettyWriter;
import static com.couchbase.client.core.utils.DefaultObjectMapper.writer;

/**
 * The {@link DefaultZombieResponseReporter} receives requests's of zombie responses
 * and stores them for aggregation and logging.
 *
 * @author Mike Goldsmith
 * @since 1.6.0
 */
public class DefaultZombieResponseReporter implements ZombieResponseReporter {

    private static final CouchbaseLogger LOGGER =
            CouchbaseLoggerFactory.getInstance(DefaultZombieResponseReporter.class);

    private static final AtomicInteger REPORTER_ID = new AtomicInteger();

    private static final long MIN_LOG_INTERVAL = TimeUnit.SECONDS.toNanos(1);

    private final Queue<CouchbaseResponse> queue;

    private final long logIntervalNanos;
    private final int sampleSize;
    private final boolean pretty;

    private final Thread worker;

    private volatile boolean running;

    public static DefaultZombieResponseReporter.Builder builder() {
        return new DefaultZombieResponseReporter.Builder();
    }

    public static DefaultZombieResponseReporter disabled() {
        return builder().logInterval(0, TimeUnit.SECONDS).build();
    }

    public static DefaultZombieResponseReporter create() {
        return builder().build();
    }

    /**
     * Creates the {@link ZombieResponseReporter} using the given config.
     */
    public DefaultZombieResponseReporter(final Builder builder) {
        logIntervalNanos = builder.logIntervalUnit.toNanos(builder.logInterval);
        sampleSize = builder.sampleSize;
        if (logIntervalNanos > 0 && logIntervalNanos < minLogInterval()) {
            throw new IllegalArgumentException("The log interval needs to be either 0 or greater than "
                    + MIN_LOG_INTERVAL + " micros");
        }

        queue = new MpscUnboundedArrayQueue<CouchbaseResponse>(builder.spanQueueSize);
        pretty = builder.pretty;
        running = true;

        if (logIntervalNanos > 0) {
            worker = new Thread(new Worker());
            worker.setDaemon(true);
            worker.start();
        } else {
            worker = null;
            LOGGER.debug("ZombieResponseLogReporter disabled via config.");
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
     * Reports the given {@link CouchbaseRequest} as a zombie response.
     *
     * @param request the request that shpuld be reported.
     */
    @Override
    public void report(final CouchbaseResponse request) {
        if (!queue.offer(request)) {
            LOGGER.debug("Could not enqueue CouchbaseRequest {} for zombie reporting, discarding.", request);
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
     * The builder to configure the {@link DefaultZombieResponseReporter}
     */
    public static class Builder {

        private static final long DEFAULT_LOG_INTERVAL = 10;
        private static final TimeUnit DEFAULT_LOG_INTERVAL_UNIT = TimeUnit.SECONDS;
        private static final int DEFAULT_SPAN_QUEUE_SIZE = 1024;
        private static final int DEFAULT_SAMPLE_SIZE = 10;
        private static final boolean DEFAULT_PRETTY = false;

        private long logInterval = DEFAULT_LOG_INTERVAL;
        private TimeUnit logIntervalUnit = DEFAULT_LOG_INTERVAL_UNIT;
        private int spanQueueSize = DEFAULT_SPAN_QUEUE_SIZE;
        private int sampleSize = DEFAULT_SAMPLE_SIZE;
        private boolean pretty = DEFAULT_PRETTY;

        public DefaultZombieResponseReporter build() {
            return new DefaultZombieResponseReporter(this);
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
            System.getProperty("com.couchbase.zombieResponseReporterSleep", "100")
        );

        private final SortedSet<CouchbaseResponse> kvZombieSet = new TreeSet<CouchbaseResponse>();
        private final SortedSet<CouchbaseResponse> n1qlZombieSet = new TreeSet<CouchbaseResponse>();
        private final SortedSet<CouchbaseResponse> viewZombieSet = new TreeSet<CouchbaseResponse>();
        private final SortedSet<CouchbaseResponse> ftsZombieSet = new TreeSet<CouchbaseResponse>();
        private final SortedSet<CouchbaseResponse> analyticsZombieSet = new TreeSet<CouchbaseResponse>();

        private int kvZombieCount = 0;
        private int n1qlZombieCount = 0;
        private int viewZombieCount = 0;
        private int ftsZombieCount = 0;
        private int analyticsZombieCount = 0;

        private long lastZombieLog;
        private boolean hasZombieWritten;

        @Override
        public void run() {
            Thread.currentThread().setName("cb-zombie-" + REPORTER_ID.incrementAndGet());
            while (running) {
                try {
                    handlerZombieQueue();
                    Thread.sleep(workerSleepMs);
                } catch (final InterruptedException ex) {
                    if (!running) {
                        return;
                    } else {
                        Thread.currentThread().interrupt();
                    }
                } catch (final Exception ex) {
                    LOGGER.warn("Got exception on zombie response reporter, ignoring.", ex);
                }
            }
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
                CouchbaseResponse response = queue.poll();
                if (response == null) {
                    return;
                }

                CouchbaseRequest request = response.request();
                if (request instanceof BinaryRequest) {
                    updateSet(kvZombieSet, response);
                    kvZombieCount += 1;
                } else if (request instanceof QueryRequest) {
                    updateSet(n1qlZombieSet, response);
                    n1qlZombieCount += 1;
                } else if (request instanceof ViewRequest) {
                    updateSet(viewZombieSet, response);
                    viewZombieCount += 1;
                } else if (request instanceof AnalyticsRequest) {
                    updateSet(analyticsZombieSet, response);
                    analyticsZombieCount += 1;
                } else if (request instanceof SearchRequest) {
                    updateSet(ftsZombieSet, response);
                    ftsZombieCount += 1;
                } else {
                    LOGGER.warn("Unknown service in zombie {}", request);
                }
            }
        }

        /**
         * Helper method which updates the set with the span and ensures that the sample
         * size is respected.
         *
         * @param set the set to work with.
         * @param response the response to store.
         */
        private void updateSet(final SortedSet<CouchbaseResponse> set, final CouchbaseResponse response) {
            set.add(response);
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
                output.add(convertThresholdSet(kvZombieSet, kvZombieCount, SERVICE_KV));
                kvZombieSet.clear();
                kvZombieCount = 0;
            }
            if (!n1qlZombieSet.isEmpty()) {
                output.add(convertThresholdSet(n1qlZombieSet, n1qlZombieCount, SERVICE_N1QL));
                n1qlZombieSet.clear();
                n1qlZombieCount = 0;
            }
            if (!viewZombieSet.isEmpty()) {
                output.add(convertThresholdSet(viewZombieSet, viewZombieCount, SERVICE_VIEW));
                viewZombieSet.clear();
                viewZombieCount = 0;
            }
            if (!ftsZombieSet.isEmpty()) {
                output.add(convertThresholdSet(ftsZombieSet, ftsZombieCount, SERVICE_FTS));
                ftsZombieSet.clear();
                ftsZombieCount = 0;
            }
            if (!analyticsZombieSet.isEmpty()) {
                output.add(convertThresholdSet(analyticsZombieSet, analyticsZombieCount, SERVICE_ANALYTICS));
                analyticsZombieSet.clear();
                analyticsZombieCount = 0;
            }

            logZombies(output);
        }

        private Map<String, Object> convertThresholdSet(SortedSet<CouchbaseResponse> set, int count, String serviceType) {
            Map<String, Object> output = new HashMap<String, Object>();
            List<Map<String, Object>> top = new ArrayList<Map<String, Object>>();
            for (CouchbaseResponse response : set) {
                HashMap<String, Object> fieldMap = new HashMap<String, Object>();

                CouchbaseRequest request = response.request();
                if (request != null) {
                    fieldMap.put("s", formatServiceType(request));
                    putIfNotNull(fieldMap, "i", request.operationId());
                    putIfNotNull(fieldMap, "b", request.bucket());
                    putIfNotNull(fieldMap, "c", request.lastLocalId());
                    putIfNotNull(fieldMap, "l", request.lastLocalSocket());
                    putIfNotNull(fieldMap, "r", request.lastRemoteSocket());
                }

                if (response instanceof BinaryResponse) {
                    putIfNotNull(fieldMap, "d", ((BinaryResponse) response).serverDuration());
                }

                top.add(fieldMap);
            }
            output.put("service", serviceType);
            output.put("count", count);
            output.put("top", top);
            return output;
        }


        private void putIfNotNull(final Map<String, Object> map, final String key, final Object value) {
            if (value != null) {
                map.put(key, value);
            }
        }

        /**
         * Helper method to turn the request into the proper string service type.
         */
        private String formatServiceType(final CouchbaseRequest request) {
            if (request instanceof BinaryRequest) {
                return ThresholdLogReporter.SERVICE_KV;
            } else if (request instanceof QueryRequest) {
                return ThresholdLogReporter.SERVICE_N1QL;
            } else if (request instanceof ViewRequest) {
                return ThresholdLogReporter.SERVICE_VIEW;
            } else if (request instanceof AnalyticsRequest) {
                return ThresholdLogReporter.SERVICE_ANALYTICS;
            } else if (request instanceof SearchRequest) {
                return ThresholdLogReporter.SERVICE_FTS;
            } else if (request instanceof ConfigRequest) {
                // Shouldn't be user visible, but just for completeness sake.
                return "config";
            } else {
                return "unknown";
            }
        }
    }

    /**
     * This method is intended to be overridden in test implementations
     * to assert against the output.
     */
    void logZombies(final List<Map<String, Object>> toLog) {
        try {
            String result = pretty
                    ? prettyWriter().writeValueAsString(toLog)
                    : writer().writeValueAsString(toLog);
            LOGGER.warn("Zombie responses observed: {}", result);
        } catch (Exception ex) {
            LOGGER.warn("Could not write zombie log.", ex);
        }
    }
}
