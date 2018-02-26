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

import com.couchbase.client.core.message.CouchbaseRequest;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Verifies the functionality of the {@link ThresholdLogReporter}.
 *
 * @author Michael Nitschinger
 * @since 1.6.0
 */
public class ThresholdLogReporterTest {

    @Test
    @SuppressWarnings({"unchecked"})
    public void shouldLogKvOverThreshold() {
        TestReporter reporter = null;
        try {
            reporter = new TestReporter(new ThresholdLogReporter.Builder()
                .logInterval(1, TimeUnit.MILLISECONDS)
            );

            CouchbaseRequest request = mock(CouchbaseRequest.class);
            when(request.operationId()).thenReturn("0x1234");

            ThresholdLogSpan span = mock(ThresholdLogSpan.class);
            when(span.getBaggageItem("couchbase.zombie")).thenReturn("false");
            when(span.tag("peer.service")).thenReturn("kv");
            when(span.operationName()).thenReturn("get");
            when(span.durationMicros()).thenReturn(TimeUnit.SECONDS.toMicros(1));
            when(span.request()).thenReturn(request);

            reporter.report(span);
            reporter.waitUntilOverThreshold(1);

            List<Map<String, Object>> services = reporter.overThreshold().get(0);
            assertEquals(1, services.size());
            Map<String, Object> kvService = services.get(0);
            assertEquals(1, kvService.get("count"));
            assertEquals("kv", kvService.get("service"));

            List<Map<String, Object>> top = (List<Map<String, Object>>) kvService.get("top");
            assertEquals(1000000L, top.get(0).get("total_us"));
            assertEquals("get:0x1234", top.get(0).get("operation_id"));
        } finally {
            if (reporter != null) {
                reporter.shutdown();
            }
        }
    }

    static class TestReporter extends ThresholdLogReporter {

        private final List<List<Map<String, Object>>> overThreshold = Collections.synchronizedList(new ArrayList<List<Map<String, Object>>>());

        private final List<List<Map<String, Object>>> zombies = Collections.synchronizedList(new ArrayList<List<Map<String, Object>>>());

        public TestReporter(final Builder builder) {
            super(builder);
        }

        @Override
        void logOverThreshold(List<Map<String, Object>> toLog) {
            overThreshold.add(toLog);
        }

        @Override
        void logZombies(List<Map<String, Object>> toLog) {
            zombies.add(toLog);
        }

        List<List<Map<String, Object>>> overThreshold() {
            return overThreshold;
        }

        List<List<Map<String, Object>>> zombies() {
            return zombies;
        }

        void waitUntilOverThreshold(int amount) {
            while (true) {
                if (overThreshold.size() >= amount) {
                    return;
                }
            }
        }

        void waitUntilZombies(int amount) {
            while (true) {
                if (zombies.size() >= amount) {
                    return;
                }
            }
        }


        @Override
        long minLogInterval() {
            return 1; // 1 nanosecond, effectively no min log interval
        }
    }

}