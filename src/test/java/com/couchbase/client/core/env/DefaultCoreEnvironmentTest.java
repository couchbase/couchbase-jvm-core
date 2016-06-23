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
package com.couchbase.client.core.env;

import com.couchbase.client.core.event.CouchbaseEvent;
import com.couchbase.client.core.event.system.TooManyEnvironmentsEvent;
import com.couchbase.client.core.logging.CouchbaseLogger;
import com.couchbase.client.core.logging.CouchbaseLoggerFactory;
import io.netty.channel.local.LocalEventLoopGroup;
import org.junit.Test;
import rx.functions.Action1;
import rx.functions.Actions;
import rx.schedulers.Schedulers;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class DefaultCoreEnvironmentTest {

    private static final CouchbaseLogger LOGGER = CouchbaseLoggerFactory.getInstance(DefaultCoreEnvironmentTest.class);

    @Test
    public void shouldInitAndShutdownCoreEnvironment() throws Exception {
        CoreEnvironment env = DefaultCoreEnvironment.create();
        assertNotNull(env.ioPool());
        assertNotNull(env.scheduler());

        assertEquals(DefaultCoreEnvironment.KEYVALUE_ENDPOINTS, env.kvEndpoints());
        assertTrue(env.shutdown());
    }

    @Test
    public void shouldOverrideDefaults() throws Exception {
        CoreEnvironment env = DefaultCoreEnvironment
            .builder()
            .kvEndpoints(3)
            .build();
        assertNotNull(env.ioPool());
        assertNotNull(env.scheduler());

        assertEquals(3, env.kvEndpoints());
        assertTrue(env.shutdown());
    }

    @Test
    public void sysPropertyShouldTakePrecedence() throws Exception {

        System.setProperty("com.couchbase.kvEndpoints", "10");

        CoreEnvironment env = DefaultCoreEnvironment
            .builder()
            .kvEndpoints(3)
            .build();
        assertNotNull(env.ioPool());
        assertNotNull(env.scheduler());

        assertEquals(10, env.kvEndpoints());
        assertTrue(env.shutdown());

        System.clearProperty("com.couchbase.kvEndpoints");
    }

    @Test
    public void shouldApplyMinPoolSize() throws Exception {
        CoreEnvironment env = DefaultCoreEnvironment
                .builder()
                .ioPoolSize(1)
                .computationPoolSize(1)
                .build();

        assertEquals(DefaultCoreEnvironment.MIN_POOL_SIZE, env.ioPoolSize());
        assertEquals(DefaultCoreEnvironment.MIN_POOL_SIZE, env.computationPoolSize());
    }

    @Test
    public void shouldNotLeakThreadsWithDefaultConfiguration() throws InterruptedException {
        int loops = 3;
        ThreadMXBean mx = ManagementFactory.getThreadMXBean();
        LOGGER.info("Initial Threads (will be ignored later):");
        Set<String> ignore = dump(threads(mx));
        int[] peaks = new int[loops];

        for (int i = 0; i < loops; i++) {
            CoreEnvironment env = DefaultCoreEnvironment.create();
            env.scheduler().createWorker().schedule(Actions.empty());
            env.scheduler().createWorker().schedule(Actions.empty());
            env.scheduler().createWorker().schedule(Actions.empty());
            env.scheduler().createWorker().schedule(Actions.empty());
            env.scheduler().createWorker().schedule(Actions.empty());
            env.scheduler().createWorker().schedule(Actions.empty());

            LOGGER.info("===Created threads:");
            Set<String> afterCreate = dump(threads(mx, ignore, false));

            LOGGER.info("Shutdown result: " + env.shutdown());
            //we only consider threads starting with cb- or containing Rx, minus the ones existing at startup
            Set<String> afterShutdown = threads(mx, ignore, true);

            peaks[i] = afterShutdown.size();
            LOGGER.info("===Shutdown went from " + afterCreate.size() + " to " + afterShutdown.size() + " threads, remaining: ");
            dump(afterShutdown);
        }
        boolean peakGrowing = false;
        StringBuilder peaksDump = new StringBuilder("========Thread peaks : ").append(peaks[0]);
        for (int i = 1; i < loops; i++) {
            peaksDump.append(' ').append(peaks[i]);
            peakGrowing = peakGrowing || (peaks[i] != peaks[i - 1]);
        }
        LOGGER.info(peaksDump.toString());
        assertFalse("Number of threads is growing despite shutdown, see console output", peakGrowing);
    }

    private Set<String> dump(Set<String> threads) {
        for (String thread : threads) {
            LOGGER.info(thread);
        }
        return threads;
    }

    private Set<String> threads(ThreadMXBean mx, Set<String> ignore, boolean ignoreNonCbRx) {
        Set<String> all = threads(mx);
        all.removeAll(ignore);
        if (!ignoreNonCbRx) {
            return all;
        } else {
            Set<String> result = new HashSet<String>(all.size());
            for (String s : all) {
                if (s.startsWith("cb-") || s.contains("Rx")) {
                    result.add(s);
                }
            }
            return result;
        }
    }

    private Set<String> threads(ThreadMXBean mx) {
        ThreadInfo[] dump = mx.getThreadInfo(mx.getAllThreadIds());
        Set<String> names = new HashSet<String>(dump.length);
        for (ThreadInfo threadInfo : dump) {
            if (threadInfo == null || threadInfo.getThreadName() == null) {
                continue;
            }

            names.add(threadInfo.getThreadName());
        }
        return names;
    }

    @Test
    public void shouldShowUnmanagedCustomResourcesInEnvDump() {
        //create an environment with a custom IOPool and Scheduler that are not cleaned up on shutdown
        DefaultCoreEnvironment env = DefaultCoreEnvironment.builder()
                .ioPool(new LocalEventLoopGroup())
                .scheduler(Schedulers.newThread()).build();
        String dump = env.dumpParameters(new StringBuilder()).toString();

        assertTrue(dump, dump.contains("LocalEventLoopGroup!unmanaged"));
        assertTrue(dump, dump.contains("NewThreadScheduler!unmanaged"));
    }

    @Test
    public void shouldShowOnlyClassNameForManagedResourcesInEnvDump() {
        //create an environment with a custom IOPool and Scheduler that are not cleaned up on shutdown
        DefaultCoreEnvironment env = DefaultCoreEnvironment.create();
        String dump = env.dumpParameters(new StringBuilder()).toString();

        assertFalse(dump, dump.contains("!unmanaged"));
    }

    /**
     * Note that since this test doesn't run in isolation and other tests heavily alter the number
     * of currently outstanding environments (which is okay), the only thing we can assert is that the
     * message got emitted and that there are more than one environments found.
     */
    @Test
    public void shouldEmitEvent() {
        CoreEnvironment env = DefaultCoreEnvironment.create();

        final AtomicInteger evtCount = new AtomicInteger(0);
        env.eventBus().get().forEach(new Action1<CouchbaseEvent>() {
            @Override
            public void call(CouchbaseEvent couchbaseEvent) {
                if (couchbaseEvent instanceof TooManyEnvironmentsEvent) {
                    evtCount.set(((TooManyEnvironmentsEvent) couchbaseEvent).numEnvs());
                }
            }
        });

        CoreEnvironment env2 = DefaultCoreEnvironment.builder().eventBus(env.eventBus()).build();

        env.shutdown();
        env2.shutdown();

        assertTrue(evtCount.get() > 1);
    }

}
