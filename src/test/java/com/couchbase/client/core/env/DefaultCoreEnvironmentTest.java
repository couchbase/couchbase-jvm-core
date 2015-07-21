/**
 * Copyright (C) 2014 Couchbase, Inc.
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
package com.couchbase.client.core.env;

import com.couchbase.client.core.logging.CouchbaseLogger;
import com.couchbase.client.core.logging.CouchbaseLoggerFactory;
import io.netty.channel.local.LocalEventLoopGroup;
import org.junit.Test;
import rx.functions.Actions;
import rx.schedulers.Schedulers;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.util.HashSet;
import java.util.Set;

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
        assertTrue(env.shutdown().toBlocking().single());
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
        assertTrue(env.shutdown().toBlocking().single());
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
        assertTrue(env.shutdown().toBlocking().single());

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
            Set<String> afterCreate = dump(threads(mx, ignore));

            env.shutdown().toBlocking().last();
            Set<String> afterShutdown = threads(mx, ignore);

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

    private Set<String> threads(ThreadMXBean mx, Set<String> ignore) {
        Set<String> all = threads(mx);
        all.removeAll(ignore);
        return all;
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
                .scheduler(Schedulers.trampoline()).build();
        String dump = env.dumpParameters(new StringBuilder()).toString();

        assertTrue(dump, dump.contains("LocalEventLoopGroup!unmanaged"));
        assertTrue(dump, dump.contains("TrampolineScheduler!unmanaged"));
    }

    @Test
    public void shouldShowOnlyClassNameForManagedResourcesInEnvDump() {
        //create an environment with a custom IOPool and Scheduler that are not cleaned up on shutdown
        DefaultCoreEnvironment env = DefaultCoreEnvironment.create();
        String dump = env.dumpParameters(new StringBuilder()).toString();

        assertFalse(dump, dump.contains("!unmanaged"));
    }

}
