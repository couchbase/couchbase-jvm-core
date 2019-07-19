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

import com.couchbase.client.core.env.resources.NoOpShutdownHook;
import com.couchbase.client.core.event.CouchbaseEvent;
import com.couchbase.client.core.event.system.TooManyEnvironmentsEvent;
import com.couchbase.client.core.logging.CouchbaseLogger;
import com.couchbase.client.core.logging.CouchbaseLoggerFactory;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.local.LocalEventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import org.junit.After;
import org.junit.Test;
import rx.functions.Action1;
import rx.functions.Actions;
import rx.schedulers.Schedulers;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.Collections.emptySet;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class DefaultCoreEnvironmentTest {

    private static final CouchbaseLogger LOGGER = CouchbaseLoggerFactory.getInstance(DefaultCoreEnvironmentTest.class);

    private DefaultCoreEnvironment env;

    @After
    public void after(){
        if (env != null) {
            env.shutdown();
            env = null;
        }
    }

    @Test
    public void shouldInitAndShutdownCoreEnvironment() throws Exception {
        env = DefaultCoreEnvironment.create();
        assertNotNull(env.ioPool());
        assertNotNull(env.scheduler());

        assertEquals(DefaultCoreEnvironment.KEYVALUE_ENDPOINTS, env.kvEndpoints());
        assertTrue(env.shutdown());
    }

    @Test
    public void shouldOverrideDefaults() throws Exception {
        env = DefaultCoreEnvironment
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
        try {
            env = DefaultCoreEnvironment
                .builder()
                .kvEndpoints(3)
                .build();
            assertNotNull(env.ioPool());
            assertNotNull(env.scheduler());

            assertEquals(10, env.kvEndpoints());
            assertTrue(env.shutdown());

        } finally {
            System.clearProperty("com.couchbase.kvEndpoints");
        }
    }

    @Test
    public void shouldApplyMinPoolSize() throws Exception {
        env = DefaultCoreEnvironment
                .builder()
                .ioPoolSize(1)
                .computationPoolSize(1)
                .build();

        assertEquals(DefaultCoreEnvironment.MIN_POOL_SIZE, env.ioPoolSize());
        assertEquals(DefaultCoreEnvironment.MIN_POOL_SIZE, env.computationPoolSize());
    }

    @Test
    public void shouldNotLeakThreadsWithDefaultConfiguration() throws InterruptedException {
        createAndShutdownDefaultEnvironment();
        final Set<String> baseline = getCouchbaseThreads();

        for (int i = 0; i < 3; i++) {
            createAndShutdownDefaultEnvironment();
        }

        // Give the threads time to finish
        for (int i = 0; i < 300 && !getCouchbaseThreadsExceptFor(baseline).isEmpty(); i++) {
            Thread.sleep(10);
        }
        assertEquals("Leaked threads", emptySet(), getCouchbaseThreadsExceptFor(baseline));
    }

    private static void createAndShutdownDefaultEnvironment() {
        CoreEnvironment env = DefaultCoreEnvironment.create();
        env.scheduler().createWorker().schedule(Actions.empty());
        env.scheduler().createWorker().schedule(Actions.empty());
        env.scheduler().createWorker().schedule(Actions.empty());
        env.scheduler().createWorker().schedule(Actions.empty());
        env.scheduler().createWorker().schedule(Actions.empty());
        env.scheduler().createWorker().schedule(Actions.empty());
        boolean shutdownResult = env.shutdown();
        assertTrue(shutdownResult);
    }

    private static Set<String> getCouchbaseThreadsExceptFor(Collection<String> remove) {
        Set<String> result = getCouchbaseThreads();
        result.removeAll(remove);
        return result;
    }

    private static Set<String> getCouchbaseThreads() {
        final ThreadMXBean threadManager = ManagementFactory.getThreadMXBean();
        final ThreadInfo[] threads = threadManager.getThreadInfo(threadManager.getAllThreadIds());

        final Set<String> result = new HashSet<>();
        for (ThreadInfo t : threads) {
            if (t == null || t.getThreadName() == null || t.getThreadState() == Thread.State.TERMINATED) {
                continue; // thread already dead, or anonymous (not one of ours)
            }
            String name = t.getThreadName();
            if (name.startsWith("cb-") || name.contains("Rx")) {
                result.add(t.getThreadId() + ":" +t.getThreadName());
            }
        }
        return result;
    }

    @Test
    public void shouldShowUnmanagedCustomResourcesInEnvDump() {
        //create an environment with a custom IOPool and Scheduler that are not cleaned up on shutdown
        env = DefaultCoreEnvironment.builder()
                .ioPool(new LocalEventLoopGroup())
                .scheduler(Schedulers.newThread()).build();
        String dump = env.dumpParameters(new StringBuilder()).toString();

        assertTrue(dump, dump.contains("LocalEventLoopGroup!unmanaged"));
        assertTrue(dump, dump.contains("NewThreadScheduler!unmanaged"));
    }

    @Test
    public void shouldShowOnlyClassNameForManagedResourcesInEnvDump() {
        //create an environment with a custom IOPool and Scheduler that are not cleaned up on shutdown
        env = DefaultCoreEnvironment.create();
        String dump = env.dumpParameters(new StringBuilder()).toString();

        assertFalse(dump, dump.contains("!unmanaged"));
    }

    /**
     * Note that since this test doesn't run in isolation and other tests heavily alter the number
     * of currently outstanding environments (which is okay), the only thing we can assert is that the
     * message got emitted and that there are more than one environments found.
     */
    @Test
    public void shouldEmitEvent() throws Exception {
        CoreEnvironment env1 = DefaultCoreEnvironment.create();

        final AtomicInteger evtCount = new AtomicInteger(0);
        final CountDownLatch latch = new CountDownLatch(1);

        env1.eventBus().get().forEach(new Action1<CouchbaseEvent>() {
            @Override
            public void call(CouchbaseEvent couchbaseEvent) {
                if (couchbaseEvent instanceof TooManyEnvironmentsEvent) {
                    evtCount.set(((TooManyEnvironmentsEvent) couchbaseEvent).numEnvs());
                    latch.countDown();
                }
            }
        });
        CoreEnvironment env2 = DefaultCoreEnvironment.builder().eventBus(env1.eventBus()).build();
        latch.await(5, TimeUnit.SECONDS);

        env1.shutdown();
        env2.shutdown();

        assertTrue(evtCount.get() > 1);
    }

    @Test
    public void shouldNullServicePoolsIfNotOverridden() {
        CoreEnvironment env = DefaultCoreEnvironment.create();
        assertNull(env.kvIoPool());
        assertNull(env.viewIoPool());
        assertNull(env.searchIoPool());
        assertNull(env.queryIoPool());
    }

    @Test
    public void shouldOverrideAndShutdownServicePools() {
        EventLoopGroup elg = new NioEventLoopGroup();
        CoreEnvironment env1 = DefaultCoreEnvironment.builder()
            .kvIoPool(elg, new NoOpShutdownHook())
            .viewIoPool(elg, new NoOpShutdownHook())
            .searchIoPool(elg, new NoOpShutdownHook())
            .queryIoPool(elg, new NoOpShutdownHook())
            .build();

        env1.shutdown();
        assertFalse(elg.isShutdown());
        elg.shutdownGracefully().awaitUninterruptibly(3000);
        assertTrue(elg.isShutdown());
    }

    @Test(expected = IllegalStateException.class)
    public void shouldFailIfClientCertEnabledButSSLIsnt() {
        DefaultCoreEnvironment.builder().certAuthEnabled(true).build();
    }

    /**
     * Make sure JVMCBC-495 functionality (generic subclassing) works.
     */
    @Test
    public void shouldSupportChildBuilder() {
        DummyEnvironment env = new ChildBuilder()
            .continuousKeepAliveEnabled(true)
            .used()
            .socketConnectTimeout(1234)
            .build();
        assertNotNull(env);
    }

    static class ChildBuilder extends DefaultCoreEnvironment.Builder<ChildBuilder> {

        private boolean used = false;

        public ChildBuilder used() {
            used = true;
            return this;
        }

        public DummyEnvironment build() {
            return new DummyEnvironment(this);
        }
    }

    static class DummyEnvironment extends DefaultCoreEnvironment {
        public DummyEnvironment(Builder builder) {
            super(builder);
        }
    }

}
