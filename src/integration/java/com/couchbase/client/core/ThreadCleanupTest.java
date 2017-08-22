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
package com.couchbase.client.core;

import com.couchbase.client.core.env.CoreEnvironment;
import com.couchbase.client.core.env.DefaultCoreEnvironment;
import com.couchbase.client.core.logging.CouchbaseLogger;
import com.couchbase.client.core.logging.CouchbaseLoggerFactory;
import com.couchbase.client.core.message.cluster.DisconnectRequest;
import com.couchbase.client.core.message.cluster.OpenBucketRequest;
import com.couchbase.client.core.message.cluster.OpenBucketResponse;
import com.couchbase.client.core.message.cluster.SeedNodesRequest;
import com.couchbase.client.core.message.cluster.SeedNodesResponse;
import com.couchbase.client.core.util.ClusterDependentTest;
import com.couchbase.client.core.util.TestProperties;
import org.junit.Test;
import rx.Observable;
import rx.functions.Func1;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import static org.junit.Assert.assertTrue;

/**
 * A test case that validates shutdown behavior of the SDK under "real use" conditions.
 * Aim is to detect potential problems with threading cleanup in containers like Tomcat.
 *
 * This test case doesn't extend ClusterDependentTest so that the connection and shutdown of the cluster
 * can be triggered at the right time for the tests.
 *
 * @author Simon Baslé
 * @since 1.2
 */
public class ThreadCleanupTest {

    private static final CouchbaseLogger LOGGER = CouchbaseLoggerFactory.getInstance(ThreadCleanupTest.class);

    private static final String seedNode = TestProperties.seedNode();
    private static final String bucket = TestProperties.bucket();
    private static String username = TestProperties.username();
    private static String password = TestProperties.password();

    private CoreEnvironment env;

    private ClusterFacade cluster;

    public void connect() throws Exception {
        if (ClusterDependentTest.minClusterVersion()[0] >= 5) {
            username = TestProperties.adminUser();
            password = TestProperties.adminPassword();
        }

        env = DefaultCoreEnvironment
                .builder()
                .mutationTokensEnabled(true)
                .build();
        cluster = new CouchbaseCore(env);
        cluster.<SeedNodesResponse>send(new SeedNodesRequest(seedNode)).flatMap(
                new Func1<SeedNodesResponse, Observable<OpenBucketResponse>>() {
                    @Override
                    public Observable<OpenBucketResponse> call(SeedNodesResponse response) {
                        return cluster.send(new OpenBucketRequest(bucket, username, password));
                    }
                }
        ).toBlocking().single();
    }

    public void disconnect() throws InterruptedException {
        cluster.send(new DisconnectRequest()).toBlocking().first();
    }

    @Test
    public void testSdkNettyRxJavaThreadsShutdownProperly() throws Exception {
        //FIXME differently improve the tolerance of this test to threads spawned by others
        Thread.sleep(500);
        ThreadMXBean mx = ManagementFactory.getThreadMXBean();
        LOGGER.info("Threads at start");
        Set<String> ignore = dump(threads(mx));

        connect();

        LOGGER.info("Threads before shutdown:");
        Set<String> beforeShutdown = dump(threads(mx, ignore, false));

        LOGGER.info("");
        LOGGER.info("Shutting Down Couchbase Cluster");
        disconnect();
        LOGGER.info("Shutting Down Couchbase Env");
        boolean hasShutdown = env.shutdownAsync().toBlocking().single();
        //TODO also test RxJava Schedulers.shutdown here once we depend on 1.0.15+
        LOGGER.info("Shutting Down RxJava should be implemented here");
        LOGGER.info("");
        LOGGER.info("Threads after shutdown:");
        Set<String> afterShutdown = dump(threads(mx, ignore, true));
        boolean hasCleanedUpThreads = Collections.disjoint(afterShutdown, beforeShutdown);
        if (hasCleanedUpThreads)
            LOGGER.info("All relevant threads properly cleaned up after shutdown");

        //TODO when shutdown RxJava is implemented, RxJava should also be restarted here
        //TODO move this test case to the rx.schedulers package to do that
        assertTrue("Some threads created by the SDK remained after shutdown", hasCleanedUpThreads);
        assertTrue("env.shutdown() returned false", hasShutdown);
    }

    private Set<String> dump(Set<String> threads) {
        for (String thread : threads) {
            LOGGER.info(thread);
        }
        return threads;
    }

    private Set<String> threads(ThreadMXBean mx, Set<String> ignore, boolean shouldIgnoreRxJavaThreads) {
        Set<String> all = threads(mx);
        all.removeAll(ignore);
        if (shouldIgnoreRxJavaThreads) {
            for (Iterator<String> iterator = all.iterator(); iterator.hasNext(); ) {
                String next = iterator.next();
                if (next.startsWith("Rx"))
                    iterator.remove();
            }
        }
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
}
