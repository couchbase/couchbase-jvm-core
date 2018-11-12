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

package com.couchbase.client.core;

import com.couchbase.client.core.env.CoreEnvironment;
import com.couchbase.client.core.env.DefaultCoreEnvironment;
import com.couchbase.client.core.message.CouchbaseResponse;
import com.couchbase.client.core.message.cluster.CloseBucketRequest;
import com.couchbase.client.core.message.cluster.OpenBucketRequest;
import com.couchbase.client.core.message.cluster.SeedNodesRequest;
import com.couchbase.client.core.message.internal.DiagnosticsRequest;
import com.couchbase.client.core.message.internal.DiagnosticsResponse;
import com.couchbase.client.core.tracing.RingBufferMonitor;
import com.couchbase.client.core.util.ClusterDependentTest;
import com.couchbase.client.core.util.TestProperties;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Arrays;

import static org.junit.Assert.assertEquals;
import static org.junit.Assume.assumeFalse;

/**
 * Checks the RingBufferMonitor functionality
 *
 * @author Graham Pople
 * @since 2.6.0
 */
public class RingBufferMonitorTest {

    private static volatile CoreEnvironment ENV;

    @BeforeClass
    public static void setup () {
        DefaultCoreEnvironment.Builder builder = DefaultCoreEnvironment.builder();
        ClusterDependentTest.configurPortsIfMocked(builder);
        ENV = builder.build();
    }

    @AfterClass
    public static final void cleanup() {
        ENV.shutdown();
    }

    @Test
    public void afterResponseRingBufferMonitorShouldBeEmpty() throws Exception {
        assumeFalse(TestProperties.isCi());

        CouchbaseCore core = new CouchbaseCore(ENV);
        // Some other unit tests bypass the ringbuffer, while will lead to negative counts - hence reset.
        RingBufferMonitor.instance().reset();

        core.send(new SeedNodesRequest(Arrays.asList(TestProperties.seedNode())));
        OpenBucketRequest request;
        if (ClusterDependentTest.minClusterVersion()[0] >= 5) {
            request = new OpenBucketRequest(TestProperties.bucket(), TestProperties.adminUser(), TestProperties.adminPassword());
        } else {
            request = new OpenBucketRequest(TestProperties.bucket(), TestProperties.username(), TestProperties.password());
        }
        core.send(request).toBlocking().single();
        BackpressureException exception = RingBufferMonitor.instance().createException();
        assertEquals(0, exception.diagnostics().totalCount());
        core.send(new CloseBucketRequest(TestProperties.bucket())).toBlocking().single();
    }

    @Test
    public void diagnosticsReportContainsRingBufferDiagnostics() {
        assumeFalse(TestProperties.isCi());

        CouchbaseCore core = new CouchbaseCore(ENV);
        RingBufferMonitor.instance().reset();

        CouchbaseResponse response = core.send(new DiagnosticsRequest("test")).toBlocking().single();
        DiagnosticsResponse res = (DiagnosticsResponse) response;

        assertEquals(0, res.diagnosticsReport().ringBufferDiagnostics().totalCount());
    }
}
