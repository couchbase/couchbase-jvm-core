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

import com.couchbase.client.core.config.ConfigurationException;
import com.couchbase.client.core.env.CoreEnvironment;
import com.couchbase.client.core.env.DefaultCoreEnvironment;
import com.couchbase.client.core.message.ResponseStatus;
import com.couchbase.client.core.message.cluster.CloseBucketRequest;
import com.couchbase.client.core.message.cluster.OpenBucketRequest;
import com.couchbase.client.core.message.cluster.OpenBucketResponse;
import com.couchbase.client.core.message.cluster.SeedNodesRequest;
import com.couchbase.client.core.util.ClusterDependentTest;
import com.couchbase.client.core.util.TestProperties;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import rx.Observable;
import rx.functions.Func0;

import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assume.assumeFalse;

/**
 * Verifies basic bucket lifecycles like opening and closing of valid and invalid buckets.
 *
 * @author Michael Nitschinger
 * @since 1.0
 */
public class BucketLifecycleTest {

    private static volatile CoreEnvironment ENV;

    @Before
    public void check() {
        assumeFalse(TestProperties.isCi());
    }

    @BeforeClass
    public static void setup () {
        DefaultCoreEnvironment.Builder builder = DefaultCoreEnvironment.builder();
        ClusterDependentTest.configurPortsIfMocked(builder);
        ENV = builder.build();
    }

    @AfterClass
    public static void cleanup() {
       ENV.shutdown();
    }

    @Test
    public void shouldSuccessfullyOpenBucket() throws Exception {
        CouchbaseCore core = new CouchbaseCore(ENV);

        core.send(new SeedNodesRequest(Arrays.asList(TestProperties.seedNode())));
        OpenBucketRequest request;
        if (ClusterDependentTest.minClusterVersion()[0] >= 5) {
            request = new OpenBucketRequest(TestProperties.bucket(), TestProperties.adminUser(), TestProperties.adminPassword());
        } else {
            request = new OpenBucketRequest(TestProperties.bucket(), TestProperties.username(), TestProperties.password());
        }
        Observable<OpenBucketResponse> response = core.send(request);
        assertEquals(ResponseStatus.SUCCESS, response.toBlocking().single().status());
        core.send(new CloseBucketRequest(TestProperties.bucket())).toBlocking().single();
    }

    @Test(expected = ConfigurationException.class)
    public void shouldFailWithNoSeedNodeList() {
        OpenBucketRequest request = new OpenBucketRequest(TestProperties.bucket(), TestProperties.username(), TestProperties.password());
        new CouchbaseCore(ENV).send(request).toBlocking().single();
    }

    @Test(expected = ConfigurationException.class)
    public void shouldFailWithEmptySeedNodeList() {
        CouchbaseCore core = new CouchbaseCore(ENV);
        core.send(new SeedNodesRequest(Collections.<String>emptyList()));
        OpenBucketRequest request = new OpenBucketRequest(TestProperties.bucket(), TestProperties.username(), TestProperties.password());
        core.send(request).toBlocking().single();
    }

    @Test(expected = ConfigurationException.class)
    public void shouldFailOpeningNonExistentBucket() {
        CouchbaseCore core = new CouchbaseCore(ENV);

        core.send(new SeedNodesRequest(Arrays.asList(TestProperties.seedNode())));
        OpenBucketRequest request = new OpenBucketRequest(TestProperties.bucket() + "asd", TestProperties.password());
        core.send(request).toBlocking().single();
    }

    @Test(expected = ConfigurationException.class)
    public void shouldFailOpeningBucketWithWrongPassword() {
        CouchbaseCore core = new CouchbaseCore(ENV);

        core.send(new SeedNodesRequest(Arrays.asList(TestProperties.seedNode())));
        OpenBucketRequest request = new OpenBucketRequest(TestProperties.bucket(), TestProperties.username(), TestProperties.password() + "asd");
        core.send(request).toBlocking().single();
    }

    @Test(expected = ConfigurationException.class)
    public void shouldFailOpeningWithWrongHost() {
        CouchbaseCore core = new CouchbaseCore(ENV);

        core.send(new SeedNodesRequest(Arrays.asList("certainlyInvalidHostname")));
        OpenBucketRequest request = new OpenBucketRequest(TestProperties.bucket(), TestProperties.username(), TestProperties.password() + "asd");
        core.send(request).toBlocking().single();
    }

    @Test
    public void shouldSucceedSubsequentlyAfterFailedAttempt() throws Exception {
        final CouchbaseCore core = new CouchbaseCore(ENV);

        core.send(new SeedNodesRequest(Arrays.asList(TestProperties.seedNode())));

        OpenBucketRequest badAttempt = new OpenBucketRequest(TestProperties.bucket() + "asd", TestProperties.username(), TestProperties.password());

        final OpenBucketRequest goodAttempt;
        if (ClusterDependentTest.minClusterVersion()[0] >= 5) {
            goodAttempt = new OpenBucketRequest(TestProperties.bucket(), TestProperties.adminUser(), TestProperties.adminPassword());
        } else {
            goodAttempt = new OpenBucketRequest(TestProperties.bucket(), TestProperties.username(), TestProperties.password());

        }

        OpenBucketResponse response = core
            .<OpenBucketResponse>send(badAttempt)
            .onErrorResumeNext(Observable.defer(new Func0<Observable<OpenBucketResponse>>() {
                @Override
                public Observable<OpenBucketResponse> call() {
                    return core.send(goodAttempt);
                }
            }))
            .timeout(10, TimeUnit.SECONDS)
            .toBlocking()
            .single();

        assertEquals(ResponseStatus.SUCCESS, response.status());
        core.send(new CloseBucketRequest(TestProperties.bucket())).toBlocking().single();
    }
}
