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
package com.couchbase.client.core.message.observe;

import com.couchbase.client.core.ClusterFacade;
import com.couchbase.client.core.CoreContext;
import com.couchbase.client.core.DocumentConcurrentlyModifiedException;
import com.couchbase.client.core.ReplicaNotConfiguredException;
import com.couchbase.client.core.config.ClusterConfig;
import com.couchbase.client.core.config.CouchbaseBucketConfig;
import com.couchbase.client.core.endpoint.ResponseStatusConverter;
import com.couchbase.client.core.endpoint.kv.KeyValueStatus;
import com.couchbase.client.core.env.CoreEnvironment;
import com.couchbase.client.core.env.DefaultCoreEnvironment;
import com.couchbase.client.core.message.CouchbaseRequest;
import com.couchbase.client.core.message.CouchbaseResponse;
import com.couchbase.client.core.message.ResponseStatus;
import com.couchbase.client.core.message.cluster.GetClusterConfigRequest;
import com.couchbase.client.core.message.cluster.GetClusterConfigResponse;
import com.couchbase.client.core.message.kv.ObserveRequest;
import com.couchbase.client.core.message.kv.ObserveResponse;
import com.couchbase.client.core.retry.BestEffortRetryStrategy;
import org.junit.AfterClass;
import org.junit.Test;
import rx.Observable;

import java.util.concurrent.TimeUnit;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.isA;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Verifies the functionality of {@link Observe}.
 *
 * @author Michael Nitschinger
 * @since 1.1.1
 */
public class ObserveTest {

    private static final CoreEnvironment ENV = DefaultCoreEnvironment.create();

    @AfterClass
    public static void cleanup() {
        ENV.shutdown();
    }

    @Test(expected = ReplicaNotConfiguredException.class)
    public void shouldFailFastWhenReplicateToGreaterThanBucketReplicas() {
        ClusterFacade cluster = mock(ClusterFacade.class);

        // Setup a mocked config which returns no replica configured
        CouchbaseBucketConfig bucketConfig = mock(CouchbaseBucketConfig.class);
        when(bucketConfig.numberOfReplicas()).thenReturn(0);
        ClusterConfig clusterConfig = mock(ClusterConfig.class);
        when(clusterConfig.bucketConfig("bucket")).thenReturn(bucketConfig);
        GetClusterConfigResponse clusterConfigResponse = new GetClusterConfigResponse(
            clusterConfig, ResponseStatus.SUCCESS
        );
        when(cluster.send(any(GetClusterConfigRequest.class))).thenReturn(
                Observable.just((CouchbaseResponse) clusterConfigResponse)
        );

        Observable<Boolean> result = Observe.call(
                cluster, "bucket", "id", 1234, false, Observe.PersistTo.NONE, Observe.ReplicateTo.ONE,
                BestEffortRetryStrategy.INSTANCE
        );
        result.toBlocking().single();
    }

    @Test(expected = ReplicaNotConfiguredException.class)
    public void shouldFailFastWhenPersistToGreaterThanBucketReplicas() {
        ClusterFacade cluster = mock(ClusterFacade.class);

        // Setup a mocked config which returns no replica configured
        CouchbaseBucketConfig bucketConfig = mock(CouchbaseBucketConfig.class);
        when(bucketConfig.numberOfReplicas()).thenReturn(2);
        ClusterConfig clusterConfig = mock(ClusterConfig.class);
        when(clusterConfig.bucketConfig("bucket")).thenReturn(bucketConfig);
        GetClusterConfigResponse clusterConfigResponse = new GetClusterConfigResponse(
                clusterConfig, ResponseStatus.SUCCESS
        );
        when(cluster.send(any(GetClusterConfigRequest.class))).thenReturn(
                Observable.just((CouchbaseResponse) clusterConfigResponse)
        );

        Observable<Boolean> result = Observe.call(
                cluster, "bucket", "id", 1234, false, Observe.PersistTo.FOUR, Observe.ReplicateTo.NONE,
                BestEffortRetryStrategy.INSTANCE
        );
        result.toBlocking().single();
    }

    /**
     * When the returned observe response is from the master but it contains a different cas
     * than requested, it is an indication that the document has been concurrently modified.
     */
    @Test(expected = DocumentConcurrentlyModifiedException.class)
    public void shouldFailWhenConcurrentlyModified() {
        CoreContext ctx = new CoreContext(ENV, null);
        ClusterFacade cluster = mock(ClusterFacade.class);
        when(cluster.ctx()).thenReturn(ctx);

        // Setup a mocked config which returns no replica configured
        CouchbaseBucketConfig bucketConfig = mock(CouchbaseBucketConfig.class);
        when(bucketConfig.numberOfReplicas()).thenReturn(1);
        ClusterConfig clusterConfig = mock(ClusterConfig.class);
        when(clusterConfig.bucketConfig("bucket")).thenReturn(bucketConfig);
        GetClusterConfigResponse clusterConfigResponse = new GetClusterConfigResponse(
                clusterConfig, ResponseStatus.SUCCESS
        );
        when(cluster.send(isA(GetClusterConfigRequest.class))).thenReturn(
                Observable.just((CouchbaseResponse) clusterConfigResponse)
        );
        ObserveResponse observeResponse = new ObserveResponse(
                ResponseStatus.SUCCESS, KeyValueStatus.SUCCESS.code(),
                ObserveResponse.ObserveStatus.FOUND_NOT_PERSISTED.value(),
                true,
                45678,
                "bucket",
                mock(CouchbaseRequest.class)
        );
        when(cluster.send(isA(ObserveRequest.class))).thenReturn(
                Observable.just((CouchbaseResponse) observeResponse)
        );

        Observable<Boolean> result = Observe.call(
                cluster, "bucket", "id", 1234, false, Observe.PersistTo.NONE, Observe.ReplicateTo.ONE,
                BestEffortRetryStrategy.INSTANCE
        );
        result
            .timeout(5, TimeUnit.SECONDS)
            .toBlocking()
            .single();
    }

    @Test
    public void shouldAlwaysAskActiveNode() {
        CoreContext ctx = new CoreContext(ENV, null);
        ClusterFacade cluster = mock(ClusterFacade.class);
        when(cluster.ctx()).thenReturn(ctx);

        // Setup a mocked config which returns no replica configured
        CouchbaseBucketConfig bucketConfig = mock(CouchbaseBucketConfig.class);
        when(bucketConfig.numberOfReplicas()).thenReturn(1);
        ClusterConfig clusterConfig = mock(ClusterConfig.class);
        when(clusterConfig.bucketConfig("bucket")).thenReturn(bucketConfig);
        GetClusterConfigResponse clusterConfigResponse = new GetClusterConfigResponse(
            clusterConfig, ResponseStatus.SUCCESS
        );
        when(cluster.send(isA(GetClusterConfigRequest.class))).thenReturn(
            Observable.just((CouchbaseResponse) clusterConfigResponse)
        );
        ObserveResponse observeResponse = new ObserveResponse(
            ResponseStatus.SUCCESS, KeyValueStatus.SUCCESS.code(),
            ObserveResponse.ObserveStatus.FOUND_NOT_PERSISTED.value(),
            false,
            45678,
            "bucket",
            mock(CouchbaseRequest.class)
        );
        when(cluster.send(isA(ObserveRequest.class))).thenReturn(
            Observable.just((CouchbaseResponse) observeResponse)
        );

        Observable<Boolean> result = Observe.call(
            cluster, "bucket", "id", 45678, false, Observe.PersistTo.NONE, Observe.ReplicateTo.ONE,
            BestEffortRetryStrategy.INSTANCE
        );
        result
            .timeout(5, TimeUnit.SECONDS)
            .toBlocking()
            .single();

        verify(cluster, times(2)).send(isA(ObserveRequest.class));
    }

}