/**
 * Copyright (c) 2015 Couchbase, Inc.
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
package com.couchbase.client.core.message.observe;

import com.couchbase.client.core.ClusterFacade;
import com.couchbase.client.core.DocumentConcurrentlyModifiedException;
import com.couchbase.client.core.ReplicaNotConfiguredException;
import com.couchbase.client.core.config.ClusterConfig;
import com.couchbase.client.core.config.CouchbaseBucketConfig;
import com.couchbase.client.core.endpoint.ResponseStatusConverter;
import com.couchbase.client.core.message.CouchbaseRequest;
import com.couchbase.client.core.message.CouchbaseResponse;
import com.couchbase.client.core.message.ResponseStatus;
import com.couchbase.client.core.message.cluster.GetClusterConfigRequest;
import com.couchbase.client.core.message.cluster.GetClusterConfigResponse;
import com.couchbase.client.core.message.kv.ObserveRequest;
import com.couchbase.client.core.message.kv.ObserveResponse;
import com.couchbase.client.core.retry.BestEffortRetryStrategy;
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
        ClusterFacade cluster = mock(ClusterFacade.class);

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
                ResponseStatus.SUCCESS, ResponseStatusConverter.BINARY_SUCCESS,
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
        ClusterFacade cluster = mock(ClusterFacade.class);

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
            ResponseStatus.SUCCESS, ResponseStatusConverter.BINARY_SUCCESS,
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