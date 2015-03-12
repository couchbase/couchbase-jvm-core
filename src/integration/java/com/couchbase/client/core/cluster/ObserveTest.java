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
package com.couchbase.client.core.cluster;

import com.couchbase.client.core.ReplicaNotConfiguredException;
import com.couchbase.client.core.config.CouchbaseBucketConfig;
import com.couchbase.client.core.message.cluster.GetClusterConfigRequest;
import com.couchbase.client.core.message.cluster.GetClusterConfigResponse;
import com.couchbase.client.core.message.kv.InsertRequest;
import com.couchbase.client.core.message.kv.InsertResponse;
import com.couchbase.client.core.message.kv.RemoveRequest;
import com.couchbase.client.core.message.kv.RemoveResponse;
import com.couchbase.client.core.message.observe.Observe;
import com.couchbase.client.core.retry.BestEffortRetryStrategy;
import com.couchbase.client.core.util.ClusterDependentTest;
import io.netty.buffer.Unpooled;
import io.netty.util.CharsetUtil;
import io.netty.util.ReferenceCountUtil;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertTrue;

/**
 * Verifies the functionality of the {@link Observe} related method overloads.
 *
 * Since durability requirements are heavily dependent on the number of nodes and replicas configured in the
 * cluster, not all tests can be run with all cluster setups.
 *
 * @author Michael Nitschinger
 * @since 1.1.2
 */
public class ObserveTest extends ClusterDependentTest {

    private CouchbaseBucketConfig config = null;
    private int numberOfReplicas;
    private int numberOfNodes;

    @Before
    public void gatherClusterInfo() {
        if (config == null) {
            GetClusterConfigResponse res = cluster().<GetClusterConfigResponse>send(new GetClusterConfigRequest())
                .toBlocking().single();
            config = (CouchbaseBucketConfig) res.config().bucketConfig(bucket());
            numberOfNodes = config.nodes().size();
            numberOfReplicas = config.numberOfReplicas();
        }
    }


    /**
     * Test that a previously inserted document is correctly persisted to disk on the master node.
     */
    @Test
    public void shouldObservePersistToMaster() {
        InsertRequest request = new InsertRequest("persInsDoc1", Unpooled.copiedBuffer("test", CharsetUtil.UTF_8), bucket());
        InsertResponse response = cluster().<InsertResponse>send(request).toBlocking().single();
        assertTrue(response.status().isSuccess());
        ReferenceCountUtil.release(response);

        Boolean observeSuccess = Observe.call(
                cluster(),
                bucket(),
                "persInsDoc1",
                response.cas(),
                false,
                Observe.PersistTo.MASTER,
                Observe.ReplicateTo.NONE,
                BestEffortRetryStrategy.INSTANCE
        ).timeout(5, TimeUnit.SECONDS).toBlocking().single();

        assertTrue(observeSuccess);
    }

    @Test
    public void shouldObservePersistToMasterOnRemoval() {
        InsertRequest request = new InsertRequest("persRemDoc1", Unpooled.copiedBuffer("test", CharsetUtil.UTF_8), bucket());
        InsertResponse response = cluster().<InsertResponse>send(request).toBlocking().single();
        assertTrue(response.status().isSuccess());
        ReferenceCountUtil.release(response);

        RemoveRequest removeRequest = new RemoveRequest("persRemDoc1", bucket());
        RemoveResponse removeResponse = cluster().<RemoveResponse>send(removeRequest).toBlocking().single();
        assertTrue(removeResponse.status().isSuccess());
        ReferenceCountUtil.release(removeResponse);

        Boolean observeSuccess = Observe.call(
                cluster(),
                bucket(),
                "persRemDoc1",
                removeResponse.cas(),
                true,
                Observe.PersistTo.MASTER,
                Observe.ReplicateTo.NONE,
                BestEffortRetryStrategy.INSTANCE
        ).timeout(5, TimeUnit.SECONDS).toBlocking().single();

        assertTrue(observeSuccess);
    }

    /**
     * Tests that it also works when a document should be observed on removal which is not even
     * created in the first place.
     *
     * The expectation is that the server directly responds with a "real delete".
     */
    @Test
    public void shouldObservePersistenceOnNonExistentDocRemoval() {
        Boolean observeSuccess = Observe.call(
                cluster(),
                bucket(),
                "nonExistentDoc",
                12345,
                true,
                Observe.PersistTo.MASTER,
                Observe.ReplicateTo.NONE,
                BestEffortRetryStrategy.INSTANCE
        ).timeout(5, TimeUnit.SECONDS).toBlocking().single();

        assertTrue(observeSuccess);
    }

    /**
     * Test that a previously inserted document is replicated to at least one replica.
     *
     * This test assumes a cluster setup where at least one replica is configured on the bucket and more or equal
     * to two nodes are available in order to correctly complete the test.
     */
    @Test
    public void shouldObserveReplicateToOne() {
        Assume.assumeTrue(numberOfReplicas >= 1 && numberOfNodes >= 2);

        InsertRequest request = new InsertRequest("persInsDoc2", Unpooled.copiedBuffer("test", CharsetUtil.UTF_8), bucket());
        InsertResponse response = cluster().<InsertResponse>send(request).toBlocking().single();
        assertTrue(response.status().isSuccess());
        ReferenceCountUtil.release(response);

        Boolean observeSuccess = Observe.call(
                cluster(),
                bucket(),
                "persInsDoc2",
                response.cas(),
                false,
                Observe.PersistTo.NONE,
                Observe.ReplicateTo.ONE,
                BestEffortRetryStrategy.INSTANCE
        ).timeout(5, TimeUnit.SECONDS).toBlocking().single();

        assertTrue(observeSuccess);
    }

    /**
     * Test that both persistence and replication are working against a previously inserted doc.
     *
     * This test assumes a cluster setup where at least one replica is configured on the bucket and more or equal
     * to two nodes are available in order to correctly complete the test.
     */
    @Test
    public void shouldObserveReplicateToOneAndPersistToMaster() {
        Assume.assumeTrue(numberOfReplicas >= 1 && numberOfNodes >= 2);

        InsertRequest request = new InsertRequest("persInsDoc3", Unpooled.copiedBuffer("test", CharsetUtil.UTF_8), bucket());
        InsertResponse response = cluster().<InsertResponse>send(request).toBlocking().single();
        assertTrue(response.status().isSuccess());
        ReferenceCountUtil.release(response);

        Boolean observeSuccess = Observe.call(
                cluster(),
                bucket(),
                "persInsDoc3",
                response.cas(),
                false,
                Observe.PersistTo.MASTER,
                Observe.ReplicateTo.ONE,
                BestEffortRetryStrategy.INSTANCE
        ).timeout(5, TimeUnit.SECONDS).toBlocking().single();

        assertTrue(observeSuccess);
    }

    /**
     * Test the persisting requirement on any node (either master or replica), whoever is first.
     */
    @Test
    public void shouldObservePersistToOne() {
        InsertRequest request = new InsertRequest("persInsDoc4", Unpooled.copiedBuffer("test", CharsetUtil.UTF_8), bucket());
        InsertResponse response = cluster().<InsertResponse>send(request).toBlocking().single();
        assertTrue(response.status().isSuccess());
        ReferenceCountUtil.release(response);

        Boolean observeSuccess = Observe.call(
                cluster(),
                bucket(),
                "persInsDoc4",
                response.cas(),
                false,
                Observe.PersistTo.ONE,
                Observe.ReplicateTo.NONE,
                BestEffortRetryStrategy.INSTANCE
        ).timeout(5, TimeUnit.SECONDS).toBlocking().single();

        assertTrue(observeSuccess);
    }

    /**
     * Test the fail fast mechanism when one asks for more replicas than are configured on the bucket, so it will
     * never be possible to successfully observe the state requested.
     *
     * This test can only be run if less than 3 replicas are defined on the bucket.
     */
    @Test(expected = ReplicaNotConfiguredException.class)
    public void shouldFailReplicaIfLessReplicaConfigureOnBucket() {
        Assume.assumeTrue(numberOfReplicas < 3);

        Observe.call(
                cluster(),
                bucket(),
                "someDoc",
                1234,
                false,
                Observe.PersistTo.NONE,
                Observe.ReplicateTo.THREE,
                BestEffortRetryStrategy.INSTANCE
        ).timeout(5, TimeUnit.SECONDS).toBlocking().single();
    }

    /**
     * Test the fail fast mechanism when one asks for more replicas than are configured on the bucket, so it will
     * never be possible to successfully observe the state requested.
     *
     * This test can only be run if less than 3 replicas are defined on the bucket.
     */
    @Test(expected = ReplicaNotConfiguredException.class)
    public void shouldFailPersistIfLessReplicaConfigureOnBucket() {
        Assume.assumeTrue(numberOfReplicas < 3);

        Observe.call(
                cluster(),
                bucket(),
                "someDoc",
                1234,
                false,
                Observe.PersistTo.FOUR,
                Observe.ReplicateTo.NONE,
                BestEffortRetryStrategy.INSTANCE
        ).timeout(5, TimeUnit.SECONDS).toBlocking().single();
    }

}
