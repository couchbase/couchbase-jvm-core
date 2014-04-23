package com.couchbase.client.core.cluster;

import com.couchbase.client.core.message.binary.GetRequest;
import com.couchbase.client.core.message.binary.GetResponse;
import com.couchbase.client.core.message.binary.UpsertRequest;
import com.couchbase.client.core.message.binary.UpsertResponse;
import com.couchbase.client.core.message.cluster.OpenBucketRequest;
import com.couchbase.client.core.message.cluster.OpenBucketResponse;
import com.couchbase.client.core.message.cluster.SeedNodesRequest;
import com.couchbase.client.core.message.cluster.SeedNodesResponse;
import io.netty.buffer.Unpooled;
import io.netty.util.CharsetUtil;
import org.junit.Test;
import rx.Observable;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Verifies basic functionality of binary operations.
 */
public class BinaryMessageTest {


    @Test
    public void shouldUpsertAndGetDocument() throws Exception {
        final Cluster cluster = new CouchbaseCluster();

        // Send seed nodes list
        Observable<SeedNodesResponse> initObservable = cluster.send(new SeedNodesRequest("127.0.0.1"));
        assertTrue(initObservable.toBlockingObservable().single().success());

        // Open bucket
        Observable<OpenBucketResponse> bucketObservable = cluster.send(new OpenBucketRequest("default", ""));
        bucketObservable.toBlockingObservable().single();

        // Do a upsert (== set for new api)
        UpsertRequest upsert = new UpsertRequest("key", Unpooled.copiedBuffer("Hello", CharsetUtil.UTF_8), "default");
        cluster.<UpsertResponse>send(upsert).toBlockingObservable().single();

        // Do a get request
        GetRequest request = new GetRequest("key", "default");
        assertEquals("Hello", cluster.<GetResponse>send(request).toBlockingObservable().single().content());
    }

}
