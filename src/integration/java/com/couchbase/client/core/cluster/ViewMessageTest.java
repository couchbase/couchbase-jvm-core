package com.couchbase.client.core.cluster;

import com.couchbase.client.core.message.cluster.OpenBucketRequest;
import com.couchbase.client.core.message.cluster.OpenBucketResponse;
import com.couchbase.client.core.message.cluster.SeedNodesRequest;
import com.couchbase.client.core.message.cluster.SeedNodesResponse;
import com.couchbase.client.core.message.view.ViewQueryRequest;
import com.couchbase.client.core.message.view.ViewQueryResponse;
import com.couchbase.client.core.util.TestProperties;
import io.netty.util.CharsetUtil;
import org.junit.BeforeClass;
import org.junit.Test;
import rx.Observable;
import rx.functions.Func1;

import static org.junit.Assert.assertEquals;

public class ViewMessageTest {

    private static final String seedNode = TestProperties.seedNode();
    private static final String bucket = TestProperties.bucket();
    private static final String password = TestProperties.password();

    private static Cluster cluster;

    @BeforeClass
    public static void connect() {
        cluster = new CouchbaseCluster();
        cluster.<SeedNodesResponse>send(new SeedNodesRequest(seedNode)).flatMap(
            new Func1<SeedNodesResponse, Observable<OpenBucketResponse>>() {
                @Override
                public Observable<OpenBucketResponse> call(SeedNodesResponse response) {
                    return cluster.send(new OpenBucketRequest(bucket, password));
                }
            }
        ).toBlockingObservable().single();
    }

    @Test
    public void shoudQueryView() {
        ViewQueryResponse single = cluster
            .<ViewQueryResponse>send(new ViewQueryRequest("design", "view", bucket, password))
            .toBlockingObservable()
            .single();

        String expected = "{\"error\":\"not_found\",\"reason\":\"Design document _design/design not found\"}\n";
        assertEquals(expected, single.content().toString(CharsetUtil.UTF_8));
    }
}
