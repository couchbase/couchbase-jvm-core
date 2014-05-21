package com.couchbase.client.core.cluster;

import com.couchbase.client.core.message.cluster.DisconnectRequest;
import com.couchbase.client.core.message.cluster.OpenBucketRequest;
import com.couchbase.client.core.message.cluster.OpenBucketResponse;
import com.couchbase.client.core.message.cluster.SeedNodesRequest;
import com.couchbase.client.core.message.cluster.SeedNodesResponse;
import com.couchbase.client.core.message.query.GenericQueryRequest;
import com.couchbase.client.core.message.query.GenericQueryResponse;
import com.couchbase.client.core.util.TestProperties;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import rx.Observable;
import rx.Subscriber;
import rx.functions.Func1;

import java.util.concurrent.CountDownLatch;

/**
 * Created by michael on 21/05/14.
 */
public class QueryMessageTest {

    private static final String seedNode = TestProperties.seedNode();
    private static final String bucket = TestProperties.bucket();
    private static final String password = TestProperties.password();

    private static Cluster cluster;

    @BeforeClass
    public static void connect() {
        System.setProperty("com.couchbase.client.queryEnabled", "true");
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

    @AfterClass
    public static void disconnect() throws InterruptedException {
        cluster.send(new DisconnectRequest()).toBlockingObservable().first();
    }

    @Test
    public void foo() throws Exception {
        GenericQueryRequest request = new GenericQueryRequest("select * from default limit 10", "default", "");
        Observable<GenericQueryResponse> response = cluster.send(request);

        final CountDownLatch latch = new CountDownLatch(1);
        response.subscribe(new Subscriber<GenericQueryResponse>() {
            @Override
            public void onCompleted() {
                System.out.println("complete"); latch.countDown();
            }

            @Override
            public void onError(Throwable e) {
                System.out.println(e);
            }

            @Override
            public void onNext(GenericQueryResponse genericQueryResponse) {
                System.out.println(genericQueryResponse.content());
            }
        });

        latch.await();
    }

}
