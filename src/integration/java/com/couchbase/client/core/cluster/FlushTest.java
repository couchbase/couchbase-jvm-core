package com.couchbase.client.core.cluster;

import com.couchbase.client.core.message.ResponseStatus;
import com.couchbase.client.core.message.binary.GetRequest;
import com.couchbase.client.core.message.binary.GetResponse;
import com.couchbase.client.core.message.binary.UpsertRequest;
import com.couchbase.client.core.message.binary.UpsertResponse;
import com.couchbase.client.core.message.config.FlushRequest;
import com.couchbase.client.core.message.config.FlushResponse;
import com.couchbase.client.core.util.ClusterDependentTest;
import io.netty.buffer.Unpooled;
import io.netty.util.CharsetUtil;
import org.junit.Test;
import rx.Observable;
import rx.functions.Func1;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 * Verifies the functionality of Flush in various scenarios.
 *
 * @author Michael Nitschinger
 * @since 1.0
 */
public class FlushTest extends ClusterDependentTest {

    @Test
    public void shouldFlush() {
        List<String> keys = Arrays.asList("key1", "key2", "key3");

        Observable.from(keys).flatMap(new Func1<String, Observable<UpsertResponse>>() {
            @Override
            public Observable<UpsertResponse> call(String key) {
                return cluster().send(new UpsertRequest(key, Unpooled.copiedBuffer("Content", CharsetUtil.UTF_8), bucket()));
            }
        }).toBlocking().last();

        Observable<FlushResponse> response = cluster().send(new FlushRequest(bucket(), password()));
        assertEquals(ResponseStatus.SUCCESS, response.toBlocking().first().status());

        List<GetResponse> responses = Observable
            .from(keys)
            .flatMap(new Func1<String, Observable<GetResponse>>() {
                @Override
                public Observable<GetResponse> call(String key) {
                    return cluster().send(new GetRequest(key, bucket()));
                }
            }).toList().toBlocking().single();

        assertEquals(keys.size(), responses.size());
        for (GetResponse get : responses) {
            assertEquals(ResponseStatus.NOT_EXISTS, get.status());
        }
    }

}
