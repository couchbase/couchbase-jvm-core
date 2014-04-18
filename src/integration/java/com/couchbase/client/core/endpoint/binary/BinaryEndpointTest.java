package com.couchbase.client.core.endpoint.binary;

import com.couchbase.client.core.env.CouchbaseEnvironment;
import com.couchbase.client.core.message.CouchbaseResponse;
import com.couchbase.client.core.message.binary.GetRequest;
import com.couchbase.client.core.message.binary.GetResponse;
import com.couchbase.client.core.message.binary.UpsertRequest;
import com.couchbase.client.core.message.binary.UpsertResponse;
import com.couchbase.client.core.message.internal.SignalFlush;
import io.netty.buffer.Unpooled;
import io.netty.util.CharsetUtil;
import org.junit.Test;
import rx.subjects.AsyncSubject;
import rx.subjects.Subject;

import static org.junit.Assert.assertEquals;

public class BinaryEndpointTest {

    private final CouchbaseEnvironment env = new CouchbaseEnvironment();

    @Test
    public void shouldSendGetRequestWithNotFound() {
        BinaryEndpoint endpoint = new BinaryEndpoint("127.0.0.1", env);
        endpoint.connect().toBlockingObservable().single();

        GetRequest request = new GetRequest("key");
        Subject<CouchbaseResponse, CouchbaseResponse> subject = AsyncSubject.create();
        request.observable(subject);
        endpoint.send(request);
        endpoint.send(SignalFlush.INSTANCE);

        GetResponse response = (GetResponse) request.observable().toBlockingObservable().single();
        assertEquals("Not found", response.content());
    }

    @Test
    public void shouldSendUpsertRequest() {
        BinaryEndpoint endpoint = new BinaryEndpoint("127.0.0.1", env);
        endpoint.connect().toBlockingObservable().single();

        UpsertRequest request = new UpsertRequest("key1", Unpooled.copiedBuffer("Hello World", CharsetUtil.UTF_8), "bucket", "");
        Subject<CouchbaseResponse, CouchbaseResponse> subject = AsyncSubject.create();
        request.observable(subject);
        endpoint.send(request);
        endpoint.send(SignalFlush.INSTANCE);

        UpsertResponse response = (UpsertResponse) request.observable().toBlockingObservable().single();
        System.out.println(response);
    }

}
