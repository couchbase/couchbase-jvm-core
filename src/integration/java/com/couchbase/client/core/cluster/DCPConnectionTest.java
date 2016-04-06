/*
 * Copyright (c) 2016 Couchbase, Inc.
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

import com.couchbase.client.core.endpoint.dcp.DCPConnection;
import com.couchbase.client.core.message.ResponseStatus;
import com.couchbase.client.core.message.cluster.OpenBucketRequest;
import com.couchbase.client.core.message.cluster.OpenBucketResponse;
import com.couchbase.client.core.message.cluster.SeedNodesRequest;
import com.couchbase.client.core.message.dcp.DCPMessage;
import com.couchbase.client.core.message.dcp.DCPRequest;
import com.couchbase.client.core.message.dcp.MutationMessage;
import com.couchbase.client.core.message.dcp.OpenConnectionRequest;
import com.couchbase.client.core.message.dcp.OpenConnectionResponse;
import com.couchbase.client.core.message.dcp.RemoveMessage;
import com.couchbase.client.core.message.dcp.SnapshotMarkerMessage;
import com.couchbase.client.core.message.kv.MutationToken;
import com.couchbase.client.core.message.kv.RemoveRequest;
import com.couchbase.client.core.message.kv.RemoveResponse;
import com.couchbase.client.core.message.kv.UpsertRequest;
import com.couchbase.client.core.message.kv.UpsertResponse;
import com.couchbase.client.core.util.TestProperties;
import io.netty.buffer.Unpooled;
import io.netty.util.CharsetUtil;
import io.netty.util.ReferenceCountUtil;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import rx.Observable;
import rx.functions.Action0;
import rx.functions.Action1;
import rx.observers.TestSubscriber;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

//TODO re-activate once infinite loop is fixed
@Ignore("DCPConnectionTest currently loops infinitely")
public class DCPConnectionTest extends DCPTest {

    @Before
    public void checkIfDCPEnabled() throws Exception {
        Assume.assumeTrue(isDCPEnabled());
    }

    private DCPConnection createConnection(String connectionName) {
        cluster().send(new SeedNodesRequest(Collections.singletonList(TestProperties.seedNode())));

        Observable<OpenBucketResponse> openBucketResponse =
                cluster().send(new OpenBucketRequest(TestProperties.bucket(), TestProperties.password()));
        assertEquals(ResponseStatus.SUCCESS, openBucketResponse.toBlocking().single().status());

        OpenConnectionResponse response = cluster().<OpenConnectionResponse>send(
                new OpenConnectionRequest(connectionName, bucket())).toBlocking().single();
        assertEquals(ResponseStatus.SUCCESS, response.status());

        return response.connection();
    }

    @Test
    public void shouldAddAndRemoveStreamsToConnection() {
        DCPConnection connection = createConnection("shouldAddAndRemoveStreamsToConnection");

        ResponseStatus status;

        status = connection.addStream((short) 42).toBlocking().single();
        assertEquals(ResponseStatus.SUCCESS, status);

        status = connection.addStream((short) 42).toBlocking().single();
        assertEquals(ResponseStatus.EXISTS, status);

        status = connection.removeStream((short) 42).toBlocking().single();
        assertEquals(ResponseStatus.SUCCESS, status);

        status = connection.removeStream((short) 42).toBlocking().single();
        assertEquals(ResponseStatus.NOT_EXISTS, status);
    }

    @Test
    public void shouldRollbackOnInvalidRange() {
        DCPConnection connection = createConnection("shouldRollbackOnInvalidRange");

        ResponseStatus status;

        status = connection.addStream((short) 1, 0, 42, 42, 0, 0).toBlocking().single();
        assertEquals(ResponseStatus.SUCCESS, status);
    }

    @Test
    public void shouldReturnCurrentState() {
        DCPConnection connection = createConnection("shouldReturnCurrentState");

        List<MutationToken> state;

        state = connection.getCurrentState().toList().toBlocking().single();
        assertEquals(numberOfPartitions(), state.size());
        for (MutationToken token : state) {
            assertTrue(token.vbucketUUID() > 0);
        }
    }

    private void upsertKey(String key, String value) {
        UpsertResponse resp = cluster()
                .<UpsertResponse>send(new UpsertRequest(key, Unpooled.copiedBuffer(value, CharsetUtil.UTF_8), 1, 0, bucket()))
                .toBlocking()
                .single();
        assertEquals(ResponseStatus.SUCCESS, resp.status());
        ReferenceCountUtil.releaseLater(resp.content());
    }

    private void removeKey(String key) {
        RemoveResponse resp = cluster()
                .<RemoveResponse>send(new RemoveRequest(key, bucket()))
                .toBlocking()
                .single();
        assertEquals(ResponseStatus.SUCCESS, resp.status());
    }

    @Test
    public void shouldTransmitTheData() {
        DCPConnection connection = createConnection("shouldTransmitTheData");

        connection.addStream(calculateVBucketForKey("foo")).toBlocking().single();
        TestSubscriber<DCPRequest> subscriber = new TestSubscriber<DCPRequest>();
        connection.subject().takeUntil(Observable.timer(5, TimeUnit.SECONDS)).subscribe(subscriber);

        upsertKey("foo", "value");
        removeKey("foo");

        subscriber.awaitTerminalEvent();
        List<DCPRequest> items = subscriber.getOnNextEvents();

        boolean seenMutation = false;
        boolean seenSnapshot = false;
        boolean seenRemove = false;
        for (DCPRequest found : items) {
            if (found instanceof SnapshotMarkerMessage) {
                seenSnapshot = true;
            } else if (found instanceof MutationMessage) {
                seenMutation = true;
                assertEquals("foo", ((MutationMessage) found).key());
                ReferenceCountUtil.releaseLater(((MutationMessage) found).content());
            } else if (found instanceof RemoveMessage) {
                seenRemove = true;
                assertEquals("foo", ((RemoveMessage) found).key());
            }
        }

        assertTrue(seenMutation);
        assertTrue(seenSnapshot);
        assertTrue(seenRemove);
    }

    @Test
    public void shouldUseFlowControl() throws InterruptedException {
        final String fooValue = "shouldUseFlowControl---foo-value";
        final String barValue = "shouldUseFlowControl---bar-value";
        final DCPConnection connection = createConnection("shouldUseFlowControl");

        List<MutationToken> state = connection.getCurrentState().toList().toBlocking().single();
        assertEquals(numberOfPartitions(), state.size());
        for (MutationToken token : state) {
            connection.addStream(
                    (short) token.vbucketID(),
                    token.vbucketUUID(),
                    token.sequenceNumber(),
                    0xffffffff,
                    token.sequenceNumber(),
                    0xffffffff
            ).toBlocking().single();
        }

        final AtomicInteger fooMutations = new AtomicInteger(0);
        final AtomicInteger barMutations = new AtomicInteger(0);
        final CountDownLatch latch = new CountDownLatch(1);

        connection.subject()
                .takeUntil(Observable.timer(10, TimeUnit.SECONDS))
                .subscribe(
                        new Action1<DCPRequest>() {
                            @Override
                            public void call(DCPRequest request) {
                                if (request instanceof MutationMessage) {
                                    MutationMessage mutation = (MutationMessage) request;
                                    String key = mutation.key();
                                    if (key.equals("foo")) {
                                        assertEquals(mutation.content().toString(CharsetUtil.UTF_8), fooValue);
                                        fooMutations.incrementAndGet();
                                    } else if (key.equals("bar")) {
                                        assertEquals(mutation.content().toString(CharsetUtil.UTF_8), barValue);
                                        barMutations.incrementAndGet();
                                    } else {
                                        fail("unexpected mutation of key: " + key);
                                    }
                                    ReferenceCountUtil.releaseLater(((MutationMessage) request).content());
                                }
                                if (request instanceof DCPMessage) {
                                    connection.consumed((DCPMessage) request);
                                }
                            }
                        },
                        new Action1<Throwable>() {
                            @Override
                            public void call(Throwable throwable) {
                                fail(throwable.toString());
                            }
                        },
                        new Action0() {
                            @Override
                            public void call() {
                                latch.countDown();
                            }
                        });

        for (int i = 0; i < 10; i++) {
            upsertKey("foo", fooValue);
            upsertKey("bar", barValue);
        }

        latch.await();

        assertEquals(10, fooMutations.get());
        assertEquals(10, barMutations.get());
    }
}