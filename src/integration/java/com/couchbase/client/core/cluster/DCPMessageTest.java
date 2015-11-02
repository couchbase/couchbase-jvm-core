/**
 * Copyright (C) 2014 Couchbase, Inc.
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

import com.couchbase.client.core.config.CouchbaseBucketConfig;
import com.couchbase.client.core.message.ResponseStatus;
import com.couchbase.client.core.message.cluster.GetClusterConfigRequest;
import com.couchbase.client.core.message.cluster.GetClusterConfigResponse;
import com.couchbase.client.core.message.dcp.DCPRequest;
import com.couchbase.client.core.message.dcp.MutationMessage;
import com.couchbase.client.core.message.dcp.OpenConnectionRequest;
import com.couchbase.client.core.message.dcp.OpenConnectionResponse;
import com.couchbase.client.core.message.dcp.SnapshotMarkerMessage;
import com.couchbase.client.core.message.dcp.StreamRequestRequest;
import com.couchbase.client.core.message.dcp.StreamRequestResponse;
import com.couchbase.client.core.message.kv.UpsertRequest;
import com.couchbase.client.core.message.kv.UpsertResponse;
import com.couchbase.client.core.util.ClusterDependentTest;
import io.netty.buffer.Unpooled;
import io.netty.util.CharsetUtil;
import io.netty.util.ReferenceCountUtil;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;
import rx.Observable;
import rx.Subscriber;
import rx.observers.TestSubscriber;

import java.io.UnsupportedEncodingException;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.zip.CRC32;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Verifies basic functionality of DCP operations.
 *
 * @author Sergey Avseyev
 * @since 1.1.0
 */
public class DCPMessageTest extends ClusterDependentTest {

    @Before
    public void checkIfDCPEnabled() throws Exception {
        Assume.assumeTrue(isDCPEnabled());
    }

    @Test
    public void shouldRequestStream() throws Exception {
        List<OpenConnectionResponse> open = cluster()
                .<OpenConnectionResponse>send(new OpenConnectionRequest("hello", bucket()))
                .toList()
                .toBlocking()
                .single();
        assertEquals(1, open.size());
        for (OpenConnectionResponse response : open) {
            assertEquals(ResponseStatus.SUCCESS, response.status());
        }

        StreamRequestResponse addStream = cluster()
                .<StreamRequestResponse>send(new StreamRequestRequest("hello", calculateVBucketForKey("foo"), bucket()))
                .toBlocking()
                .single();
        assertEquals(ResponseStatus.SUCCESS, addStream.status());

        TestSubscriber<DCPRequest> subscriber = new TestSubscriber<DCPRequest>();
        open.get(0).connection().subject().takeUntil(Observable.timer(2, TimeUnit.SECONDS)).subscribe((Subscriber) subscriber);

        UpsertResponse foo = cluster()
                .<UpsertResponse>send(new UpsertRequest("foo", Unpooled.copiedBuffer("bar", CharsetUtil.UTF_8), 1, 0, bucket()))
                .toBlocking()
                .single();
        assertEquals(ResponseStatus.SUCCESS, foo.status());
        ReferenceCountUtil.releaseLater(foo.content());

        subscriber.awaitTerminalEvent();
        List<DCPRequest> items = subscriber.getOnNextEvents();

        boolean seenMutation = false;
        boolean seenSnapshot = false;
        for (DCPRequest found : items) {
            if (found instanceof SnapshotMarkerMessage) {
                seenSnapshot = true;
            } else if (found instanceof MutationMessage) {
                seenMutation = true;
                assertEquals("foo", ((MutationMessage) found).key());
                ReferenceCountUtil.releaseLater(((MutationMessage) found).content());
            }
        }

        assertTrue(seenMutation);
        assertTrue(seenSnapshot);
    }

    private short calculateVBucketForKey(String key) {
        GetClusterConfigResponse res = cluster()
                .<GetClusterConfigResponse>send(new GetClusterConfigRequest()).toBlocking().single();
        CouchbaseBucketConfig config = (CouchbaseBucketConfig) res.config().bucketConfig(bucket());
        CRC32 crc32 = new CRC32();
        try {
            crc32.update(key.getBytes("UTF-8"));
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
        long rv = (crc32.getValue() >> 16) & 0x7fff;
        return (short) ((int) rv & config.numberOfPartitions() - 1);
    }
}
