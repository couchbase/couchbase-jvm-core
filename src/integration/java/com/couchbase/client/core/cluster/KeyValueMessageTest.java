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
package com.couchbase.client.core.cluster;

import com.couchbase.client.core.message.ResponseStatus;
import com.couchbase.client.core.message.kv.AppendRequest;
import com.couchbase.client.core.message.kv.AppendResponse;
import com.couchbase.client.core.message.kv.CounterRequest;
import com.couchbase.client.core.message.kv.CounterResponse;
import com.couchbase.client.core.message.kv.GetRequest;
import com.couchbase.client.core.message.kv.GetResponse;
import com.couchbase.client.core.message.kv.InsertRequest;
import com.couchbase.client.core.message.kv.InsertResponse;
import com.couchbase.client.core.message.kv.MutationToken;
import com.couchbase.client.core.message.kv.RemoveRequest;
import com.couchbase.client.core.message.kv.RemoveResponse;
import com.couchbase.client.core.message.kv.ReplaceRequest;
import com.couchbase.client.core.message.kv.ReplaceResponse;
import com.couchbase.client.core.message.kv.TouchRequest;
import com.couchbase.client.core.message.kv.TouchResponse;
import com.couchbase.client.core.message.kv.UnlockRequest;
import com.couchbase.client.core.message.kv.UnlockResponse;
import com.couchbase.client.core.message.kv.UpsertRequest;
import com.couchbase.client.core.message.kv.UpsertResponse;
import com.couchbase.client.core.util.ClusterDependentTest;
import io.netty.buffer.Unpooled;
import io.netty.util.CharsetUtil;
import io.netty.util.ReferenceCountUtil;
import org.junit.BeforeClass;
import org.junit.Test;
import rx.Observable;
import rx.functions.Func1;

import java.util.Arrays;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Verifies basic functionality of binary operations.
 *
 * @author Michael Nitschinger
 * @since 1.0
 */
public class KeyValueMessageTest extends ClusterDependentTest {

    @BeforeClass
    public static void setup() throws Exception {
        connect();
        assertTrue(cluster().ctx().coreId() > 0);
    }

    @Test
    public void shouldUpsertAndGetDocument() throws Exception {
        String key = "upsert-key";
        String content = "Hello World!";
        UpsertRequest upsert = new UpsertRequest(key, Unpooled.copiedBuffer(content, CharsetUtil.UTF_8), bucket());
        UpsertResponse response = cluster().<UpsertResponse>send(upsert).toBlocking().single();
        ReferenceCountUtil.releaseLater(response.content());
        assertValidMetadata(response.mutationToken());

        GetRequest request = new GetRequest(key, bucket());
        GetResponse getResponse = cluster().<GetResponse>send(request).toBlocking().single();
        assertEquals(content, getResponse.content().toString(CharsetUtil.UTF_8));
        ReferenceCountUtil.releaseLater(getResponse.content());
    }

    @Test
    public void shouldUpsertWithExpiration() throws Exception {
        String key = "upsert-key-vanish";
        String content = "Hello World!";
        UpsertRequest upsert = new UpsertRequest(key, Unpooled.copiedBuffer(content, CharsetUtil.UTF_8), 1, 0, bucket());
        UpsertResponse response = cluster().<UpsertResponse>send(upsert).toBlocking().single();
        ReferenceCountUtil.releaseLater(response.content());
        assertValidMetadata(response.mutationToken());

        Thread.sleep(2000);

        GetRequest request = new GetRequest(key, bucket());
        GetResponse getResponse = cluster().<GetResponse>send(request).toBlocking().single();
        assertEquals(ResponseStatus.NOT_EXISTS, getResponse.status());
        ReferenceCountUtil.releaseLater(getResponse.content());
    }

    @Test
    public void shouldHandleDoubleInsert() throws Exception {
        String key = "insert-key";
        String content = "Hello World!";
        InsertRequest insert = new InsertRequest(key, Unpooled.copiedBuffer(content, CharsetUtil.UTF_8), bucket());
        InsertResponse insertResponse = cluster().<InsertResponse>send(insert).toBlocking().single();
        assertEquals(ResponseStatus.SUCCESS, insertResponse.status());
        ReferenceCountUtil.releaseLater(insertResponse.content());
        assertValidMetadata(insertResponse.mutationToken());

        insert = new InsertRequest(key, Unpooled.copiedBuffer(content, CharsetUtil.UTF_8), bucket());
        insertResponse = cluster().<InsertResponse>send(insert).toBlocking().single();
        assertEquals(ResponseStatus.EXISTS, insertResponse.status());
        ReferenceCountUtil.releaseLater(insertResponse.content());
        assertNull(insertResponse.mutationToken());
    }

    @Test
    public void shouldReplaceWithoutCAS() throws Exception {
        final String key = "replace-key";
        final String content = "replace content";

        ReplaceRequest insert = new ReplaceRequest(key, Unpooled.copiedBuffer(content, CharsetUtil.UTF_8), bucket());
        ReplaceResponse response = cluster().<ReplaceResponse>send(insert).toBlocking().single();
        assertEquals(ResponseStatus.NOT_EXISTS, response.status());
        ReferenceCountUtil.releaseLater(response.content());
        assertNull(response.mutationToken());

        UpsertRequest upsert = new UpsertRequest(key, Unpooled.copiedBuffer("insert content", CharsetUtil.UTF_8), bucket());
        response = cluster()
            .<UpsertResponse>send(upsert)
            .flatMap(new Func1<UpsertResponse, Observable<ReplaceResponse>>() {
                 @Override
                 public Observable<ReplaceResponse> call(UpsertResponse response) {
                     ReferenceCountUtil.releaseLater(response.content());
                     return cluster().send(new ReplaceRequest(key, Unpooled.copiedBuffer(content, CharsetUtil.UTF_8), bucket()));
                 }
            }).toBlocking().single();
        ReferenceCountUtil.releaseLater(response.content());
        assertValidMetadata(response.mutationToken());

        assertEquals(ResponseStatus.SUCCESS, response.status());
    }

    @Test
    public void shouldReplaceWithFailingCAS() {
        final String key = "replace-key-cas-fail";
        final String content = "replace content";

        ReplaceRequest insert = new ReplaceRequest(key, Unpooled.copiedBuffer(content, CharsetUtil.UTF_8), bucket());
        ReplaceResponse response = cluster().<ReplaceResponse>send(insert).toBlocking().single();
        assertEquals(ResponseStatus.NOT_EXISTS, response.status());
        ReferenceCountUtil.releaseLater(response.content());
        assertNull(response.mutationToken());

        UpsertRequest upsert = new UpsertRequest(key, Unpooled.copiedBuffer("insert content", CharsetUtil.UTF_8), bucket());
        response = cluster().<UpsertResponse>send(upsert)
            .flatMap(new Func1<UpsertResponse, Observable<ReplaceResponse>>() {
                @Override
                public Observable<ReplaceResponse> call(UpsertResponse response) {
                    ReferenceCountUtil.releaseLater(response.content());
                    return cluster().send(new ReplaceRequest(key, Unpooled.copiedBuffer(content, CharsetUtil.UTF_8), 24234234L, bucket()));
                }
            }).toBlocking().single();
        ReferenceCountUtil.releaseLater(response.content());
        assertEquals(ResponseStatus.EXISTS, response.status());
        assertNull(response.mutationToken());
    }

    @Test
    public void shouldReplaceWithMatchingCAS() throws Exception {
        final String key = "replace-key-cas-match";
        final String content = "replace content";

        ReplaceRequest insert = new ReplaceRequest(key, Unpooled.copiedBuffer(content, CharsetUtil.UTF_8), bucket());
        ReplaceResponse response = cluster().<ReplaceResponse>send(insert).toBlocking().single();
        assertEquals(ResponseStatus.NOT_EXISTS, response.status());
        ReferenceCountUtil.releaseLater(response.content());
        assertNull(response.mutationToken());

        UpsertRequest upsert = new UpsertRequest(key, Unpooled.copiedBuffer("insert content", CharsetUtil.UTF_8), bucket());
        response = cluster().<UpsertResponse>send(upsert)
            .flatMap(new Func1<UpsertResponse, Observable<ReplaceResponse>>() {
                @Override
                public Observable<ReplaceResponse> call(UpsertResponse response) {
                    ReferenceCountUtil.releaseLater(response.content());
                    return cluster().send(new ReplaceRequest(key, Unpooled.copiedBuffer(content, CharsetUtil.UTF_8), response.cas(), bucket()));
                }
            }).toBlocking().single();
        ReferenceCountUtil.releaseLater(response.content());
        assertEquals(ResponseStatus.SUCCESS, response.status());
        assertValidMetadata(response.mutationToken());
    }

    @Test
    public void shouldRemoveDocumentWithoutCAS() throws Exception {
        String key = "remove-key";
        String content = "Hello World!";
        UpsertRequest upsert = new UpsertRequest(key, Unpooled.copiedBuffer(content, CharsetUtil.UTF_8), bucket());
        UpsertResponse upsertResponse = cluster().<UpsertResponse>send(upsert).toBlocking().single();
        assertEquals(ResponseStatus.SUCCESS, upsertResponse.status());
        ReferenceCountUtil.releaseLater(upsertResponse.content());
        assertValidMetadata(upsertResponse.mutationToken());

        RemoveRequest remove = new RemoveRequest(key, bucket());
        RemoveResponse response = cluster().<RemoveResponse>send(remove).toBlocking().single();
        assertEquals(ResponseStatus.SUCCESS, response.status());
        assertTrue(response.cas() != 0);
        ReferenceCountUtil.releaseLater(response.content());
        assertValidMetadata(response.mutationToken());
        assertMetadataSequence(upsertResponse.mutationToken(), response.mutationToken());

        GetRequest get = new GetRequest(key, bucket());
        GetResponse getResponse = cluster().<GetResponse>send(get).toBlocking().single();
        assertEquals(ResponseStatus.NOT_EXISTS, getResponse.status());
        ReferenceCountUtil.releaseLater(getResponse.content());
    }

    @Test
    public void shouldRemoveDocumentWithCAS() throws Exception {
        String key = "remove-key-cas";
        String content = "Hello World!";
        UpsertRequest upsert = new UpsertRequest(key, Unpooled.copiedBuffer(content, CharsetUtil.UTF_8), bucket());
        UpsertResponse upsertResponse = cluster().<UpsertResponse>send(upsert).toBlocking().single();
        assertEquals(ResponseStatus.SUCCESS, upsertResponse.status());
        ReferenceCountUtil.releaseLater(upsertResponse.content());
        assertValidMetadata(upsertResponse.mutationToken());

        RemoveRequest remove = new RemoveRequest(key, 1233443, bucket());
        RemoveResponse response = cluster().<RemoveResponse>send(remove).toBlocking().single();
        assertEquals(ResponseStatus.EXISTS, response.status());
        ReferenceCountUtil.releaseLater(response.content());
        assertNull(response.mutationToken());

        remove = new RemoveRequest(key, upsertResponse.cas(), bucket());
        response = cluster().<RemoveResponse>send(remove).toBlocking().single();
        assertEquals(ResponseStatus.SUCCESS, response.status());
        assertTrue(response.cas() != 0);
        ReferenceCountUtil.releaseLater(response.content());
        assertValidMetadata(response.mutationToken());
        assertMetadataSequence(upsertResponse.mutationToken(), response.mutationToken());
    }

    @Test
    public void shouldIncrementFromCounter() throws Exception {
        String key = "counter-incr";

        CounterResponse response1 = cluster().<CounterResponse>send(new CounterRequest(key, 0, 10, 0, bucket())).toBlocking().single();
        assertEquals(0, response1.value());

        CounterResponse response2 = cluster().<CounterResponse>send(new CounterRequest(key, 0, 10, 0, bucket())).toBlocking().single();
        assertEquals(10, response2.value());

        CounterResponse response3 = cluster().<CounterResponse>send(new CounterRequest(key, 0, 10, 0, bucket())).toBlocking().single();
        assertEquals(20, response3.value());

        assertTrue(response1.cas() != response2.cas());
        assertTrue(response2.cas() != response3.cas());

        assertMetadataSequence(response1.mutationToken(), response2.mutationToken());
        assertMetadataSequence(response2.mutationToken(), response3.mutationToken());
    }

    @Test
    public void shouldDecrementFromCounter() throws Exception {
        String key = "counter-decr";

        CounterResponse response1 = cluster().<CounterResponse>send(new CounterRequest(key, 100, -10, 0, bucket())).toBlocking().single();
        assertEquals(100, response1.value());

        CounterResponse response2 = cluster().<CounterResponse>send(new CounterRequest(key, 100, -10, 0, bucket())).toBlocking().single();
        assertEquals(90, response2.value());

        CounterResponse response3 = cluster().<CounterResponse>send(new CounterRequest(key, 100, -10, 0, bucket())).toBlocking().single();
        assertEquals(80, response3.value());

        assertTrue(response1.cas() != response2.cas());
        assertTrue(response2.cas() != response3.cas());

        assertMetadataSequence(response1.mutationToken(), response2.mutationToken());
        assertMetadataSequence(response2.mutationToken(), response3.mutationToken());
    }

    @Test
    public void shouldGetAndTouch() throws Exception {
        String key = "get-and-touch";

        UpsertRequest request = new UpsertRequest(key, Unpooled.copiedBuffer("content", CharsetUtil.UTF_8), 3, 0, bucket());
        UpsertResponse response = cluster().<UpsertResponse>send(request).toBlocking().single();
        assertEquals(ResponseStatus.SUCCESS, response.status());
        ReferenceCountUtil.releaseLater(response.content());

        Thread.sleep(2000);

        GetResponse getResponse = cluster().<GetResponse>send(new GetRequest(key, bucket(), false, true, 3)).toBlocking().single();
        assertEquals(ResponseStatus.SUCCESS, getResponse.status());
        assertEquals("content", getResponse.content().toString(CharsetUtil.UTF_8));
        ReferenceCountUtil.releaseLater(getResponse.content());

        Thread.sleep(2000);

        getResponse = cluster().<GetResponse>send(new GetRequest(key, bucket(), false, true, 3)).toBlocking().single();
        assertEquals(ResponseStatus.SUCCESS, getResponse.status());
        assertEquals("content", getResponse.content().toString(CharsetUtil.UTF_8));
        ReferenceCountUtil.releaseLater(getResponse.content());
    }

    @Test
    public void shouldGetAndLock() throws Exception {
        String key = "get-and-lock";

        UpsertRequest request = new UpsertRequest(key, Unpooled.copiedBuffer("content", CharsetUtil.UTF_8), bucket());
        UpsertResponse response = cluster().<UpsertResponse>send(request).toBlocking().single();
        assertEquals(ResponseStatus.SUCCESS, response.status());
        ReferenceCountUtil.releaseLater(response.content());

        GetResponse getResponse = cluster().<GetResponse>send(new GetRequest(key, bucket(), true, false, 2)).toBlocking().single();
        assertEquals(ResponseStatus.SUCCESS, getResponse.status());
        assertEquals("content", getResponse.content().toString(CharsetUtil.UTF_8));
        ReferenceCountUtil.releaseLater(getResponse.content());

        request = new UpsertRequest(key, Unpooled.copiedBuffer("content", CharsetUtil.UTF_8), bucket());
        response = cluster().<UpsertResponse>send(request).toBlocking().single();
        assertTrue(ResponseStatus.EXISTS == response.status() || ResponseStatus.LOCKED == response.status());
        ReferenceCountUtil.releaseLater(response.content());

        GetResponse secondLockResponse = (GetResponse) cluster().send(new GetRequest(key, bucket(), true, false, 2))
                .toBlocking().single();
        assertTrue(ResponseStatus.TEMPORARY_FAILURE == secondLockResponse.status() || ResponseStatus.LOCKED == secondLockResponse.status());
        ReferenceCountUtil.releaseLater(secondLockResponse.content());

        Thread.sleep(3000);

        request = new UpsertRequest(key, Unpooled.copiedBuffer("content", CharsetUtil.UTF_8), bucket());
        response = cluster().<UpsertResponse>send(request).toBlocking().single();
        ReferenceCountUtil.releaseLater(response.content());
        assertEquals(ResponseStatus.SUCCESS, response.status());
    }

    /**
     * Verificiation for MB-15727.
     *
     * This test is ignored in versions lower than 4.5 since thats the version where it has been fixed.
     */
    @Test
    public void shouldGetAndLockWithAppend() throws Exception {
        assumeMinimumVersionCompatible(4, 5);

        String key = "get-and-lock-append";

        UpsertRequest request = new UpsertRequest(key, Unpooled.copiedBuffer("foo", CharsetUtil.UTF_8), bucket());
        UpsertResponse response = cluster().<UpsertResponse>send(request).toBlocking().single();
        assertEquals(ResponseStatus.SUCCESS, response.status());
        ReferenceCountUtil.releaseLater(response.content());

        GetResponse getResponse = cluster().<GetResponse>send(new GetRequest(key, bucket(), true, false, 2)).toBlocking().single();
        assertEquals(ResponseStatus.SUCCESS, getResponse.status());
        assertEquals("foo", getResponse.content().toString(CharsetUtil.UTF_8));
        ReferenceCountUtil.releaseLater(getResponse.content());

        AppendResponse appendResponse = cluster().<AppendResponse>send(
            new AppendRequest(key, getResponse.cas(), Unpooled.copiedBuffer("bar", CharsetUtil.UTF_8), bucket())
        ).toBlocking().single();
        assertEquals(ResponseStatus.SUCCESS, getResponse.status());
        assertTrue(getResponse.cas() != appendResponse.cas());
        ReferenceCountUtil.releaseLater(appendResponse.content());

        getResponse = cluster().<GetResponse>send(new GetRequest(key, bucket(), false, false, 0)).toBlocking().single();
        assertEquals(ResponseStatus.SUCCESS, getResponse.status());
        assertEquals("foobar", getResponse.content().toString(CharsetUtil.UTF_8));
        ReferenceCountUtil.releaseLater(getResponse.content());
    }

    @Test
    public void shouldTouch() throws Exception {
        String key = "touch";

        UpsertRequest request = new UpsertRequest(key, Unpooled.copiedBuffer("content", CharsetUtil.UTF_8), 3, 0, bucket());
        UpsertResponse response = cluster().<UpsertResponse>send(request).toBlocking().single();
        assertEquals(ResponseStatus.SUCCESS, response.status());
        ReferenceCountUtil.releaseLater(response.content());

        Thread.sleep(2000);

        TouchResponse touchResponse = cluster().<TouchResponse>send(new TouchRequest(key, 3, bucket())).toBlocking().single();
        assertEquals(ResponseStatus.SUCCESS, touchResponse.status());
        ReferenceCountUtil.releaseLater(touchResponse.content());

        Thread.sleep(2000);

        GetResponse getResponse = cluster().<GetResponse>send(new GetRequest(key, bucket())).toBlocking().single();
        assertEquals(ResponseStatus.SUCCESS, getResponse.status());
        assertEquals("content", getResponse.content().toString(CharsetUtil.UTF_8));
        ReferenceCountUtil.releaseLater(getResponse.content());
    }

    @Test
    public void shouldUnlock() throws Exception {
        String key = "unlock";

        UpsertRequest request = new UpsertRequest(key, Unpooled.copiedBuffer("content", CharsetUtil.UTF_8), bucket());
        UpsertResponse response = cluster().<UpsertResponse>send(request).toBlocking().single();
        assertEquals(ResponseStatus.SUCCESS, response.status());
        ReferenceCountUtil.releaseLater(response.content());

        GetResponse getResponse = cluster().<GetResponse>send(new GetRequest(key, bucket(), true, false, 15)).toBlocking().single();
        assertEquals(ResponseStatus.SUCCESS, getResponse.status());
        assertEquals("content", getResponse.content().toString(CharsetUtil.UTF_8));
        ReferenceCountUtil.releaseLater(getResponse.content());

        request = new UpsertRequest(key, Unpooled.copiedBuffer("content", CharsetUtil.UTF_8), bucket());
        response = cluster().<UpsertResponse>send(request).toBlocking().single();
        assertTrue(ResponseStatus.EXISTS == response.status() || ResponseStatus.LOCKED == response.status());
        ReferenceCountUtil.releaseLater(response.content());

        UnlockRequest unlockRequest = new UnlockRequest(key, getResponse.cas(), bucket());
        UnlockResponse unlockResponse = cluster().<UnlockResponse>send(unlockRequest).toBlocking().single();
        assertEquals(ResponseStatus.SUCCESS, unlockResponse.status());
        ReferenceCountUtil.releaseLater(unlockResponse.content());

        request = new UpsertRequest(key, Unpooled.copiedBuffer("content", CharsetUtil.UTF_8), bucket());
        response = cluster().<UpsertResponse>send(request).toBlocking().single();
        assertEquals(ResponseStatus.SUCCESS, response.status());
        ReferenceCountUtil.releaseLater(response.content());
    }

    @Test
    public void shouldHandleSpecialKeyChars() {
        String key = "AVERY® READY INDEX®";
        String content = "Hello World!";
        UpsertRequest upsert = new UpsertRequest(key, Unpooled.copiedBuffer(content, CharsetUtil.UTF_8), bucket());
        UpsertResponse response = cluster().<UpsertResponse>send(upsert).toBlocking().single();
        ReferenceCountUtil.releaseLater(response.content());

        GetRequest request = new GetRequest(key, bucket());
        GetResponse getResponse = cluster().<GetResponse>send(request).toBlocking().single();
        assertEquals(content, getResponse.content().toString(CharsetUtil.UTF_8));
        ReferenceCountUtil.releaseLater(getResponse.content());
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldRejectEmptyKey() {
        cluster().<GetResponse>send(new GetRequest("", bucket())).toBlocking().single();
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldRejectNullKey() {
        cluster().<GetResponse>send(new GetRequest(null, bucket())).toBlocking().single();
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldRejectTooLongKey() {
        char[] array = new char[251];
        Arrays.fill(array, 'a');
        cluster().<GetResponse>send(new GetRequest(new String(array), bucket())).toBlocking().single();
    }

    /**
     * Helper method to assert if the mutation metadata is correct.
     *
     * Note that if mutation metadata is disabled, null is expected.
     *
     * @param token the token to check
     * @throws Exception
     */
    private void assertValidMetadata(MutationToken token) throws Exception {
        if (isMutationMetadataEnabled()) {
            assertNotNull(token);
            assertTrue(token.sequenceNumber() > 0);
            assertTrue(token.vbucketUUID() != 0);
            assertTrue(token.vbucketID() > 0);
            assertTrue(token.bucket() != null && token.bucket().equals(bucket()));
        } else {
            assertNull(token);
        }
    }

    /**
     * Helper method to make sure that two consecutive sequences are valid.
     *
     * They are valid if the vbucket uuid is the same and the sequence is higher by one.
     *
     * @param first the first mutation
     * @param second the second mutation
     * @throws Exception
     */
    private void assertMetadataSequence(MutationToken first, MutationToken second) throws Exception {
        if (isMutationMetadataEnabled()) {
            assertNotNull(first);
            assertNotNull(second);
            assertTrue(first.vbucketUUID() != 0);
            assertTrue(first.vbucketID() > 0);
            assertTrue(second.vbucketID() > 0);
            assertTrue(first.bucket() != null && first.bucket().equals(bucket()));
            assertEquals(first.bucket(), second.bucket());
            assertEquals(first.vbucketUUID(), second.vbucketUUID());
            assertTrue((first.sequenceNumber()+1) == second.sequenceNumber());
            assertEquals(first.vbucketID(), second.vbucketID());
        } else {
            assertNull(first);
            assertNull(second);
        }
    }

}
