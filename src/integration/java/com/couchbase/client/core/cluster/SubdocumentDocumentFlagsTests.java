/*
 * Copyright (c) 2017 Couchbase, Inc.
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
import com.couchbase.client.core.message.kv.RemoveRequest;
import com.couchbase.client.core.message.kv.RemoveResponse;
import com.couchbase.client.core.message.kv.subdoc.multi.Lookup;
import com.couchbase.client.core.message.kv.subdoc.multi.LookupCommandBuilder;
import com.couchbase.client.core.message.kv.subdoc.multi.MultiLookupResponse;
import com.couchbase.client.core.message.kv.subdoc.multi.SubMultiLookupDocOptionsBuilder;
import com.couchbase.client.core.message.kv.subdoc.multi.SubMultiLookupRequest;
import com.couchbase.client.core.message.kv.subdoc.simple.SimpleSubdocResponse;
import com.couchbase.client.core.message.kv.subdoc.simple.SubDictAddRequest;
import com.couchbase.client.core.message.kv.subdoc.simple.SubGetRequest;
import com.couchbase.client.core.util.ClusterDependentTest;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.util.CharsetUtil;
import io.netty.util.ReferenceCountUtil;
import io.netty.util.ResourceLeakDetector;
import org.junit.BeforeClass;
import org.junit.Test;

import static junit.framework.TestCase.assertFalse;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Tests document flags for subdocument operations added in Couchbase server 5.0
 *
 * @author Subhashni Balakrishnan
 * @since 2.4.7
 */
public class SubdocumentDocumentFlagsTests extends ClusterDependentTest {

    static {
        ResourceLeakDetector.setLevel(ResourceLeakDetector.Level.PARANOID);
    }

    @BeforeClass
    public static void checkExtendedAttributeAvailable() throws Exception {
        connect(false);
        assumeMinimumVersionCompatible(5, 0);
    }


    @Test
    public void shouldCreateDocumentIfSet() {
        String subPath = "hello";
        ByteBuf fragment = Unpooled.copiedBuffer("\"world\"", CharsetUtil.UTF_8);
        ReferenceCountUtil.releaseLater(fragment);

        SubDictAddRequest insertRequest = new SubDictAddRequest("shouldCreateDocumentIfSet", subPath, fragment, bucket());
        insertRequest.createDocument(true);
        SimpleSubdocResponse insertResponse = cluster().<SimpleSubdocResponse>send(insertRequest).toBlocking().single();
        ReferenceCountUtil.releaseLater(insertResponse.content());
        assertTrue(insertResponse.status().isSuccess());
        RemoveResponse response = cluster().<RemoveResponse>send(new RemoveRequest("shouldCreateDocumentIfSet", bucket())).toBlocking().single();
        assert(response.status() == ResponseStatus.SUCCESS);
    }

    @Test
    public void shouldCreateDocumentIfSetWithExpiryAndPathFlags() {
        String subPath = "first.hello";
        ByteBuf fragment = Unpooled.copiedBuffer("\"world\"", CharsetUtil.UTF_8);
        ReferenceCountUtil.releaseLater(fragment);
        SubDictAddRequest insertRequest = new SubDictAddRequest("shouldCreateDocumentIfSetWithExpiryAndPathFlags", subPath, fragment, bucket(), 10, 0);
        insertRequest.createDocument(true);
        insertRequest.createIntermediaryPath(true);
        SimpleSubdocResponse insertResponse = cluster().<SimpleSubdocResponse>send(insertRequest).toBlocking().single();
        ReferenceCountUtil.releaseLater(insertResponse.content());
        assertTrue(insertResponse.status().isSuccess());
        RemoveResponse response = cluster().<RemoveResponse>send(new RemoveRequest("shouldCreateDocumentIfSetWithExpiryAndPathFlags", bucket())).toBlocking().single();
        assertEquals(response.status(), ResponseStatus.SUCCESS);
    }

    @Test
    public void shouldFailIfCreateDocumentIsNotSetWhenDocumentDoesNotExist() {
        String subPath = "hello";
        ByteBuf fragment = Unpooled.copiedBuffer("\"world\"", CharsetUtil.UTF_8);
        ReferenceCountUtil.releaseLater(fragment);
        SubDictAddRequest insertRequest = new SubDictAddRequest("shouldFailIfCreateDocumentIsNotSetWhenDocumentDoesNotExist", subPath, fragment, bucket());
        SimpleSubdocResponse insertResponse = cluster().<SimpleSubdocResponse>send(insertRequest).toBlocking().single();
        ReferenceCountUtil.releaseLater(insertResponse.content());
        assertEquals(insertResponse.status(), ResponseStatus.NOT_EXISTS);
    }

    @Test
    public void shouldInsertDocumentIfSet() {
        String subPath = "hello";
        ByteBuf fragment = Unpooled.copiedBuffer("\"world\"", CharsetUtil.UTF_8);
        ReferenceCountUtil.releaseLater(fragment);

        SubDictAddRequest insertRequest = new SubDictAddRequest("shouldInsertDocumentIfSet", subPath, fragment, bucket());
        insertRequest.insertDocument(true);
        SimpleSubdocResponse insertResponse = cluster().<SimpleSubdocResponse>send(insertRequest).toBlocking().single();
        ReferenceCountUtil.releaseLater(insertResponse.content());
        assertTrue(insertResponse.status().isSuccess());
        RemoveResponse response = cluster().<RemoveResponse>send(new RemoveRequest("shouldInsertDocumentIfSet", bucket())).toBlocking().single();
        assertEquals(response.status(), ResponseStatus.SUCCESS);
    }

    @Test
    public void shouldFailOnInsertDocumentIfSetOnDocExists() {
        String subPath = "hello";
        ByteBuf fragment = Unpooled.copiedBuffer("\"world\"", CharsetUtil.UTF_8);
        ReferenceCountUtil.releaseLater(fragment);

        SubDictAddRequest insertRequest = new SubDictAddRequest("shouldFailOnInsertDocumentIfSetOnDocExists", subPath, fragment, bucket());
        insertRequest.insertDocument(true);
        SimpleSubdocResponse insertResponse = cluster().<SimpleSubdocResponse>send(insertRequest).toBlocking().single();
        ReferenceCountUtil.releaseLater(insertResponse.content());
        assertTrue(insertResponse.status().isSuccess());
        SubDictAddRequest insertRequest2 = new SubDictAddRequest("shouldFailOnInsertDocumentIfSetOnDocExists", subPath, Unpooled.EMPTY_BUFFER, bucket());
        insertRequest2.insertDocument(false);
        SimpleSubdocResponse insertResponse2 = cluster().<SimpleSubdocResponse>send(insertRequest2).toBlocking().single();
        ReferenceCountUtil.releaseLater(insertResponse2.content());
        assertFalse(insertResponse2.status().isSuccess());
        RemoveResponse removeResponse = cluster().<RemoveResponse>send(new RemoveRequest("shouldFailOnInsertDocumentIfSetOnDocExists", bucket())).toBlocking().single();
        assertEquals(removeResponse.status(), ResponseStatus.SUCCESS);
    }

    @Test
    public void shouldAccessDeletedDocumentIfSet() {
        String subPath = "_hello";
        ByteBuf fragment = Unpooled.copiedBuffer("\"world\"", CharsetUtil.UTF_8);
        ReferenceCountUtil.releaseLater(fragment);

        SubDictAddRequest insertRequest = new SubDictAddRequest("shouldAccessDeletedDocumentIfSet", subPath, fragment, bucket());
        insertRequest.createDocument(true);
        insertRequest.xattr(true);
        insertRequest.createIntermediaryPath(true);
        SimpleSubdocResponse insertResponse = cluster().<SimpleSubdocResponse>send(insertRequest).toBlocking().single();
        ReferenceCountUtil.releaseLater(insertResponse.content());
        assertTrue(insertResponse.status().isSuccess());
        RemoveResponse response = cluster().<RemoveResponse>send(new RemoveRequest("shouldAccessDeletedDocumentIfSet", bucket())).toBlocking().single();
        assertEquals(response.status(), ResponseStatus.SUCCESS);

        SubGetRequest getRequest = new SubGetRequest("shouldAccessDeletedDocumentIfSet", subPath, bucket());
        getRequest.xattr(true);
        getRequest.accessDeleted(true);
        SimpleSubdocResponse getResponse = cluster().<SimpleSubdocResponse>send(getRequest).toBlocking().single();
        assertEquals(getResponse.status(), ResponseStatus.SUCCESS);
    }

    @Test
    public void shouldAccessDeletedDocumentIfSetInMultiPath() {
        String subPath = "_class";
        ByteBuf fragment = Unpooled.copiedBuffer("\"test\"", CharsetUtil.UTF_8);
        ReferenceCountUtil.releaseLater(fragment);

        SubDictAddRequest insertRequest = new SubDictAddRequest("shouldAccessDeletedDocumentIfSetInMultiPath", subPath, fragment, bucket());
        insertRequest.insertDocument(true);
        insertRequest.xattr(true);
        SimpleSubdocResponse insertResponse = cluster().<SimpleSubdocResponse>send(insertRequest).toBlocking().single();
        ReferenceCountUtil.releaseLater(insertResponse.content());
        assertTrue(insertResponse.status().isSuccess());
        RemoveResponse response = cluster().<RemoveResponse>send(new RemoveRequest("shouldAccessDeletedDocumentIfSetInMultiPath", bucket())).toBlocking().single();
        assertEquals(response.status(), ResponseStatus.SUCCESS);

        SubMultiLookupRequest lookupRequest = new SubMultiLookupRequest("shouldAccessDeletedDocumentIfSetInMultiPath", bucket(),
                SubMultiLookupDocOptionsBuilder.builder().accessDeleted(true),
                new LookupCommandBuilder(Lookup.GET, "_class")
                        .xattr(true).build());
        MultiLookupResponse lookupResponse = cluster().<MultiLookupResponse>send(lookupRequest).toBlocking().single();
        assertTrue(lookupResponse.status().isSuccess());
    }

}