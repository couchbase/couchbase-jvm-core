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
import com.couchbase.client.core.message.kv.subdoc.simple.SimpleSubdocResponse;
import com.couchbase.client.core.message.kv.subdoc.simple.SubDictAddRequest;
import com.couchbase.client.core.util.ClusterDependentTest;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.util.CharsetUtil;
import io.netty.util.ReferenceCountUtil;
import io.netty.util.ResourceLeakDetector;
import org.junit.BeforeClass;
import org.junit.Test;
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
        assert(response.status() == ResponseStatus.SUCCESS);
    }

    @Test
    public void shouldFailIfCreateDocumentIsNotSetWhenDocumentDoesNotExist() {
        String subPath = "hello";
        ByteBuf fragment = Unpooled.copiedBuffer("\"world\"", CharsetUtil.UTF_8);
        ReferenceCountUtil.releaseLater(fragment);
        SubDictAddRequest insertRequest = new SubDictAddRequest("shouldFailIfCreateDocumentIsNotSetWhenDocumentDoesNotExist", subPath, fragment, bucket());
        SimpleSubdocResponse insertResponse = cluster().<SimpleSubdocResponse>send(insertRequest).toBlocking().single();
        ReferenceCountUtil.releaseLater(insertResponse.content());
        assert(insertResponse.status() == ResponseStatus.NOT_EXISTS);
    }

}
