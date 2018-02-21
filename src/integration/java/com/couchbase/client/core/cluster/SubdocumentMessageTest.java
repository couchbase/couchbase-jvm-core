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
import com.couchbase.client.core.message.kv.GetRequest;
import com.couchbase.client.core.message.kv.GetResponse;
import com.couchbase.client.core.message.kv.MutationToken;
import com.couchbase.client.core.message.kv.RemoveRequest;
import com.couchbase.client.core.message.kv.RemoveResponse;
import com.couchbase.client.core.message.kv.UpsertRequest;
import com.couchbase.client.core.message.kv.UpsertResponse;
import com.couchbase.client.core.message.kv.subdoc.multi.Lookup;
import com.couchbase.client.core.message.kv.subdoc.multi.LookupCommandBuilder;
import com.couchbase.client.core.message.kv.subdoc.multi.MultiLookupResponse;
import com.couchbase.client.core.message.kv.subdoc.multi.MultiMutationResponse;
import com.couchbase.client.core.message.kv.subdoc.multi.MultiResult;
import com.couchbase.client.core.message.kv.subdoc.multi.Mutation;
import com.couchbase.client.core.message.kv.subdoc.multi.MutationCommand;
import com.couchbase.client.core.message.kv.subdoc.multi.MutationCommandBuilder;
import com.couchbase.client.core.message.kv.subdoc.multi.SubMultiLookupRequest;
import com.couchbase.client.core.message.kv.subdoc.multi.SubMultiMutationRequest;
import com.couchbase.client.core.message.kv.subdoc.simple.AbstractSubdocRequest;
import com.couchbase.client.core.message.kv.subdoc.simple.SimpleSubdocResponse;
import com.couchbase.client.core.message.kv.subdoc.simple.SubArrayRequest;
import com.couchbase.client.core.message.kv.subdoc.simple.SubCounterRequest;
import com.couchbase.client.core.message.kv.subdoc.simple.SubDeleteRequest;
import com.couchbase.client.core.message.kv.subdoc.simple.SubDictAddRequest;
import com.couchbase.client.core.message.kv.subdoc.simple.SubDictUpsertRequest;
import com.couchbase.client.core.message.kv.subdoc.simple.SubExistRequest;
import com.couchbase.client.core.message.kv.subdoc.simple.SubGetRequest;
import com.couchbase.client.core.message.kv.subdoc.simple.SubReplaceRequest;
import com.couchbase.client.core.util.ClusterDependentTest;
import com.couchbase.client.core.utils.DefaultObjectMapper;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.util.CharsetUtil;
import io.netty.util.ReferenceCountUtil;
import io.netty.util.ResourceLeakDetector;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Verifies basic functionality of binary subdocument operations.
 *
 * @author Simon Baslé
 * @since 1.2
 */
public class SubdocumentMessageTest extends ClusterDependentTest {

    static {
        ResourceLeakDetector.setLevel(ResourceLeakDetector.Level.PARANOID);
    }

    /** Use this key when you want previous data to exist in your test*/
    private static final String testSubKey = "testSubKey";

    /** Use this key when you want to create data in your test, to be cleaned up after*/
    private static final String testInsertionSubKey = "testInsertionSubKey";

    /** Use this key when you want existing data that is simply a JSON array at the root*/
    private static final String testArrayRoot = "testArrayRootSubKey";

    /** Use this key when you want existing data that include a deep complex array*/
    private static final String testComplexSubArray = "testComplexSubArray";

    private static final String jsonContent = "{\"value\":\"stringValue\", \"sub\": {\"value\": \"subStringValue\",\"array\": [\"array1\", 2, true]}}";
    private static final String jsonArrayContent = "[\"v1\"]";
    private static final String jsonComplexSubArrayContent = "{\"value\":\"stringValue\", \"complexArray\": [{\"v1\": 123}]}";

    @BeforeClass
    public static void checkSubdocAvailable() throws Exception {
        connect();
        assumeMinimumVersionCompatible(4, 5);
    }

    @Before
    public void prepareData() {
        UpsertRequest upsert = new UpsertRequest(testSubKey, Unpooled.copiedBuffer(jsonContent, CharsetUtil.UTF_8), bucket(), true);
        UpsertResponse response = cluster().<UpsertResponse>send(upsert).toBlocking().single();
        ReferenceCountUtil.releaseLater(response.content());
        assertTrue("Couldn't insert " + testSubKey, response.status().isSuccess());

        upsert = new UpsertRequest(testArrayRoot, Unpooled.copiedBuffer(jsonArrayContent, CharsetUtil.UTF_8), bucket(), true);
        response = cluster().<UpsertResponse>send(upsert).toBlocking().single();
        ReferenceCountUtil.releaseLater(response.content());
        assertTrue("Couldn't insert " + testArrayRoot, response.status().isSuccess());

        upsert = new UpsertRequest(testComplexSubArray, Unpooled.copiedBuffer(jsonComplexSubArrayContent, CharsetUtil.UTF_8), bucket(), true);
        response = cluster().<UpsertResponse>send(upsert).toBlocking().single();
        ReferenceCountUtil.releaseLater(response.content());
        assertTrue("Couldn't insert " + testComplexSubArray, response.status().isSuccess());
    }

    @After
    public void deleteData() {
        RemoveRequest remove = new RemoveRequest(testSubKey, bucket());
        RemoveResponse response = cluster().<RemoveResponse>send(remove).toBlocking().single();
        boolean removedSubkey = response.status().isSuccess();
        ReferenceCountUtil.releaseLater(response.content());

        remove = new RemoveRequest(testInsertionSubKey, bucket());
        response = cluster().<RemoveResponse>send(remove).toBlocking().single();
        boolean removedInsertionSubkey = response.status().isSuccess() || response.status().equals(ResponseStatus.NOT_EXISTS);
        ReferenceCountUtil.releaseLater(response.content());

        remove = new RemoveRequest(testArrayRoot, bucket());
        response = cluster().<RemoveResponse>send(remove).toBlocking().single();
        boolean removedArraySubkey = response.status().isSuccess();
        ReferenceCountUtil.releaseLater(response.content());

        remove = new RemoveRequest(testComplexSubArray, bucket());
        response = cluster().<RemoveResponse>send(remove).toBlocking().single();
        boolean removeComplexSubArray = response.status().isSuccess();
        ReferenceCountUtil.releaseLater(response.content());

        assertTrue("Couldn't remove " + testSubKey, removedSubkey);
        assertTrue("Couldn't remove " + testInsertionSubKey, removedInsertionSubkey);
        assertTrue("Couldn't remove " + testArrayRoot, removedArraySubkey);
        assertTrue("Couldn't remove " + testComplexSubArray, removeComplexSubArray);
    }

    @Test
    public void shouldThrowNullPointerIfPathIsNull() {
        boolean npeOnGet = false;
        boolean npeOnMutation = false;
        try {
            new SubGetRequest(testSubKey, null, bucket());
        } catch (NullPointerException e) {
            npeOnGet = true;
        }

        try {
            new SubReplaceRequest(testSubKey, null, Unpooled.copiedBuffer("test", CharsetUtil.UTF_8), bucket());
        } catch (NullPointerException e) {
            npeOnMutation = true;
        }

        assertTrue("Expected null path get to fail with NPE", npeOnGet);
        assertTrue("Expected null path mutation to fail with NPE", npeOnMutation);
    }

    @Test
    public void shouldSubGetValueInExistingDocumentObject() throws Exception{
        String subValuePath = "sub.value";

        SubGetRequest valueRequest = new SubGetRequest(testSubKey, subValuePath, bucket());
        SimpleSubdocResponse valueResponse = cluster().<SimpleSubdocResponse>send(valueRequest).toBlocking().single();
        String raw = valueResponse.content().toString(CharsetUtil.UTF_8);
        ReferenceCountUtil.releaseLater(valueResponse.content());
        assertNotNull(raw);
        assertEquals("\"subStringValue\"", raw);
    }

    @Test
    public void shouldSubGetValueInExistingDocumentArray() throws Exception{
        String subArrayPath = "sub.array[1]";

        SubGetRequest arrayRequest = new SubGetRequest(testSubKey, subArrayPath, bucket());
        SimpleSubdocResponse arrayResponse = cluster().<SimpleSubdocResponse>send(arrayRequest).toBlocking().single();
        String raw = arrayResponse.content().toString(CharsetUtil.UTF_8);
        ReferenceCountUtil.releaseLater(arrayResponse.content());
        assertNotNull(raw);
        assertEquals("2", raw);
    }

    @Test
    public void shouldFailSubGetIfPathDoesntExist() {
        String wrongPath = "sub.arra[1]";

        SubGetRequest badRequest = new SubGetRequest(testSubKey, wrongPath, bucket());
        SimpleSubdocResponse response = cluster().<SimpleSubdocResponse>send(badRequest).toBlocking().single();
        ReferenceCountUtil.releaseLater(response.content());

        assertFalse(response.status().isSuccess());
        assertEquals(ResponseStatus.SUBDOC_PATH_NOT_FOUND, response.status());
    }

    @Test
    public void shouldSucceedExistOnDictionnaryPath() {
        String path = "sub.value";

        SubExistRequest request = new SubExistRequest(testSubKey, path, bucket());
        SimpleSubdocResponse response = cluster().<SimpleSubdocResponse>send(request).toBlocking().single();

        assertEquals(ResponseStatus.SUCCESS, response.status());
    }

    @Test
    public void shouldSucceedExistOnArrayPath() {
        String path = "sub.array[1]";

        SubExistRequest request = new SubExistRequest(testSubKey, path, bucket());
        SimpleSubdocResponse response = cluster().<SimpleSubdocResponse>send(request).toBlocking().single();

        assertEquals(ResponseStatus.SUCCESS, response.status());
    }

    @Test
    public void shouldReturnPathNotFoundOnExistWithBadPath() {
        String path = "sub.bad";

        SubExistRequest request = new SubExistRequest(testSubKey, path, bucket());
        SimpleSubdocResponse response = cluster().<SimpleSubdocResponse>send(request).toBlocking().single();

        assertEquals(ResponseStatus.SUBDOC_PATH_NOT_FOUND, response.status());;
    }

    @Test
    public void testExistDoesntContainValueOnSuccess() {
        String path = "sub.array[1]";

        SubExistRequest request = new SubExistRequest(testSubKey, path, bucket());
        SimpleSubdocResponse response = cluster().<SimpleSubdocResponse>send(request).toBlocking().single();

        assertEquals(ResponseStatus.SUCCESS, response.status());
        assertEquals(0, response.content().readableBytes());
    }

    @Test
    public void shouldDictUpsertInExistingDocumentObject() {
        String subPath = "sub.value";
        ByteBuf fragment = Unpooled.copiedBuffer("\"mutated\"", CharsetUtil.UTF_8);
        ReferenceCountUtil.releaseLater(fragment);

        //verify initial state
        assertMutation(testSubKey, jsonContent);

        //mutate
        SubDictUpsertRequest upsertRequest = new SubDictUpsertRequest(testSubKey, subPath, fragment, bucket());
        SimpleSubdocResponse upsertResponse = cluster().<SimpleSubdocResponse>send(upsertRequest).toBlocking().single();
        ReferenceCountUtil.releaseLater(upsertResponse.content());
        assertTrue(upsertResponse.status().isSuccess());

        //verify mutated state
        assertMutation(testSubKey, jsonContent.replace("subStringValue", "mutated"));
    }

    @Test
    public void shouldReturnPathInvalidOnDictUpsertInArray() {
        String subPath = "sub.array[1]";
        ByteBuf fragment = Unpooled.copiedBuffer("\"mutated\"", CharsetUtil.UTF_8);
        ReferenceCountUtil.releaseLater(fragment);

        //mutate
        SubDictUpsertRequest upsertRequest = new SubDictUpsertRequest(testSubKey, subPath, fragment, bucket());
        SimpleSubdocResponse upsertResponse = cluster().<SimpleSubdocResponse>send(upsertRequest).toBlocking().single();
        ReferenceCountUtil.releaseLater(upsertResponse.content());
        assertFalse(upsertResponse.status().isSuccess());
        assertEquals(0, upsertResponse.content().readableBytes());
        assertEquals(ResponseStatus.SUBDOC_PATH_INVALID, upsertResponse.status());
    }

    @Test
    public void shouldGetMutationTokenWithMutation() throws Exception {
        String subPath = "sub.value";
        ByteBuf fragment = Unpooled.copiedBuffer("\"mutated\"", CharsetUtil.UTF_8);
        ReferenceCountUtil.releaseLater(fragment);

        SubDictUpsertRequest upsertRequest = new SubDictUpsertRequest(testSubKey, subPath, fragment, bucket());
        SimpleSubdocResponse upsertResponse = cluster().<SimpleSubdocResponse>send(upsertRequest).toBlocking().single();
        ReferenceCountUtil.releaseLater(upsertResponse.content());
        assertValidMetadata(upsertResponse.mutationToken());
    }

    @Test
    public void shouldNotGetMutationTokenWithGet() throws Exception {
        String subPath = "sub.value";
        ByteBuf fragment = Unpooled.copiedBuffer("\"mutated\"", CharsetUtil.UTF_8);
        ReferenceCountUtil.releaseLater(fragment);

        SubGetRequest getRequest = new SubGetRequest(testSubKey, subPath, bucket());
        SimpleSubdocResponse response = cluster().<SimpleSubdocResponse>send(getRequest).toBlocking().single();
        ReferenceCountUtil.releaseLater(response.content());
        assertNull(response.mutationToken());
    }

    @Test
    public void shouldDictAddOnSubObject() {
        String subPath = "sub.otherValue";
        ByteBuf fragment = Unpooled.copiedBuffer("\"inserted\"", CharsetUtil.UTF_8);
        ReferenceCountUtil.releaseLater(fragment);

        //mutate
        SubDictAddRequest insertRequest = new SubDictAddRequest(testSubKey, subPath, fragment, bucket());
        SimpleSubdocResponse insertResponse = cluster().<SimpleSubdocResponse>send(insertRequest).toBlocking().single();
        ReferenceCountUtil.releaseLater(insertResponse.content());
        assertTrue(insertResponse.status().isSuccess());
        assertEquals(0, insertResponse.content().readableBytes());

        //check the insertion at the end of "sub" object
        String expected = "{\"value\":\"stringValue\", \"sub\": {\"value\": \"subStringValue\",\"array\": [\"array1\", 2, true]" +
                ",\"otherValue\":\"inserted\"}}";
        assertMutation(testSubKey, expected);
    }

    @Test
    public void shouldReturnPathExistOnDictAddOnSubValue() {
        String subPath = "sub.value";
        ByteBuf fragment = Unpooled.copiedBuffer("\"mutated\"", CharsetUtil.UTF_8);
        ReferenceCountUtil.releaseLater(fragment);

        //mutate
        SubDictAddRequest insertRequest = new SubDictAddRequest(testSubKey, subPath, fragment, bucket());
        SimpleSubdocResponse insertResponse = cluster().<SimpleSubdocResponse>send(insertRequest).toBlocking().single();
        ReferenceCountUtil.releaseLater(insertResponse.content());
        assertFalse(insertResponse.status().isSuccess());
        assertEquals(0, insertResponse.content().readableBytes());
        assertEquals(ResponseStatus.SUBDOC_PATH_EXISTS, insertResponse.status());
    }

    @Test
    public void shouldReturnPathNotFoundOnDictAddForNewDeepPath() {
        String subPath = "sub2.value";
        ByteBuf fragment = Unpooled.copiedBuffer("\"insertedPath\"", CharsetUtil.UTF_8);
        ReferenceCountUtil.releaseLater(fragment);

        //mutate
        SubDictAddRequest insertRequest = new SubDictAddRequest(testSubKey, subPath, fragment, bucket());
        assertFalse(insertRequest.createIntermediaryPath());

        SimpleSubdocResponse insertResponse = cluster().<SimpleSubdocResponse>send(insertRequest).toBlocking().single();
        ReferenceCountUtil.releaseLater(insertResponse.content());
        assertFalse(insertResponse.status().isSuccess());
        assertEquals(0, insertResponse.content().readableBytes());
        assertEquals(ResponseStatus.SUBDOC_PATH_NOT_FOUND, insertResponse.status());
    }

    @Test
    public void shouldReturnPathInvalidOnDictAddForArrayPath() {
        String subPath = "sub.array[1]";
        ByteBuf fragment = Unpooled.copiedBuffer("\"insertedPath\"", CharsetUtil.UTF_8);
        ReferenceCountUtil.releaseLater(fragment);

        //mutate
        SubDictAddRequest insertRequest = new SubDictAddRequest(testSubKey, subPath, fragment, bucket());
        assertFalse(insertRequest.createIntermediaryPath());

        SimpleSubdocResponse insertResponse = cluster().<SimpleSubdocResponse>send(insertRequest).toBlocking().single();
        ReferenceCountUtil.releaseLater(insertResponse.content());
        assertFalse(insertResponse.status().isSuccess());
        assertEquals(0, insertResponse.content().readableBytes());
        assertEquals(ResponseStatus.SUBDOC_PATH_INVALID, insertResponse.status());
    }

    @Test
    public void shouldCreateIntermediaryNodesOnDictAddForNewDeepPathIfForced() {
        String subPath = "sub2.value";
        ByteBuf fragment = Unpooled.copiedBuffer("\"insertedPath\"", CharsetUtil.UTF_8);
        ReferenceCountUtil.releaseLater(fragment);

        //mutate
        SubDictAddRequest insertRequest = new SubDictAddRequest(testSubKey, subPath, fragment, bucket());
        insertRequest.createIntermediaryPath(true);

        SimpleSubdocResponse insertResponse = cluster().<SimpleSubdocResponse>send(insertRequest).toBlocking().single();
        ReferenceCountUtil.releaseLater(insertResponse.content());
        assertTrue(insertResponse.status().isSuccess());
        assertEquals(0, insertResponse.content().readableBytes());
    }

    @Test
    public void shouldNotCreateIntermediaryNodesOnDictAddForNewDeepPathByDefault() {
        String subPath = "sub2.value";
        ByteBuf fragment = Unpooled.copiedBuffer("\"insertedPath\"", CharsetUtil.UTF_8);
        ReferenceCountUtil.releaseLater(fragment);

        SubDictAddRequest insertRequest = new SubDictAddRequest(testSubKey, subPath, fragment, bucket());
        SimpleSubdocResponse insertResponse = cluster().<SimpleSubdocResponse>send(insertRequest).toBlocking().single();
        ReferenceCountUtil.releaseLater(insertResponse.content());

        assertFalse(insertResponse.status().isSuccess());
        assertEquals(0, insertResponse.content().readableBytes());
        assertEquals(ResponseStatus.SUBDOC_PATH_NOT_FOUND, insertResponse.status());
    }

    @Test
    public void shouldDeleteExistingObjectPath() {
        String path = "sub.value";

        SubDeleteRequest request = new SubDeleteRequest(testSubKey, path, bucket());
        SimpleSubdocResponse response = cluster().<SimpleSubdocResponse>send(request).toBlocking().single();
        ReferenceCountUtil.releaseLater(response.content());

        assertTrue(response.status().isSuccess());
        assertEquals(0, response.content().readableBytes());
        assertEquals(ResponseStatus.SUCCESS, response.status());

        //assert the mutation
        String expected = "{\"value\":\"stringValue\", \"sub\": {\"array\": [\"array1\", 2, true]}}";
        assertMutation(testSubKey, expected);
    }

    @Test
    public void shouldDeleteExistingArrayIndexPath() {
        String path = "sub.array[1]";
        SubDeleteRequest request = new SubDeleteRequest(testSubKey, path, bucket());
        SimpleSubdocResponse response = cluster().<SimpleSubdocResponse>send(request).toBlocking().single();
        ReferenceCountUtil.releaseLater(response.content());

        assertTrue(response.status().isSuccess());
        assertEquals(0, response.content().readableBytes());
        assertEquals(ResponseStatus.SUCCESS, response.status());

        assertMutation(testSubKey, "{\"value\":\"stringValue\", \"sub\": {\"value\": \"subStringValue\",\"array\": [\"array1\", true]}}");
    }

    @Test
    public void shouldDeleteExistingWholeArrayPath() {
        String path = "sub.array";
        SubDeleteRequest request = new SubDeleteRequest(testSubKey, path, bucket());
        SimpleSubdocResponse response = cluster().<SimpleSubdocResponse>send(request).toBlocking().single();
        ReferenceCountUtil.releaseLater(response.content());

        assertTrue(response.status().isSuccess());
        assertEquals(0, response.content().readableBytes());
        assertEquals(ResponseStatus.SUCCESS, response.status());

        //assert the mutation
        String expected = "{\"value\":\"stringValue\", \"sub\": {\"value\": \"subStringValue\"}}";
        assertMutation(testSubKey, expected);
    }

    @Test
    public void shouldReturnPathNotFoundOnDeleteNonexistingPath() {
        String path = "sub.bad";
        SubDeleteRequest request = new SubDeleteRequest(testSubKey, path, bucket());
        SimpleSubdocResponse response = cluster().<SimpleSubdocResponse>send(request).toBlocking().single();
        ReferenceCountUtil.releaseLater(response.content());

        assertFalse(response.status().isSuccess());
        assertEquals(0, response.content().readableBytes());
        assertEquals(ResponseStatus.SUBDOC_PATH_NOT_FOUND, response.status());
    }

    @Test
    public void shouldThrowIllegalArgumentExceptionOnOperationsNotPermittingEmptyPath() {
        String path = "";
        ByteBuf fragment = Unpooled.EMPTY_BUFFER;
        String operation;

        operation = "GET";
        try {
            new SubGetRequest(testSubKey, path, bucket());
            fail("Expected IllegalArgumentException for " + operation);
        } catch (IllegalArgumentException e) {
            assertSame("Expected emtpy path IllegalArgumentException for " + operation, AbstractSubdocRequest.EXCEPTION_EMPTY_PATH, e);
        }


        operation = "EXIST";
        try {
            new SubExistRequest(testSubKey, path, bucket());
            fail("Expected IllegalArgumentException for " + operation);
        } catch (IllegalArgumentException e) {
            assertSame("Expected emtpy path IllegalArgumentException for " + operation, AbstractSubdocRequest.EXCEPTION_EMPTY_PATH, e);
        }


        operation = "DELETE";
        try {
            new SubDeleteRequest(testSubKey, path, bucket());
            fail("Expected IllegalArgumentException for " + operation);
        } catch (IllegalArgumentException e) {
            assertSame("Expected emtpy path IllegalArgumentException for " + operation, AbstractSubdocRequest.EXCEPTION_EMPTY_PATH, e);
        }


        operation = "ARRAY INSERT";
        try {
            new SubArrayRequest(testSubKey, path, SubArrayRequest.ArrayOperation.INSERT, fragment, bucket());
            fail("Expected IllegalArgumentException for " + operation);
        } catch (IllegalArgumentException e) {
            assertSame("Expected emtpy path IllegalArgumentException for " + operation, AbstractSubdocRequest.EXCEPTION_EMPTY_PATH, e);
        }


        operation = "COUNTER";
        try {
            new SubCounterRequest(testSubKey, path, 21L, bucket());
            fail("Expected IllegalArgumentException for " + operation);
        } catch (IllegalArgumentException e) {
            assertSame("Expected emtpy path IllegalArgumentException for " + operation, AbstractSubdocRequest.EXCEPTION_EMPTY_PATH, e);
        }


        operation = "DICT_ADD";
        try {
            new SubDictAddRequest(testSubKey, path, fragment, bucket());
            fail("Expected IllegalArgumentException for " + operation);
        } catch (IllegalArgumentException e) {
            assertSame("Expected emtpy path IllegalArgumentException for " + operation, AbstractSubdocRequest.EXCEPTION_EMPTY_PATH, e);
        }


        operation = "DICT_UPSERT";
        try {
            new SubDictUpsertRequest(testSubKey, path, fragment, bucket());
            fail("Expected IllegalArgumentException for " + operation);
        } catch (IllegalArgumentException e) {
            assertSame("Expected emtpy path IllegalArgumentException for " + operation, AbstractSubdocRequest.EXCEPTION_EMPTY_PATH, e);
        }


        operation = "REPLACE";
        try {
            new SubReplaceRequest(testSubKey, path, fragment, bucket());
            fail("Expected IllegalArgumentException for " + operation);
        } catch (IllegalArgumentException e) {
            assertSame("Expected emtpy path IllegalArgumentException for " + operation, AbstractSubdocRequest.EXCEPTION_EMPTY_PATH, e);
        }
    }

    @Test
    public void shouldReplaceValueInSubObject() {
        String path = "sub.value";
        ByteBuf fragment = Unpooled.copiedBuffer("\"mutated\"", CharsetUtil.UTF_8);
        ReferenceCountUtil.releaseLater(fragment);

        SubReplaceRequest request = new SubReplaceRequest(testSubKey, path, fragment, bucket());
        SimpleSubdocResponse response = cluster().<SimpleSubdocResponse>send(request).toBlocking().single();
        ReferenceCountUtil.releaseLater(response.content());

        assertTrue(response.status().isSuccess());
        assertEquals(0, response.content().readableBytes());
        assertEquals(ResponseStatus.SUCCESS, response.status());

        //assert the mutation
        String expected = "{\"value\":\"stringValue\", \"sub\": {\"value\": \"mutated\",\"array\": [\"array1\", 2, true]}}";
        assertMutation(testSubKey, expected);
    }

    @Test
    public void shouldReplaceEntryInArray() {
        String path = "sub.array[0]";
        ByteBuf fragment = Unpooled.copiedBuffer("\"mutated\"", CharsetUtil.UTF_8);
        ReferenceCountUtil.releaseLater(fragment);

        SubReplaceRequest request = new SubReplaceRequest(testSubKey, path, fragment, bucket());
        SimpleSubdocResponse response = cluster().<SimpleSubdocResponse>send(request).toBlocking().single();
        ReferenceCountUtil.releaseLater(response.content());

        assertTrue(response.status().isSuccess());
        assertEquals(0, response.content().readableBytes());
        assertEquals(ResponseStatus.SUCCESS, response.status());

        assertMutation(testSubKey, "{\"value\":\"stringValue\", \"sub\": {\"value\": \"subStringValue\",\"array\": [\"mutated\", 2, true]}}");
    }

    @Test
    public void shouldReturnPathNotFoundOnReplaceNonExistingPath() {
        String path = "sub.value2";
        ByteBuf fragment = Unpooled.copiedBuffer("\"mutated\"", CharsetUtil.UTF_8);
        ReferenceCountUtil.releaseLater(fragment);

        SubReplaceRequest request = new SubReplaceRequest(testSubKey, path, fragment, bucket());
        SimpleSubdocResponse response = cluster().<SimpleSubdocResponse>send(request).toBlocking().single();
        ReferenceCountUtil.releaseLater(response.content());

        assertFalse(response.status().isSuccess());
        assertEquals(0, response.content().readableBytes());
        assertEquals(ResponseStatus.SUBDOC_PATH_NOT_FOUND, response.status());
    }

    @Test
    public void shouldAllowArrayOperationOnDocumentRootIfArray() {
        SubArrayRequest.ArrayOperation arrayOp = SubArrayRequest.ArrayOperation.PUSH_FIRST;

        ByteBuf fragment = Unpooled.copiedBuffer("\"arrayElement\"", CharsetUtil.UTF_8);
        ReferenceCountUtil.releaseLater(fragment);

        SubArrayRequest request = new SubArrayRequest(testArrayRoot, "", arrayOp, fragment, bucket());
        SimpleSubdocResponse response = cluster().<SimpleSubdocResponse>send(request).toBlocking().single();

        assertEquals(ResponseStatus.SUCCESS, response.status());
    }

    @Test
    public void shouldNotAllowArrayOperationOnDocumentRootIfNotArray() {
        SubArrayRequest.ArrayOperation arrayOp = SubArrayRequest.ArrayOperation.PUSH_LAST;

        ByteBuf fragment = Unpooled.copiedBuffer("\"arrayElement\"", CharsetUtil.UTF_8);
        ReferenceCountUtil.releaseLater(fragment);

        SubArrayRequest request = new SubArrayRequest(testSubKey, "", arrayOp, fragment, bucket());
        SimpleSubdocResponse response = cluster().<SimpleSubdocResponse>send(request).toBlocking().single();

        assertEquals(ResponseStatus.SUBDOC_PATH_MISMATCH, response.status());
    }

    @Test
    public void shouldReturnPathMismatchForArrayOperationOnNonArrayFinalElement() {
        SubArrayRequest.ArrayOperation arrayOp = SubArrayRequest.ArrayOperation.PUSH_FIRST;
        String path = "sub";

        ByteBuf fragment = Unpooled.copiedBuffer("\"arrayElement\"", CharsetUtil.UTF_8);
        ReferenceCountUtil.releaseLater(fragment);

        SubArrayRequest request = new SubArrayRequest(testSubKey, path, arrayOp, fragment, bucket());
        SimpleSubdocResponse response = cluster().<SimpleSubdocResponse>send(request).toBlocking().single();

        assertEquals(ResponseStatus.SUBDOC_PATH_MISMATCH, response.status());
    }

    @Test
    public void shouldPushLastInArray() {
        SubArrayRequest.ArrayOperation arrayOp = SubArrayRequest.ArrayOperation.PUSH_LAST;
        String path = "sub.array";

        ByteBuf fragment = Unpooled.copiedBuffer("\"arrayElement\"", CharsetUtil.UTF_8);
        ReferenceCountUtil.releaseLater(fragment);

        SubArrayRequest request = new SubArrayRequest(testSubKey, path, arrayOp, fragment, bucket());
        SimpleSubdocResponse response = cluster().<SimpleSubdocResponse>send(request).toBlocking().single();

        assertEquals(ResponseStatus.SUCCESS, response.status());
        assertMutation(testSubKey, jsonContent.replace("]", ", \"arrayElement\"]"));
    }

    @Test
    public void shouldReturnPathNotFoundOnPushLastOnNonExistingPath() {
        SubArrayRequest.ArrayOperation arrayOp = SubArrayRequest.ArrayOperation.PUSH_LAST;
        String path = "sub.array2";

        ByteBuf fragment = Unpooled.copiedBuffer("\"arrayElement\"", CharsetUtil.UTF_8);
        ReferenceCountUtil.releaseLater(fragment);

        SubArrayRequest request = new SubArrayRequest(testSubKey, path, arrayOp, fragment, bucket());
        SimpleSubdocResponse response = cluster().<SimpleSubdocResponse>send(request).toBlocking().single();

        assertEquals(ResponseStatus.SUBDOC_PATH_NOT_FOUND, response.status());
    }

    @Test
    public void shouldPushFirstInArray() {
        SubArrayRequest.ArrayOperation arrayOp = SubArrayRequest.ArrayOperation.PUSH_FIRST;
        String path = "sub.array";

        ByteBuf fragment = Unpooled.copiedBuffer("\"arrayElement\"", CharsetUtil.UTF_8);
        ReferenceCountUtil.releaseLater(fragment);

        SubArrayRequest request = new SubArrayRequest(testSubKey, path, arrayOp, fragment, bucket());
        SimpleSubdocResponse response = cluster().<SimpleSubdocResponse>send(request).toBlocking().single();

        assertEquals(ResponseStatus.SUCCESS, response.status());
        assertMutation(testSubKey, jsonContent.replace("[", "[\"arrayElement\", "));
    }

    @Test
    public void shouldReturnPathNotFoundOnPushFirstOnNonExistingPath() {
        SubArrayRequest.ArrayOperation arrayOp = SubArrayRequest.ArrayOperation.PUSH_FIRST;
        String path = "sub.array2";

        ByteBuf fragment = Unpooled.copiedBuffer("\"arrayElement\"", CharsetUtil.UTF_8);
        ReferenceCountUtil.releaseLater(fragment);

        SubArrayRequest request = new SubArrayRequest(testSubKey, path, arrayOp, fragment, bucket());
        SimpleSubdocResponse response = cluster().<SimpleSubdocResponse>send(request).toBlocking().single();

        assertEquals(ResponseStatus.SUBDOC_PATH_NOT_FOUND, response.status());
    }

    @Test
    public void shouldShiftArrayByOneOnArrayInsert() {
        SubArrayRequest.ArrayOperation arrayOp = SubArrayRequest.ArrayOperation.INSERT;
        String path = "sub.array[1]";

        ByteBuf fragment = Unpooled.copiedBuffer("99", CharsetUtil.UTF_8);
        ReferenceCountUtil.releaseLater(fragment);

        SubArrayRequest request = new SubArrayRequest(testSubKey, path, arrayOp, fragment, bucket());
        SimpleSubdocResponse response = cluster().<SimpleSubdocResponse>send(request).toBlocking().single();

        assertEquals(ResponseStatus.SUCCESS, response.status());
        assertMutation(testSubKey, jsonContent.replace("2", "99, 2"));
    }

    @Test
    public void shouldPrependIfArrayInsertIndexIsZero() {
        SubArrayRequest.ArrayOperation arrayOp = SubArrayRequest.ArrayOperation.INSERT;
        String path = "sub.array[0]";

        ByteBuf fragment = Unpooled.copiedBuffer("99", CharsetUtil.UTF_8);
        ReferenceCountUtil.releaseLater(fragment);

        SubArrayRequest request = new SubArrayRequest(testSubKey, path, arrayOp, fragment, bucket());
        SimpleSubdocResponse response = cluster().<SimpleSubdocResponse>send(request).toBlocking().single();

        assertEquals(ResponseStatus.SUCCESS, response.status());
        assertMutation(testSubKey, jsonContent.replace("[", "[99, "));
    }

    @Test
    public void shouldAppendIfArrayInsertIndexIsSize() {
        SubArrayRequest.ArrayOperation arrayOp = SubArrayRequest.ArrayOperation.INSERT;
        String path = "sub.array[3]";

        ByteBuf fragment = Unpooled.copiedBuffer("99", CharsetUtil.UTF_8);
        ReferenceCountUtil.releaseLater(fragment);

        SubArrayRequest request = new SubArrayRequest(testSubKey, path, arrayOp, fragment, bucket());
        SimpleSubdocResponse response = cluster().<SimpleSubdocResponse>send(request).toBlocking().single();

        assertEquals(ResponseStatus.SUCCESS, response.status());
        assertMutation(testSubKey, jsonContent.replace("]", ", 99]"));
    }

    @Test
    public void shouldReturnPathNotFoundOnArrayInsertOverSize() {
        SubArrayRequest.ArrayOperation arrayOp = SubArrayRequest.ArrayOperation.INSERT;
        String path = "sub.array[5]";

        ByteBuf fragment = Unpooled.copiedBuffer("99", CharsetUtil.UTF_8);
        ReferenceCountUtil.releaseLater(fragment);

        SubArrayRequest request = new SubArrayRequest(testSubKey, path, arrayOp, fragment, bucket());
        SimpleSubdocResponse response = cluster().<SimpleSubdocResponse>send(request).toBlocking().single();

        assertEquals(ResponseStatus.SUBDOC_PATH_NOT_FOUND, response.status());
    }

    @Test
    public void shouldReturnPathInvalidOnArrayInsertAtNegativeIndex() {
        SubArrayRequest.ArrayOperation arrayOp = SubArrayRequest.ArrayOperation.INSERT;
        String path = "sub.array[-1]";

        ByteBuf fragment = Unpooled.copiedBuffer("99", CharsetUtil.UTF_8);
        ReferenceCountUtil.releaseLater(fragment);

        SubArrayRequest request = new SubArrayRequest(testSubKey, path, arrayOp, fragment, bucket());
        SimpleSubdocResponse response = cluster().<SimpleSubdocResponse>send(request).toBlocking().single();

        assertEquals(ResponseStatus.SUBDOC_PATH_INVALID, response.status());
    }

    @Test
    public void shouldAddUniqueIfValueNotAlreadyInArray() {
        SubArrayRequest.ArrayOperation arrayOp = SubArrayRequest.ArrayOperation.ADD_UNIQUE;
        String path = "sub.array";

        ByteBuf fragment = Unpooled.copiedBuffer("99", CharsetUtil.UTF_8);
        ReferenceCountUtil.releaseLater(fragment);

        SubArrayRequest request = new SubArrayRequest(testSubKey, path, arrayOp, fragment, bucket());
        SimpleSubdocResponse response = cluster().<SimpleSubdocResponse>send(request).toBlocking().single();

        assertEquals(ResponseStatus.SUCCESS, response.status());
        assertMutation(testSubKey, jsonContent.replace("]", ", 99]"));
    }

    @Test
    public void shouldReturnPathExistOnArrayAddUniqueWithDuplicateValue() {
        SubArrayRequest.ArrayOperation arrayOp = SubArrayRequest.ArrayOperation.ADD_UNIQUE;
        String path = "sub.array";

        ByteBuf fragment = Unpooled.copiedBuffer("2", CharsetUtil.UTF_8);
        ReferenceCountUtil.releaseLater(fragment);

        SubArrayRequest request = new SubArrayRequest(testSubKey, path, arrayOp, fragment, bucket());
        SimpleSubdocResponse response = cluster().<SimpleSubdocResponse>send(request).toBlocking().single();

        assertEquals(ResponseStatus.SUBDOC_PATH_EXISTS, response.status());
    }

    @Test
    public void shouldReturnPathMismatchOnArrayAddUniqueIfArrayIsNotPrimitive() {
        SubArrayRequest.ArrayOperation arrayOp = SubArrayRequest.ArrayOperation.ADD_UNIQUE;
        String path = "complexArray";

        ByteBuf fragment = Unpooled.copiedBuffer("\"arrayElement\"", CharsetUtil.UTF_8);
        ReferenceCountUtil.releaseLater(fragment);

        SubArrayRequest request = new SubArrayRequest(testComplexSubArray, path, arrayOp, fragment, bucket());
        SimpleSubdocResponse response = cluster().<SimpleSubdocResponse>send(request).toBlocking().single();

        assertEquals(ResponseStatus.SUBDOC_PATH_MISMATCH, response.status());
    }

    @Test
    public void shouldIncrementOnCounterWithPositiveDelta() {
        String path = "sub.array[1]";
        long delta = 100L;
        long expected = 102L;

        SubCounterRequest request = new SubCounterRequest(testSubKey, path, delta, bucket());
        SimpleSubdocResponse response = cluster().<SimpleSubdocResponse>send(request).toBlocking().single();
        ReferenceCountUtil.releaseLater(response.content());
        String result = response.content().toString(CharsetUtil.UTF_8);

        assertEquals(ResponseStatus.SUCCESS, response.status());
        assertEquals(""+expected, result);
        assertEquals(expected, Long.parseLong(result));
    }

    @Test
    public void shouldDecrementOnCounterWithNegativeDelta() {
        String path = "sub.array[1]";
        long delta = -100L;
        long expected = -98L;

        SubCounterRequest request = new SubCounterRequest(testSubKey, path, delta, bucket());
        SimpleSubdocResponse response = cluster().<SimpleSubdocResponse>send(request).toBlocking().single();
        ReferenceCountUtil.releaseLater(response.content());
        String result = response.content().toString(CharsetUtil.UTF_8);

        assertEquals(ResponseStatus.SUCCESS, response.status());
        assertEquals(""+expected, result);
        assertEquals(expected, Long.parseLong(result));
    }

    @Test
    public void shouldStartFromZeroAndApplyDeltaOnCounterInNewFinalPath() {
        String path = "counter";
        long delta = 1234L;
        long expected = delta;

        SubCounterRequest request = new SubCounterRequest(testSubKey, path, delta, bucket());
        SimpleSubdocResponse response = cluster().<SimpleSubdocResponse>send(request).toBlocking().single();
        ReferenceCountUtil.releaseLater(response.content());
        String result = response.content().toString(CharsetUtil.UTF_8);

        assertEquals(ResponseStatus.SUCCESS, response.status());
        assertEquals(""+expected, result);
        assertEquals(expected, Long.parseLong(result));
    }

    @Test
    public void shouldReturnPathNotExistOnCounterInIncompletePath() {
        String path = "counter.subCounter";
        long delta = 99L;

        SubCounterRequest request = new SubCounterRequest(testSubKey, path, delta, bucket());
        SimpleSubdocResponse response = cluster().<SimpleSubdocResponse>send(request).toBlocking().single();
        ReferenceCountUtil.releaseLater(response.content());
        String result = response.content().toString(CharsetUtil.UTF_8);

        assertEquals(0, result.length());
        assertEquals(ResponseStatus.SUBDOC_PATH_NOT_FOUND, response.status());
    }

    @Test
    public void shouldReturnDeltaRangeOnCounterDeltaZero() {
        String path = "counter";
        long delta = 0L;

        SubCounterRequest request = new SubCounterRequest(testSubKey, path, delta, bucket());
        SimpleSubdocResponse response = cluster().<SimpleSubdocResponse>send(request).toBlocking().single();
        ReferenceCountUtil.releaseLater(response.content());
        String result = response.content().toString(CharsetUtil.UTF_8);

        assertEquals(0, result.length());
        assertEquals(ResponseStatus.SUBDOC_DELTA_RANGE, response.status());
    }

    @Test
    public void shouldReturnDeltaRangeOnCounterDeltaOverflow() {
        String path = "counter";
        long prepareOverFlow = 1L;
        long delta = Long.MAX_VALUE;

        //first request will bring the value to +1
        SubCounterRequest request = new SubCounterRequest(testSubKey, path, prepareOverFlow, bucket());
        SimpleSubdocResponse response = cluster().<SimpleSubdocResponse>send(request).toBlocking().single();
        ReferenceCountUtil.releaseLater(response.content());
        String result = response.content().toString(CharsetUtil.UTF_8);

        assertEquals("1", result);

        //second request will overflow
        request = new SubCounterRequest(testSubKey, path, delta, bucket());
        response = cluster().<SimpleSubdocResponse>send(request).toBlocking().single();
        ReferenceCountUtil.releaseLater(response.content());
        result = response.content().toString(CharsetUtil.UTF_8);

        assertEquals(result, 0, result.length());
        /*
         * Was SUBDOC_DELTA_RANGE, but changed to VALUE_CANTINSERT between 4.5 dp and BETA.
         * See https://issues.couchbase.com/browse/JCBC-931, https://issues.couchbase.com/browse/MB-18169
         */
        assertEquals(ResponseStatus.SUBDOC_VALUE_CANTINSERT, response.status());
    }

    @Test
    public void shouldReturnDeltaRangeOnCounterDeltaUnderflow() {
        String path = "counter";
        long prepareUnderflow = -1L;
        long delta = Long.MIN_VALUE;

        //first request will bring the value to -1
        SubCounterRequest request = new SubCounterRequest(testSubKey, path, prepareUnderflow, bucket());
        SimpleSubdocResponse response = cluster().<SimpleSubdocResponse>send(request).toBlocking().single();
        ReferenceCountUtil.releaseLater(response.content());
        String result = response.content().toString(CharsetUtil.UTF_8);

        assertEquals("-1", result);

        //second request will underflow
        request = new SubCounterRequest(testSubKey, path, delta, bucket());
        response = cluster().<SimpleSubdocResponse>send(request).toBlocking().single();
        ReferenceCountUtil.releaseLater(response.content());
        result = response.content().toString(CharsetUtil.UTF_8);

        assertEquals(result, 0, result.length());
        assertEquals(ResponseStatus.SUBDOC_DELTA_RANGE, response.status());
    }

    @Test
    public void shouldHaveIndividualResultsOnSparseMultiLookup() {
        String expected = "EXIST(sub): SUCCESS\n" +
                "EXIST(sub2): SUBDOC_PATH_NOT_FOUND\n" +
                "GET(sub): SUCCESS = {\"value\": \"subStringValue\",\"array\": [\"array1\", 2, true]}\n" +
                "GET(sub.array[1]): SUCCESS = 2\n" +
                "GET(sub2): SUBDOC_PATH_NOT_FOUND\n";

        SubMultiLookupRequest request = new SubMultiLookupRequest(testSubKey, bucket(),
                new LookupCommandBuilder(Lookup.EXIST, "sub").build(),
                new LookupCommandBuilder(Lookup.EXIST, "sub2").build(),
                new LookupCommandBuilder(Lookup.GET, "sub").build(),
                new LookupCommandBuilder(Lookup.GET, "sub.array[1]").build(),
                new LookupCommandBuilder(Lookup.GET, "sub2").build());

        MultiLookupResponse response = cluster().<MultiLookupResponse>send(request).toBlocking().single();
        assertEquals(Unpooled.EMPTY_BUFFER, response.content());
        StringBuilder body = new StringBuilder();
        for (MultiResult r : response.responses()) {
            body.append(r.toString()).append('\n');
            ReferenceCountUtil.release(r.value());
        }

        assertTrue(response.cas() != 0);
        assertEquals(ResponseStatus.SUBDOC_MULTI_PATH_FAILURE, response.status());
        assertEquals(expected, body.toString());
    }

    @Test
    public void shouldHaveIndividualResultsOnFullySuccessfulMultiLookup() {
        String expected = "EXIST(sub): SUCCESS\n" +
                "GET(sub.array[1]): SUCCESS = 2\n";

        SubMultiLookupRequest request = new SubMultiLookupRequest(testSubKey, bucket(),
                new LookupCommandBuilder(Lookup.EXIST, "sub").build(),
                new LookupCommandBuilder(Lookup.GET, "sub.array[1]").build());

        MultiLookupResponse response = cluster().<MultiLookupResponse>send(request).toBlocking().single();
        assertEquals(Unpooled.EMPTY_BUFFER, response.content());
        StringBuilder body = new StringBuilder();
        for (MultiResult r : response.responses()) {
            body.append(r.toString()).append('\n');
            ReferenceCountUtil.release(r.value());
        }

        assertTrue(response.cas() != 0);
        assertEquals(ResponseStatus.SUCCESS, response.status());
        assertEquals(expected, body.toString());
    }

    @Test
    public void shouldApplyAllMultiMutationsAndReleaseCommandFragments() {
        ByteBuf counterFragment = Unpooled.copiedBuffer("-404", CharsetUtil.UTF_8);
        counterFragment.retain(2);

        ByteBuf stringFragment = Unpooled.copiedBuffer("\"mutated\"", CharsetUtil.UTF_8);
        stringFragment.retain(2);

        ByteBuf arrayInsertedFragment = Unpooled.copiedBuffer("\"inserted\"", CharsetUtil.UTF_8);
        ByteBuf arrayFirstFragment = Unpooled.copiedBuffer("\"first\"", CharsetUtil.UTF_8);
        ByteBuf arrayLastFragment = Unpooled.copiedBuffer("\"last\"", CharsetUtil.UTF_8);
        ByteBuf uniqueFragment = Unpooled.copiedBuffer("\"unique\"", CharsetUtil.UTF_8);

        MutationCommand[] commands = new MutationCommand[] {
                new MutationCommandBuilder(Mutation.COUNTER, "counter").fragment(counterFragment).build(),
                new MutationCommandBuilder(Mutation.COUNTER, "another.counter").fragment(counterFragment).createIntermediaryPath(true).build(),
                new MutationCommandBuilder(Mutation.COUNTER, "another.counter").fragment(counterFragment).build(),
                new MutationCommandBuilder(Mutation.DICT_ADD, "sub.value2").fragment(stringFragment).build(),
                new MutationCommandBuilder(Mutation.DICT_UPSERT, "sub.value3").fragment(stringFragment).build(),
                new MutationCommandBuilder(Mutation.REPLACE, "value").fragment(stringFragment).build(),
                new MutationCommandBuilder(Mutation.ARRAY_INSERT, "sub.array[1]").fragment(arrayInsertedFragment).build(),
                new MutationCommandBuilder(Mutation.ARRAY_PUSH_FIRST, "sub.array").fragment(arrayFirstFragment).build(),
                new MutationCommandBuilder(Mutation.ARRAY_PUSH_LAST, "sub.array").fragment(arrayLastFragment).build(),
                new MutationCommandBuilder(Mutation.ARRAY_ADD_UNIQUE, "sub.array").fragment(uniqueFragment).build(),
                new MutationCommandBuilder(Mutation.DELETE, "sub.value").build()
        };

        SubMultiMutationRequest request = new SubMultiMutationRequest(testSubKey, bucket(), commands);
        MultiMutationResponse response = cluster().<MultiMutationResponse>send(request).toBlocking().single();
        assertEquals(ResponseStatus.SUCCESS, response.status());
        assertEquals(Unpooled.EMPTY_BUFFER, response.content());
        assertEquals(-1, response.firstErrorIndex());
        assertEquals(ResponseStatus.SUCCESS, response.firstErrorStatus());

        assertEquals(0, stringFragment.refCnt());
        assertEquals(0, counterFragment.refCnt());
        assertEquals(0, arrayInsertedFragment.refCnt());
        assertEquals(0, arrayFirstFragment.refCnt());
        assertEquals(0, arrayLastFragment.refCnt());
        assertEquals(0, uniqueFragment.refCnt());

        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < commands.length; i++) {
            MutationCommand command = commands[i];
            MultiResult result = response.responses().get(i);
            assertEquals(command.path(), result.path());
            assertEquals(command.mutation(), result.operation());

            sb.append('\n').append(result);
            ReferenceCountUtil.releaseLater(result.value());
        }
        if (sb.length() > 1) sb.deleteCharAt(0);

        String expectedResponse = "COUNTER(counter): SUCCESS = -404" +
                "\nCOUNTER(another.counter): SUCCESS = -404" +
                "\nCOUNTER(another.counter): SUCCESS = -808" +
                //values below have no content
                "\nDICT_ADD(sub.value2): SUCCESS" +
                "\nDICT_UPSERT(sub.value3): SUCCESS" +
                "\nREPLACE(value): SUCCESS" +
                "\nARRAY_INSERT(sub.array[1]): SUCCESS" +
                "\nARRAY_PUSH_FIRST(sub.array): SUCCESS" +
                "\nARRAY_PUSH_LAST(sub.array): SUCCESS" +
                "\nARRAY_ADD_UNIQUE(sub.array): SUCCESS" +
                "\nDELETE(sub.value): SUCCESS";
        assertEquals(expectedResponse, sb.toString());

        String expected = "{\"value\":\"mutated\"," +
                "\"sub\":{" +
//                "\"value\":\"subStringValue\"," + //DELETED
                "\"array\":[\"first\",\"array1\",\"inserted\",2,true,\"last\",\"unique\"]" +
                ",\"value2\":\"mutated\"" +
                ",\"value3\":\"mutated\"}," +
                "\"counter\":-404," +
                "\"another\":{\"counter\":-808}}";
        assertMutation(testSubKey, expected);
    }

    @Test
    public void shouldFailSomeMultiMutationsAndReleaseCommandFragments() {
        ByteBuf counterFragment = Unpooled.copiedBuffer("-404", CharsetUtil.UTF_8);
        counterFragment.retain();

        ByteBuf stringFragment = Unpooled.copiedBuffer("\"mutated\"", CharsetUtil.UTF_8);
        stringFragment.retain(2);

        ByteBuf arrayInsertedFragment = Unpooled.copiedBuffer("\"inserted\"", CharsetUtil.UTF_8);
        ByteBuf arrayFirstFragment = Unpooled.copiedBuffer("\"first\"", CharsetUtil.UTF_8);
        ByteBuf arrayLastFragment = Unpooled.copiedBuffer("\"last\"", CharsetUtil.UTF_8);
        ByteBuf uniqueFragment = Unpooled.copiedBuffer("\"unique\"", CharsetUtil.UTF_8);

        SubMultiMutationRequest request = new SubMultiMutationRequest(testSubKey, bucket(),
                new MutationCommandBuilder(Mutation.COUNTER, "counter", counterFragment).build(),
                new MutationCommandBuilder(Mutation.COUNTER, "another.counter", counterFragment).createIntermediaryPath(true).build(),
                new MutationCommandBuilder(Mutation.DICT_ADD, "sub.value2", stringFragment).build(),
                new MutationCommandBuilder(Mutation.DICT_UPSERT, "sub.value3", stringFragment).build(),
                new MutationCommandBuilder(Mutation.REPLACE, "value", stringFragment).build(),
                //this one fails
                new MutationCommandBuilder(Mutation.ARRAY_INSERT, "sub.array[5]", arrayInsertedFragment).build(),
                new MutationCommandBuilder(Mutation.ARRAY_PUSH_FIRST, "sub.array", arrayFirstFragment).build(),
                new MutationCommandBuilder(Mutation.ARRAY_PUSH_LAST, "sub.array", arrayLastFragment).build(),
                new MutationCommandBuilder(Mutation.ARRAY_ADD_UNIQUE, "sub.array", uniqueFragment).build(),
                //this one would also fail, but server stops at first failure
                new MutationCommandBuilder(Mutation.DELETE, "path.not.found").build()
        );
        MultiMutationResponse response = cluster().<MultiMutationResponse>send(request).toBlocking().single();
        assertEquals(ResponseStatus.SUBDOC_MULTI_PATH_FAILURE, response.status());
        assertEquals(Unpooled.EMPTY_BUFFER, response.content());
        assertEquals(5, response.firstErrorIndex());
        assertEquals(ResponseStatus.SUBDOC_PATH_NOT_FOUND, response.firstErrorStatus());
        assertEquals(0, response.responses().size());

        assertEquals(0, stringFragment.refCnt());
        assertEquals(0, counterFragment.refCnt());
        assertEquals(0, arrayInsertedFragment.refCnt());
        assertEquals(0, arrayFirstFragment.refCnt());
        assertEquals(0, arrayLastFragment.refCnt());
        assertEquals(0, uniqueFragment.refCnt());

        //no mutation happened
        String expected = jsonContent;
        assertMutation(testSubKey, expected);
    }

    @Test
    public void shouldFailAllMultiMutationsAndReleaseCommandFragments() {
        ByteBuf counterFragment = Unpooled.copiedBuffer("-404", CharsetUtil.UTF_8);
        ByteBuf stringFragment = Unpooled.copiedBuffer("\"mutated\"", CharsetUtil.UTF_8);
        ByteBuf arrayFirstFragment = Unpooled.copiedBuffer("\"first\"", CharsetUtil.UTF_8);

        SubMultiMutationRequest request = new SubMultiMutationRequest(testInsertionSubKey, bucket(),
                new MutationCommandBuilder(Mutation.COUNTER, "counter", counterFragment).build(),
                new MutationCommandBuilder(Mutation.DICT_UPSERT, "sub.value3", stringFragment).build(),
                new MutationCommandBuilder(Mutation.ARRAY_PUSH_FIRST, "sub.array", arrayFirstFragment).build(),
                new MutationCommandBuilder(Mutation.DELETE, "some.paht").build()
        );
        MultiMutationResponse response = cluster().<MultiMutationResponse>send(request).toBlocking().single();
        assertEquals(ResponseStatus.NOT_EXISTS, response.status());
        assertEquals(Unpooled.EMPTY_BUFFER, response.content());
        assertEquals(-1, response.firstErrorIndex());
        assertEquals(ResponseStatus.FAILURE, response.firstErrorStatus());
        assertEquals(0, response.responses().size());

        assertEquals(0, stringFragment.refCnt());
        assertEquals(0, counterFragment.refCnt());
        assertEquals(0, arrayFirstFragment.refCnt());
    }

    /**
     * Helper method to get a whole document and assert its content against a subdoc mutation.
     * @param key the document ID.
     * @param expected the JSON content that is expected.
     */
    private void assertMutation(String key, String expected) {
        //assert the mutation
        GetResponse finalState = cluster().<GetResponse>send(new GetRequest(key, bucket())).toBlocking().single();
        ReferenceCountUtil.releaseLater(finalState.content());
        String actual = finalState.content().toString(CharsetUtil.UTF_8);
        //work around MB-17143 and generally fact that no order/space guarantees
        // are made for JSON production by the subdoc API.
        try {
            Map actualMap = DefaultObjectMapper.readValue(actual, Map.class);
            Map expectedMap = DefaultObjectMapper.readValue(expected, Map.class);
            assertEquals("Expected mutation on " + key + " was not observed", expectedMap, actualMap);
        } catch (IOException e) {
            fail("Failure to compare JSON contents: " + e.toString());
        }
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
        } else {
            assertNull(token);
        }
    }
}
