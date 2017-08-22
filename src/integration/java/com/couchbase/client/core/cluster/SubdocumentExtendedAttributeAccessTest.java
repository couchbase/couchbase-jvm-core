package com.couchbase.client.core.cluster;

import static org.junit.Assert.*;

import com.couchbase.client.core.message.ResponseStatus;
import com.couchbase.client.core.message.kv.UpsertRequest;
import com.couchbase.client.core.message.kv.UpsertResponse;
import com.couchbase.client.core.message.kv.subdoc.multi.*;
import com.couchbase.client.core.message.kv.subdoc.simple.SimpleSubdocResponse;
import com.couchbase.client.core.message.kv.subdoc.simple.SubDeleteRequest;
import com.couchbase.client.core.message.kv.subdoc.simple.SubDictAddRequest;
import com.couchbase.client.core.message.kv.subdoc.simple.SubGetRequest;
import com.couchbase.client.core.util.ClusterDependentTest;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.util.CharsetUtil;
import io.netty.util.ReferenceCountUtil;
import io.netty.util.ResourceLeakDetector;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Extended attributes test
 * Works on couchbase server versions 5.0 and above
 *
 * @author Subhashni Balakrishnan
 * @since 1.4.2
 */
public class SubdocumentExtendedAttributeAccessTest extends ClusterDependentTest {

    static {
        ResourceLeakDetector.setLevel(ResourceLeakDetector.Level.PARANOID);
    }

    private static final String testXAttrKey = "testXAttrKey";
    private static final String jsonContent = "{\"value\":\"stringValue\", \"sub\": {\"value\": \"subStringValue\",\"array\": [\"array1\", 2, true]}}";

    @BeforeClass
    public static void checkExtendedAttributeAvailable() throws Exception {
        connect(false);
        assumeMinimumVersionCompatible(5, 0);
    }

    @Before
    public void prepareData() {
        UpsertRequest upsert = new UpsertRequest(testXAttrKey, Unpooled.copiedBuffer(jsonContent, CharsetUtil.UTF_8), bucket(), true);
        UpsertResponse response = cluster().<UpsertResponse>send(upsert).toBlocking().single();
        assertTrue("Couldn't insert " + testXAttrKey + "(" + response.status() + ")", response.status().isSuccess());
        response.release();
    }

    @Test
    public void shouldBeAbleToAccessXAttrs() {
        String subPath = "spring.class";
        ByteBuf fragment = Unpooled.copiedBuffer("\"SomeClass\"", CharsetUtil.UTF_8);

        //insert
        SubDictAddRequest insertRequest = new SubDictAddRequest(testXAttrKey, subPath, fragment, bucket());
        insertRequest.xattr(true);
        insertRequest.createIntermediaryPath(true);
        SimpleSubdocResponse insertResponse = cluster().<SimpleSubdocResponse>send(insertRequest).toBlocking().single();
        assertTrue(insertResponse.status().isSuccess());
        assertEquals(0, insertResponse.content().readableBytes());
        insertResponse.content().release();


        //get
        SubGetRequest getRequest = new SubGetRequest(testXAttrKey, "spring.class", bucket());
        getRequest.xattr(true);
        SimpleSubdocResponse lookupResponse = cluster().<SimpleSubdocResponse>send(getRequest).toBlocking().single();
        assertTrue(lookupResponse.status().isSuccess());
        assertEquals(lookupResponse.content().toString(CharsetUtil.UTF_8) , "\"SomeClass\"");
        lookupResponse.content().release();


        //delete
        SubDeleteRequest deleteRequest = new SubDeleteRequest(testXAttrKey, subPath, bucket());
        deleteRequest.xattr(true);
        SimpleSubdocResponse deleteResponse = cluster().<SimpleSubdocResponse>send(deleteRequest).toBlocking().single();
        assertTrue(deleteResponse.status().isSuccess());
        assertEquals(0, deleteResponse.content().readableBytes());
        deleteResponse.content().release();

        getRequest = new SubGetRequest(testXAttrKey, "spring.class", bucket());
        getRequest.xattr(true);
        lookupResponse = cluster().<SimpleSubdocResponse>send(getRequest).toBlocking().single();
        assertTrue(lookupResponse.status() == ResponseStatus.SUBDOC_PATH_NOT_FOUND);
        lookupResponse.content().release();

    }

    @Test
    public void shouldBeAbleToAccessMultipleXAttrsInSameNameSpace() {
        ByteBuf classFragment = Unpooled.copiedBuffer("\"SomeClass\"", CharsetUtil.UTF_8);
        ByteBuf refFragment = Unpooled.copiedBuffer("\"ids\"", CharsetUtil.UTF_8);

        SubMultiMutationRequest mutationRequest = new SubMultiMutationRequest(testXAttrKey, bucket(),
                new MutationCommandBuilder(Mutation.DICT_UPSERT, "spring.class")
                        .fragment(classFragment).createIntermediaryPath(true).xattr(true).build(),
                new MutationCommandBuilder(Mutation.DICT_UPSERT, "spring.refs")
                        .fragment(refFragment).createIntermediaryPath(true).xattr(true).build());
        MultiMutationResponse mutationResponse = cluster().<MultiMutationResponse>send(mutationRequest).toBlocking().single();
        assertTrue(mutationResponse.status().isSuccess());
        mutationResponse.responses().get(0).value().release();
        mutationResponse.responses().get(1).value().release();

        SubMultiLookupRequest lookupRequest = new SubMultiLookupRequest(testXAttrKey, bucket(),
                new LookupCommandBuilder(Lookup.GET, "spring.class")
                        .xattr(true).build(),
                new LookupCommandBuilder(Lookup.GET, "spring.refs")
                        .xattr(true).build());

        MultiLookupResponse lookupResponse = cluster().<MultiLookupResponse>send(lookupRequest).toBlocking().single();
        assertTrue(lookupResponse.status().isSuccess());
        lookupResponse.responses().get(0).value().release();
        lookupResponse.responses().get(1).value().release();


        mutationRequest = new SubMultiMutationRequest(testXAttrKey, bucket(),
                new MutationCommandBuilder(Mutation.DELETE, "spring.class").xattr(true).build(),
                new MutationCommandBuilder(Mutation.DELETE, "spring.refs").xattr(true).build());
        mutationResponse = cluster().<MultiMutationResponse>send(mutationRequest).toBlocking().single();
        assertTrue(mutationResponse.status().isSuccess());
        mutationResponse.responses().get(0).value().release();
        mutationResponse.responses().get(1).value().release();

    }
}
