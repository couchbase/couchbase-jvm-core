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

import com.couchbase.client.core.endpoint.ResponseStatusConverter;
import com.couchbase.client.core.endpoint.kv.ErrorMap;
import com.couchbase.client.core.message.kv.UpsertRequest;
import com.couchbase.client.core.message.kv.UpsertResponse;
import com.couchbase.client.core.util.ClusterDependentTest;
import com.couchbase.mock.JsonUtils;
import io.netty.buffer.Unpooled;
import io.netty.util.CharsetUtil;
import io.netty.util.ReferenceCountUtil;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static com.couchbase.client.core.endpoint.kv.KeyValueHandler.OP_UPSERT;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assume.assumeTrue;

/**
 * Basic error map test
 *
 * @author Subhashni Balakrishnan
 * @since 1.4.5
 */
public class KeyValueErrorMapTest extends ClusterDependentTest {

    @BeforeClass
    public static void setup() throws Exception {
        connect(true);
    }

    @Before
    public void before() throws Exception {
        assumeTrue(
            "Ignoring because extended error not enabled",
            Boolean.parseBoolean(System.getProperty("com.couchbase.xerrorEnabled", "true"))
        );
    }

    private static void startRetryVerifyRequest() throws Exception {
        Map<String, String> parameters = new HashMap<String, String>();
        parameters.put("idx", "0");
        parameters.put("bucket", bucket());
        String rawBody = sendGetHttpRequestToMock("mock/start_retry_verify", parameters);
        com.google.gson.JsonObject respObject = JsonUtils.GSON.fromJson(rawBody, com.google.gson.JsonObject.class);
        String verifyStatus = respObject.get("status").getAsString();
        if (verifyStatus.compareTo("ok") != 0) {
            throw new Exception(respObject.get("error").getAsString());
        }
    }

    private static void checkRetryVerifyRequest(long code, byte opcode, int fuzz) throws Exception {
        Map<String, String> parameters = new HashMap<String, String>();
        parameters.put("idx", "0");
        parameters.put("opcode", Byte.toString(opcode));
        parameters.put("errcode", Long.toString(code));
        parameters.put("bucket", bucket());
        parameters.put("fuzz_ms",  Integer.toString(fuzz));
        String rawBody = sendGetHttpRequestToMock("mock/check_retry_verify", parameters);
        com.google.gson.JsonObject respObject = JsonUtils.GSON.fromJson(rawBody, com.google.gson.JsonObject.class);
        String verifyStatus = respObject.get("status").getAsString();
        if (verifyStatus.compareTo("ok") != 0) {
            throw new Exception(respObject.get("error").getAsString());
        }
    }

    private static void opFailRequest(long code, int count) throws Exception {
        Map<String, String> parameters = new HashMap<String, String>();
        parameters.put("code", Long.toString(code));
        parameters.put("count", Integer.toString(count));
        parameters.put("servers", "[0]");
        String rawBody = sendGetHttpRequestToMock("mock/opfail", parameters);
        com.google.gson.JsonObject respObject = JsonUtils.GSON.fromJson(rawBody, com.google.gson.JsonObject.class);
        String verifyStatus = respObject.get("status").getAsString();
        if (verifyStatus.compareTo("ok") != 0) {
            throw new Exception(respObject.get("error").getAsString());
        }
    }

    @Test
    public void checkIfTheErrorMapIsRead() throws Exception {
        ErrorMap errMap = ResponseStatusConverter.getBinaryErrorMap();
        assertNotNull(errMap);
    }

    @Test
    public void verifyConstantRetry() throws Exception {
        opFailRequest(Long.parseLong("7FF0", 16),  -1);
        startRetryVerifyRequest();
        try {
            String key = "upsert-key";
            UpsertRequest upsert = new UpsertRequest(key, Unpooled.copiedBuffer("", CharsetUtil.UTF_8), bucket());
            UpsertResponse response = cluster().<UpsertResponse>send(upsert).toBlocking().single();
            ReferenceCountUtil.releaseLater(response.content());
        } catch (Exception ex) {
            //ignore exception
        }
        checkRetryVerifyRequest(Long.parseLong("7FF0", 16), OP_UPSERT, 25);
        opFailRequest(Long.parseLong("7FF0", 16),  0);
    }

    @Test
    public void verifyLinearRetry() throws Exception {
        opFailRequest(Long.parseLong("7FF1", 16), -1);
        startRetryVerifyRequest();
        try {
            String key = "upsert-key";
            UpsertRequest upsert = new UpsertRequest(key, Unpooled.copiedBuffer("", CharsetUtil.UTF_8), bucket());
            UpsertResponse response = cluster().<UpsertResponse>send(upsert).toBlocking().single();
            ReferenceCountUtil.releaseLater(response.content());
        } catch (Exception ex) {
            //ignore exception
        }
        checkRetryVerifyRequest(Long.parseLong("7FF1", 16), OP_UPSERT, 25);
        opFailRequest(Long.parseLong("7FF1", 16),  0);
    }

    @Test
    public void verifyExponentialRetry() throws Exception {
        opFailRequest(Long.parseLong("7FF2", 16), -1);
        startRetryVerifyRequest();
        try {
            String key = "upsert-key";
            UpsertRequest upsert = new UpsertRequest(key, Unpooled.copiedBuffer("", CharsetUtil.UTF_8), bucket());
            UpsertResponse response = cluster().<UpsertResponse>send(upsert).toBlocking().single();
            ReferenceCountUtil.releaseLater(response.content());
        } catch (Exception ex) {
            //ignore exception
        }
        checkRetryVerifyRequest(Long.parseLong("7FF2", 16), OP_UPSERT, 25);
        opFailRequest(Long.parseLong("7FF2", 16),  0);
    }
}
