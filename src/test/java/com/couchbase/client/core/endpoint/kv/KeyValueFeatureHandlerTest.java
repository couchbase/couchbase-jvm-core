/*
 * Copyright (c) 2018 Couchbase, Inc.
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

package com.couchbase.client.core.endpoint.kv;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.netty.util.CharsetUtil;
import org.junit.Test;

import java.util.HashMap;

import static org.junit.Assert.assertEquals;

/**
 * Verifies the functionality of the {@link KeyValueFeatureHandler}.
 *
 * @author Michael Nitschinger
 * @since 1.5.6
 */
public class KeyValueFeatureHandlerTest {

    private static final ObjectMapper JACKSON = new ObjectMapper();

    @Test
    public void shouldConvertUserAgent() throws Exception {
        byte[] result = KeyValueFeatureHandler.generateAgentJson(
            "my-agent",
            1234,
            5678
        );

        HashMap decoded = JACKSON.readValue(result, HashMap.class);
        assertEquals("my-agent", decoded.get("a"));
        assertEquals("00000000000004D2/000000000000162E", decoded.get("i"));
    }

    @Test
    public void shouldCutTooLongAgent() throws Exception {
        String tooLongAgent = "foobar";
        while (tooLongAgent.length() < 200) {
            tooLongAgent += tooLongAgent;
        }
        byte[] result = KeyValueFeatureHandler.generateAgentJson(
            tooLongAgent,
            1234,
            5678
        );

        HashMap decoded = JACKSON.readValue(result, HashMap.class);
        assertEquals(200, ((String) decoded.get("a")).length());
    }

}