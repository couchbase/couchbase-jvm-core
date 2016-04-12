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

package com.couchbase.client.core.config;

import com.couchbase.client.core.service.ServiceType;
import org.junit.Test;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

/**
 * Verifies the functionality of the {@link DefaultNodeInfo}.
 *
 * @author Michael Nitschinger
 * @since 1.0.3
 */
public class DefaultNodeInfoTest {

    @Test
    public void shouldExposeViewServiceWhenAvailable() {
        Map<String, Integer> ports = new HashMap<String, Integer>();
        String viewBase = "http://127.0.0.1:8092/default%2Baa4b515529fa706f1e5f09f21abb5c06";
        DefaultNodeInfo info = new DefaultNodeInfo(viewBase, "localhost:8091", ports);

        assertEquals(2, info.services().size());
        assertEquals(8091, (long) info.services().get(ServiceType.CONFIG));
        assertEquals(8092, (long) info.services().get(ServiceType.VIEW));
    }

    @Test
    public void shouldNotExposeViewServiceWhenNotAvailable() {
        Map<String, Integer> ports = new HashMap<String, Integer>();
        DefaultNodeInfo info = new DefaultNodeInfo(null, "localhost:8091", ports);

        assertEquals(1, info.services().size());
        assertEquals(8091, (long) info.services().get(ServiceType.CONFIG));
    }
}
