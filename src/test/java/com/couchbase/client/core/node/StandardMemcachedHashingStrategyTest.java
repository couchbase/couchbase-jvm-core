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

package com.couchbase.client.core.node;

import com.couchbase.client.core.config.NodeInfo;
import com.couchbase.client.core.service.ServiceType;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Verifies the functionality of {@link StandardMemcachedHashingStrategy}.
 */
public class StandardMemcachedHashingStrategyTest {

    private static final MemcachedHashingStrategy STRATEGY
        = StandardMemcachedHashingStrategy.INSTANCE;

    @Test
    public void shouldHashCorrectly() throws Exception {
        Map<ServiceType, Integer> services = new HashMap<ServiceType, Integer>();
        services.put(ServiceType.BINARY, 11210);

        NodeInfo nodeInfo = mock(NodeInfo.class);
        when(nodeInfo.rawHostname()).thenReturn("hostname");
        when(nodeInfo.services()).thenReturn(services);

        assertEquals("hostname:11210-0", STRATEGY.hash(nodeInfo, 0));

        nodeInfo = mock(NodeInfo.class);
        when(nodeInfo.rawHostname()).thenReturn("1.2.3.4");
        when(nodeInfo.services()).thenReturn(services);

        assertEquals("1.2.3.4:11210-99", STRATEGY.hash(nodeInfo, 99));
    }

}