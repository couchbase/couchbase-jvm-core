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
package com.couchbase.client.core.node;

import com.couchbase.client.core.config.NodeInfo;
import com.couchbase.client.core.service.ServiceType;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Verifies the functionality of the {@link LegacyMemcachedHashingStrategy}.
 *
 * @author Michael Nitschinger
 * @since 2.3.6
 */
public class LegacyMemcachedNodeHashingStrategyTest {

    @Test
    public void shouldHashNode() throws Exception {
        MemcachedHashingStrategy strategy = LegacyMemcachedHashingStrategy.INSTANCE;

        NodeInfo infoMock = mock(NodeInfo.class);
        when(infoMock.hostname()).thenReturn("127.0.0.1");
        Map<ServiceType, Integer> serviceMap = new HashMap<ServiceType, Integer>();
        serviceMap.put(ServiceType.BINARY, 11210);
        when(infoMock.services()).thenReturn(serviceMap);
        assertEquals("127.0.0.1:11210-0", strategy.hash(infoMock,0));
    }


}