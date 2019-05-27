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
import com.couchbase.client.core.util.TestProperties;
import com.couchbase.client.core.utils.NetworkAddress;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assume.assumeFalse;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Verifies the functionality of the {@link DefaultMemcachedHashingStrategy}.
 *
 * @author Michael Nitschinger
 * @since 2.3.6
 */
public class DefaultMemcachedHashingStrategyTest {

    @Test
    public void shouldHashNode() {
        assumeFalse(TestProperties.isCi());

        MemcachedHashingStrategy strategy = DefaultMemcachedHashingStrategy.INSTANCE;

        NodeInfo infoMock = mock(NodeInfo.class);
        when(infoMock.hostname()).thenReturn("localhost");
        assertEquals("localhost-0", strategy.hash(infoMock,0));
    }

}