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

import com.couchbase.client.core.config.parser.BucketConfigParser;
import com.couchbase.client.core.env.CoreEnvironment;
import com.couchbase.client.core.env.DefaultCoreEnvironment;
import com.couchbase.client.core.service.ServiceType;
import com.couchbase.client.core.util.Resources;
import com.couchbase.client.core.utils.NetworkAddress;
import org.junit.AfterClass;
import org.junit.Test;

import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeFalse;

/**
 * Verifies that parsing various bucket configs works as expected through the
 * {@link BucketConfigParser}.
 */
public class DefaultMemcachedBucketConfigTest {

    private static final CoreEnvironment ENV = DefaultCoreEnvironment.create();

    @AfterClass
    public static final void cleanup() {
        ENV.shutdown();
    }

    /**
     * The config loaded has 4 nodes, but only two are data nodes. This tests checks that the ketama
     * nodes are only populated for those two nodes which include the binary service type.
     */
    @Test
    public void shouldOnlyUseDataNodesForKetama() throws Exception {
        String raw = Resources.read("memcached_mixed_sherlock.json", getClass());
        MemcachedBucketConfig config = (MemcachedBucketConfig) BucketConfigParser.parse(raw, ENV);

        assertEquals(4, config.nodes().size());
        for (Map.Entry<Long, NodeInfo> node : config.ketamaNodes().entrySet()) {
            String hostname = node.getValue().hostname().address();
            assertTrue(hostname.equals("192.168.56.101") || hostname.equals("192.168.56.102"));
            assertTrue(node.getValue().services().containsKey(ServiceType.BINARY));
        }
    }

    @Test
    public void shouldLoadConfigWithIPv6() throws Exception {
        assumeFalse(NetworkAddress.FORCE_IPV4);

        String raw = Resources.read("memcached_with_ipv6.json", getClass());
        MemcachedBucketConfig config = (MemcachedBucketConfig) BucketConfigParser.parse(raw, ENV);

        assertEquals(2, config.nodes().size());
        for (Map.Entry<Long, NodeInfo> node : config.ketamaNodes().entrySet()) {
            String hostname = node.getValue().hostname().address();
            assertTrue(hostname.equals("fd63:6f75:6368:2068:1471:75ff:fe25:a8be")
                || hostname.equals("fd63:6f75:6368:2068:c490:b5ff:fe86:9cf7"));
            assertTrue(node.getValue().services().containsKey(ServiceType.BINARY));
        }
    }

}
