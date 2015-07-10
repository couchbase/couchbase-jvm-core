/**
 * Copyright (c) 2015 Couchbase, Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALING
 * IN THE SOFTWARE.
 */
package com.couchbase.client.core.config;

import com.couchbase.client.core.service.ServiceType;
import com.couchbase.client.core.util.Resources;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Test;

import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class DefaultMemcachedBucketConfigTest {

    private static final ObjectMapper JSON_MAPPER = new ObjectMapper();

    /**
     * The config loaded has 4 nodes, but only two are data nodes. This tests checks that the ketama
     * nodes are only populated for those two nodes which include the binary service type.
     */
    @Test
    public void shouldOnlyUseDataNodesForKetama() throws Exception {
        String raw = Resources.read("memcached_mixed_sherlock.json", getClass());
        MemcachedBucketConfig config = JSON_MAPPER.readValue(raw, MemcachedBucketConfig.class);

        assertEquals(4, config.nodes().size());
        for (Map.Entry<Long, NodeInfo> node : config.ketamaNodes().entrySet()) {
            String hostname = node.getValue().hostname().getHostName();
            assertTrue(hostname.equals("vnode1") || hostname.equals("vnode2"));
            assertTrue(node.getValue().services().containsKey(ServiceType.BINARY));
        }
    }

}
