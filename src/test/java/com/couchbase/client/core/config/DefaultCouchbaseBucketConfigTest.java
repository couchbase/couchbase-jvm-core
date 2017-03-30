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
import com.couchbase.client.core.util.Resources;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Test;
import java.net.InetAddress;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class DefaultCouchbaseBucketConfigTest {

    private static final ObjectMapper JSON_MAPPER = new ObjectMapper();

    @Test
    public void shouldHavePrimaryPartitionsOnNode() throws Exception {
        String raw = Resources.read("config_with_mixed_partitions.json", getClass());
        CouchbaseBucketConfig config = JSON_MAPPER.readValue(raw, CouchbaseBucketConfig.class);

        assertTrue(config.hasPrimaryPartitionsOnNode(InetAddress.getByName("1.2.3.4")));
        assertFalse(config.hasPrimaryPartitionsOnNode(InetAddress.getByName("2.3.4.5")));
        assertEquals(BucketNodeLocator.VBUCKET, config.locator());
        assertFalse(config.ephemeral());
    }

    @Test
    public void shouldFallbackToNodeHostnameIfNotInNodesExt() throws Exception {
        String raw = Resources.read("nodes_ext_without_hostname.json", getClass());
        CouchbaseBucketConfig config = JSON_MAPPER.readValue(raw, CouchbaseBucketConfig.class);

        InetAddress expected = InetAddress.getByName("1.2.3.4");
        assertEquals(1, config.nodes().size());
        assertEquals(expected, config.nodes().get(0).hostname());
        assertEquals(BucketNodeLocator.VBUCKET, config.locator());
        assertFalse(config.ephemeral());
    }

    @Test
    public void shouldGracefullyHandleEmptyPartitions() throws Exception {
        String raw = Resources.read("config_with_no_partitions.json", getClass());
        CouchbaseBucketConfig config = JSON_MAPPER.readValue(raw, CouchbaseBucketConfig.class);

        assertEquals(DefaultCouchbaseBucketConfig.PARTITION_NOT_EXISTENT, config.nodeIndexForMaster(24, false));
        assertEquals(DefaultCouchbaseBucketConfig.PARTITION_NOT_EXISTENT, config.nodeIndexForReplica(24, 1, false));
        assertFalse(config.ephemeral());
    }

    @Test
    public void shouldLoadEphemeralBucketConfig() throws Exception {
        String raw = Resources.read("ephemeral_bucket_config.json", getClass());
        CouchbaseBucketConfig config = JSON_MAPPER.readValue(raw, CouchbaseBucketConfig.class);
        assertTrue(config.ephemeral());
        assertTrue(config.serviceEnabled(ServiceType.BINARY));
        assertFalse(config.serviceEnabled(ServiceType.VIEW));
    }

    @Test
    public void shouldLoadConfigWithoutBucketCapabilities() throws Exception {
        String raw = Resources.read("config_without_capabilities.json", getClass());
        CouchbaseBucketConfig config = JSON_MAPPER.readValue(raw, CouchbaseBucketConfig.class);
        assertFalse(config.ephemeral());
        assertEquals(0, config.numberOfReplicas());
        assertEquals(64, config.numberOfPartitions());
        assertEquals(2, config.nodes().size());
        assertTrue(config.serviceEnabled(ServiceType.BINARY));
        assertTrue(config.serviceEnabled(ServiceType.VIEW));
    }

    @Test
    public void shouldLoadConfigWithSameNodesButDifferentPorts() throws Exception {
        String raw = Resources.read("cluster_run_two_nodes_same_host.json", getClass());
        CouchbaseBucketConfig config = JSON_MAPPER.readValue(raw, CouchbaseBucketConfig.class);
        assertFalse(config.ephemeral());
        assertEquals(1, config.numberOfReplicas());
        assertEquals(1024, config.numberOfPartitions());
        assertEquals(2, config.nodes().size());
        assertEquals("192.168.1.194", config.nodes().get(0).hostname().getHostAddress());
        assertEquals(9000, (int)config.nodes().get(0).services().get(ServiceType.CONFIG));
        assertEquals("192.168.1.194", config.nodes().get(1).hostname().getHostAddress());
        assertEquals(9001, (int)config.nodes().get(1).services().get(ServiceType.CONFIG));
    }
}