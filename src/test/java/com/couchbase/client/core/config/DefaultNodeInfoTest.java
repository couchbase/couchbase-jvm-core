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

import com.couchbase.client.core.env.NetworkResolution;
import com.couchbase.client.core.service.ServiceType;
import com.couchbase.client.core.utils.NetworkAddress;
import org.junit.Test;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assume.assumeFalse;

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
        DefaultNodeInfo info = new DefaultNodeInfo(viewBase, "localhost:8091", ports, null);

        assertEquals(2, info.services().size());
        assertEquals(8091, (long) info.services().get(ServiceType.CONFIG));
        assertEquals(8092, (long) info.services().get(ServiceType.VIEW));
    }

    @Test
    public void shouldNotExposeViewServiceWhenNotAvailable() {
        Map<String, Integer> ports = new HashMap<String, Integer>();
        DefaultNodeInfo info = new DefaultNodeInfo(null, "localhost:8091", ports, null);

        assertEquals(1, info.services().size());
        assertEquals(8091, (long) info.services().get(ServiceType.CONFIG));
    }

    @Test
    public void shouldExposeRawHostnameFromConstruction() {
        assertEquals(
            "localhost",
            new DefaultNodeInfo(null, "localhost:8091", new HashMap<String, Integer>(), null).rawHostname()
        );

        assertEquals(
            "127.0.0.1",
            new DefaultNodeInfo(null, "127.0.0.1:8091", new HashMap<String, Integer>(), null).rawHostname()
        );
    }

    @Test
    public void shouldHandleIPv6() {
        assumeFalse(NetworkAddress.FORCE_IPV4);

        Map<String, Integer> ports = new HashMap<String, Integer>();
        DefaultNodeInfo info = new DefaultNodeInfo(null, "[fd63:6f75:6368:2068:c490:b5ff:fe86:9cf7]:8091", ports, null);

        assertEquals(1, info.services().size());
        assertEquals("fd63:6f75:6368:2068:c490:b5ff:fe86:9cf7", info.hostname().address());
        assertEquals(8091, (long) info.services().get(ServiceType.CONFIG));
    }

    @Test
    public void shouldUseExposeAlternateIfSet() {
        Map<String, Integer> ports = new HashMap<String, Integer>();
        ports.put("direct", 1234);
        Map<String, Integer> externalPorts = new HashMap<String, Integer>();
        externalPorts.put("kv", 5678);

        Map<String, AlternateAddress> alternate = new HashMap<String, AlternateAddress>();
        alternate.put(NetworkResolution.EXTERNAL.name(), new DefaultAlternateAddress("5.6.7.8", externalPorts));

        NodeInfo nodeInfo = new DefaultNodeInfo(null, "1.2.3.4:8091", ports, alternate);
        nodeInfo.useAlternateNetwork(NetworkResolution.EXTERNAL.name());
        assertEquals("5.6.7.8", nodeInfo.rawHostname());
        assertEquals(NetworkAddress.create("5.6.7.8"), nodeInfo.hostname());
        assertEquals(5678, (int) nodeInfo.services().get(ServiceType.BINARY));
    }

    @Test
    public void shouldNotUseExposeAlternateIfNotSet() {
        Map<String, Integer> ports = new HashMap<String, Integer>();
        ports.put("direct", 1234);
        Map<String, Integer> externalPorts = new HashMap<String, Integer>();
        externalPorts.put("kv", 5678);

        Map<String, AlternateAddress> alternate = new HashMap<String, AlternateAddress>();
        alternate.put(NetworkResolution.EXTERNAL.name(), new DefaultAlternateAddress("5.6.7.8", externalPorts));

        NodeInfo nodeInfo = new DefaultNodeInfo(null, "1.2.3.4:8091", ports, alternate);
        assertEquals("1.2.3.4", nodeInfo.rawHostname());
        assertEquals(NetworkAddress.create("1.2.3.4"), nodeInfo.hostname());
        assertEquals(1234, (int) nodeInfo.services().get(ServiceType.BINARY));
    }

    @Test
    public void shouldUseInternalPortsIfAlternateNotPresent() {
        Map<String, Integer> ports = new HashMap<String, Integer>();
        ports.put("direct", 1234);
        Map<String, Integer> externalPorts = new HashMap<String, Integer>();

        Map<String, AlternateAddress> alternate = new HashMap<String, AlternateAddress>();
        alternate.put(NetworkResolution.EXTERNAL.name(), new DefaultAlternateAddress("5.6.7.8", externalPorts));

        NodeInfo nodeInfo = new DefaultNodeInfo(null, "1.2.3.4:8091", ports, alternate);
        nodeInfo.useAlternateNetwork(NetworkResolution.EXTERNAL.name());
        assertEquals("5.6.7.8", nodeInfo.rawHostname());
        assertEquals(NetworkAddress.create("5.6.7.8"), nodeInfo.hostname());
        assertEquals(1234, (int) nodeInfo.services().get(ServiceType.BINARY));
    }

}
