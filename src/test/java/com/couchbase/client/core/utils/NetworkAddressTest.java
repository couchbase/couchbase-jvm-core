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
package com.couchbase.client.core.utils;

import com.couchbase.client.core.util.TestProperties;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assume.assumeFalse;


/**
 * Verifies the functionality of the {@link NetworkAddress} wrapper class.
 */
public class NetworkAddressTest {

    @Test
    public void shouldCreateForLocalhost() {
        assumeFalse(TestProperties.isCi());

        NetworkAddress na = NetworkAddress.localhost();
        assertEquals("localhost", na.hostname());
        assertEquals("127.0.0.1", na.address());
        assertEquals("NetworkAddress{localhost/127.0.0.1, " +
            "fromHostname=false, reverseDns=true}", na.toString());
        assertEquals("localhost", na.nameOrAddress());
        assertEquals("127.0.0.1/localhost", na.nameAndAddress());
    }

    @Test
    public void shouldCreateFromHostname() {
        NetworkAddress na = NetworkAddress.create("localhost");
        assertEquals("localhost", na.hostname());
        assertEquals("127.0.0.1", na.address());
        assertEquals("NetworkAddress{localhost/127.0.0.1, " +
            "fromHostname=true, reverseDns=true}", na.toString());
        assertEquals("localhost", na.nameOrAddress());
        assertEquals("127.0.0.1/localhost", na.nameAndAddress());
    }

    @Test
    public void shouldCreateFromAddress() {
        assumeFalse(TestProperties.isCi());

        NetworkAddress na = NetworkAddress.create("127.0.0.1");
        assertEquals("localhost", na.hostname());
        assertEquals("127.0.0.1", na.address());
        assertEquals("NetworkAddress{localhost/127.0.0.1, " +
            "fromHostname=false, reverseDns=true}", na.toString());
        assertEquals("localhost", na.nameOrAddress());
        assertEquals("127.0.0.1/localhost", na.nameAndAddress());
    }

    @Test(expected = IllegalStateException.class)
    public void shouldAvoidReverseDnsLookup() {
        NetworkAddress na = new NetworkAddress("127.0.0.1", false);
        assertEquals("127.0.0.1", na.address());
        assertEquals("NetworkAddress{/127.0.0.1, " +
            "fromHostname=false, reverseDns=false}", na.toString());
        assertEquals("127.0.0.1", na.nameOrAddress());
        assertEquals("127.0.0.1", na.nameAndAddress());
        na.hostname(); // this will fail :-)
    }

    @Test
    public void shouldWorkIfHostnameAndDnsDisabled() {
        NetworkAddress na = new NetworkAddress("localhost", false);
        assertEquals("127.0.0.1", na.address());
        assertEquals("NetworkAddress{localhost/127.0.0.1, " +
            "fromHostname=true, reverseDns=false}", na.toString());
        assertEquals("localhost", na.hostname());
        assertEquals("localhost", na.nameOrAddress());
        assertEquals("127.0.0.1/localhost", na.nameAndAddress());
    }

    @Test
    public void shouldCreateFromIpv6Address() {
        NetworkAddress na = new NetworkAddress("[::1]", false);
        assertEquals("::1", na.address());
        assertEquals("0:0:0:0:0:0:0:1", na.hostname());
        assertEquals("0:0:0:0:0:0:0:1", na.nameOrAddress());
        assertEquals("::1/0:0:0:0:0:0:0:1", na.nameAndAddress());

        NetworkAddress na2 = new NetworkAddress("[2001:DB4:BAD:A55::5]", false);
        assertEquals("2001:DB4:BAD:A55::5".toLowerCase(), na2.address());
        assertEquals("2001:DB4:BAD:A55:0:0:0:5".toLowerCase(), na2.hostname());
        assertEquals("2001:DB4:BAD:A55:0:0:0:5".toLowerCase(), na2.nameOrAddress());
        assertEquals("2001:db4:bad:a55::5/2001:db4:bad:a55:0:0:0:5", na2.nameAndAddress());
    }
}