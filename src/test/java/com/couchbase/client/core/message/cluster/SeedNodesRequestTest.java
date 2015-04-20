/**
 * Copyright (C) 2014 Couchbase, Inc.
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
package com.couchbase.client.core.message.cluster;

import com.couchbase.client.core.config.ConfigurationException;
import org.junit.Test;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Verifies the correct functionality of a {@link SeedNodesRequest}.
 *
 * @author Michael Nitschinger
 * @since 1.0
 */
public class SeedNodesRequestTest {

    @Test
    public void shouldConstructWithDefaultHostname() throws Exception {
        SeedNodesRequest request = new SeedNodesRequest();
        assertEquals(1, request.nodes().size());
        assertTrue(request.nodes().contains(InetAddress.getByName("localhost")));
    }

    @Test
    public void shouldConstructWithCustomHostname() throws Exception {
        SeedNodesRequest request = new SeedNodesRequest("127.0.0.1");
        assertEquals(1, request.nodes().size());
        assertTrue(request.nodes().contains(InetAddress.getByName("127.0.0.1")));
    }

    @Test
    public void shouldDeduplicateHosts() {
        SeedNodesRequest request = new SeedNodesRequest("127.0.0.1", "localhost");
        assertEquals(1, request.nodes().size());
    }

    @Test
    public void shouldProceedIfOnlyPartialFailure() {
        String invalidIp = "999.999.999.999";
        SeedNodesRequest request = new SeedNodesRequest("127.0.0.1", invalidIp);
        assertEquals(1, request.nodes().size());

        request = new SeedNodesRequest("127.0.0.1", "");
        assertEquals(1, request.nodes().size());
    }

    @Test(expected = ConfigurationException.class)
    public void shouldFailOnNullHostname() {
        List<String> nodes = null;
        new SeedNodesRequest(nodes);
    }

    @Test(expected = ConfigurationException.class)
    public void shouldFailOnEmptyHostname() {
        new SeedNodesRequest(new ArrayList<String>());
    }

    @Test(expected = ConfigurationException.class)
    public void shouldFailOnEmptyHostInVarargs() {
        new SeedNodesRequest("999.999.999.999", "");
    }

}