/**
 * Copyright (c) 2014 Couchbase, Inc.
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
