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
package com.couchbase.client.core.env;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class DefaultCoreEnvironmentTest {

    @Test
    public void shouldInitAndShutdownCoreEnvironment() throws Exception {
        CoreEnvironment env = DefaultCoreEnvironment.create();
        assertNotNull(env.ioPool());
        assertNotNull(env.scheduler());

        assertEquals(DefaultCoreEnvironment.KEYVALUE_ENDPOINTS, env.kvEndpoints());
        assertTrue(env.shutdown().toBlocking().single());
    }

    @Test
    public void shouldOverrideDefaults() throws Exception {
        CoreEnvironment env = DefaultCoreEnvironment
            .builder()
            .kvEndpoints(3)
            .build();
        assertNotNull(env.ioPool());
        assertNotNull(env.scheduler());

        assertEquals(3, env.kvEndpoints());
        assertTrue(env.shutdown().toBlocking().single());
    }

    @Test
    public void sysPropertyShouldTakePrecedence() throws Exception {

        System.setProperty("com.couchbase.kvEndpoints", "10");

        CoreEnvironment env = DefaultCoreEnvironment
            .builder()
            .kvEndpoints(3)
            .build();
        assertNotNull(env.ioPool());
        assertNotNull(env.scheduler());

        assertEquals(10, env.kvEndpoints());
        assertTrue(env.shutdown().toBlocking().single());

        System.clearProperty("com.couchbase.kvEndpoints");
    }

    @Test
    public void shouldApplyMinPoolSize() throws Exception {
        CoreEnvironment env = DefaultCoreEnvironment
                .builder()
                .ioPoolSize(1)
                .computationPoolSize(1)
                .build();

        assertEquals(DefaultCoreEnvironment.MIN_POOL_SIZE, env.ioPoolSize());
        assertEquals(DefaultCoreEnvironment.MIN_POOL_SIZE, env.computationPoolSize());
    }

}
