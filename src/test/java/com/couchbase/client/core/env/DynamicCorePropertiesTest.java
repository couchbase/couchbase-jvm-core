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

/**
 * .
 *
 * @author Michael Nitschinger
 */
public class DynamicCorePropertiesTest {

    @Test
    public void shouldLoadDefaults() {
        CoreProperties props = DynamicCoreProperties.create();
        assertEquals(DefaultCoreProperties.BINARY_ENDPOINTS, props.binaryServiceEndpoints());
        assertEquals(DefaultCoreProperties.IO_POOL_SIZE, props.ioPoolSize());
    }

    @Test
    public void builderShouldTakePrecedence() {
        CoreProperties props = DynamicCoreProperties
            .builder()
            .ioPoolSize(123)
            .build();

        assertEquals(DefaultCoreProperties.BINARY_ENDPOINTS, props.binaryServiceEndpoints());
        assertEquals(123, props.ioPoolSize());
    }

    @Test
    public void sysPropertyShouldTakePrecedence() {
        System.setProperty("com.couchbase.ioPoolSize", "456");

        CoreProperties props = DynamicCoreProperties
            .builder()
            .ioPoolSize(123)
            .build();

        assertEquals(DefaultCoreProperties.BINARY_ENDPOINTS, props.binaryServiceEndpoints());
        assertEquals(456, props.ioPoolSize());

        System.clearProperty("com.couchbase.ioPoolSize");
    }
}
