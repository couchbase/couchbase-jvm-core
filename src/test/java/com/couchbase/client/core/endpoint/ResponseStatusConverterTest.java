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

package com.couchbase.client.core.endpoint;

import com.couchbase.client.core.message.ResponseStatus;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Verifies the functionality of the {@link ResponseStatusConverter}.
 *
 * @author Michael Nitschinger
 * @author Simon Basle
 * @since 1.1.2
 */
public class ResponseStatusConverterTest {

    @Test
    public void shouldMapBinaryResponseCodes() {
        assertEquals(ResponseStatus.SUCCESS, ResponseStatusConverter.fromBinary((short) 0x00));
        assertEquals(ResponseStatus.NOT_EXISTS, ResponseStatusConverter.fromBinary((short) 0x01));
        assertEquals(ResponseStatus.EXISTS, ResponseStatusConverter.fromBinary((short) 0x02));
        assertEquals(ResponseStatus.TOO_BIG, ResponseStatusConverter.fromBinary((short) 0x03));
        assertEquals(ResponseStatus.INVALID_ARGUMENTS, ResponseStatusConverter.fromBinary((short) 0x04));
        assertEquals(ResponseStatus.NOT_STORED, ResponseStatusConverter.fromBinary((short) 0x05));
        assertEquals(ResponseStatus.INVALID_ARGUMENTS, ResponseStatusConverter.fromBinary((short) 0x06));
        assertEquals(ResponseStatus.RETRY, ResponseStatusConverter.fromBinary((short) 0x07));
        assertEquals(ResponseStatus.COMMAND_UNAVAILABLE, ResponseStatusConverter.fromBinary((short) 0x81));
        assertEquals(ResponseStatus.OUT_OF_MEMORY, ResponseStatusConverter.fromBinary((short) 0x82));
        assertEquals(ResponseStatus.COMMAND_UNAVAILABLE, ResponseStatusConverter.fromBinary((short) 0x83));
        assertEquals(ResponseStatus.INTERNAL_ERROR, ResponseStatusConverter.fromBinary((short) 0x84));
        assertEquals(ResponseStatus.SERVER_BUSY, ResponseStatusConverter.fromBinary((short) 0x85));
        assertEquals(ResponseStatus.TEMPORARY_FAILURE, ResponseStatusConverter.fromBinary((short) 0x86));

        assertEquals(ResponseStatus.FAILURE, ResponseStatusConverter.fromBinary(Short.MAX_VALUE));
    }
}