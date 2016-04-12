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