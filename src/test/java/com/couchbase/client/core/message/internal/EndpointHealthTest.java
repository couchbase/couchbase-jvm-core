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

package com.couchbase.client.core.message.internal;

import com.couchbase.client.core.service.ServiceType;
import com.couchbase.client.core.state.LifecycleState;
import org.junit.Test;

import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

/**
 * Verifies the functionality of {@link EndpointHealth}.
 *
 * @author Michael Nitschinger
 * @since 1.5.2
 */
public class EndpointHealthTest {

    @Test
    public void shouldAllowInitOfLocalAndRemoteWithNull() {
        EndpointHealth eh = new EndpointHealth(
            ServiceType.QUERY,
            LifecycleState.CONNECTED,
            null,
            null,
            0,
            "0xdeadbeef"
        );

        assertNull(eh.local());
        assertNull(eh.remote());

        Map<String, Object> result = eh.toMap();
        assertEquals(0L, result.get("last_activity_us"));
        assertEquals("connected", result.get("state"));
        assertEquals("", result.get("remote"));
        assertEquals("", result.get("local"));
        assertEquals("0xdeadbeef", result.get("id"));
    }

}