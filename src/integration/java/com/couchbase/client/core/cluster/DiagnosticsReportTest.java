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
package com.couchbase.client.core.cluster;

import com.couchbase.client.core.message.internal.EndpointHealth;
import com.couchbase.client.core.message.internal.DiagnosticsRequest;
import com.couchbase.client.core.message.internal.DiagnosticsResponse;
import com.couchbase.client.core.message.internal.DiagnosticsReport;
import com.couchbase.client.core.state.LifecycleState;
import com.couchbase.client.core.util.ClusterDependentTest;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Simple verification of the {@link DiagnosticsResponse}.
 *
 * @author Michael Nitschinger
 * @since 1.5.0
 */
public class DiagnosticsReportTest extends ClusterDependentTest {

    @BeforeClass
    public static void setup() throws Exception {
        connect();
    }

    @Test
    public void shouldExposeHealthInfoAfterConnect() {
        DiagnosticsResponse response = cluster()
            .<DiagnosticsResponse>send(new DiagnosticsRequest("diag-id-provided")).toBlocking().single();

        DiagnosticsReport sh = response.diagnosticsReport();
        assertNotNull(sh);

        List<EndpointHealth> eph = sh.endpoints();
        assertFalse(eph.isEmpty());

        for (EndpointHealth eh : eph) {
            assertNotNull(eh.type());
            assertEquals(LifecycleState.CONNECTED, eh.state());
            assertNotNull(eh.local());
            assertNotNull(eh.remote());
            if (!useMock()) {
                assertTrue(eh.lastActivity() > 0);
            }
            assertTrue(eh.id().startsWith("0x"));
        }

        assertNotNull(sh.sdk());
        assertEquals(sh.sdk(), env().userAgent());
        assertEquals("diag-id-provided", sh.id());

        assertNotNull(sh.exportToJson());
    }

}
