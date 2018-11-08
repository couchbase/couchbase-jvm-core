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

import com.couchbase.client.core.message.internal.PingReport;
import com.couchbase.client.core.message.internal.PingServiceHealth;
import com.couchbase.client.core.service.ServiceType;
import com.couchbase.client.core.util.ClusterDependentTest;
import com.couchbase.client.core.utils.HealthPinger;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class HealthPingerTest extends ClusterDependentTest {

    @BeforeClass
    public static void setup() throws Exception {
        connect();
    }

    @Test
    public void shouldPerformPingAfterConnect() {
        PingReport pr = HealthPinger.ping(
            env(),
            bucket(),
            password(),
            cluster(),
            "ping-id",
            1,
            TimeUnit.SECONDS
        ).toBlocking().value();

        assertNotNull(pr.sdk());
        assertEquals(pr.sdk(), env().userAgent());
        assertEquals("ping-id", pr.id());
        assertTrue(pr.version() > 0);
        if (!useMock()) {
            // The mock currently does not contain a rev in the config
            assertTrue(pr.configRev() > 0);
        }
        assertNotNull(pr.exportToJson());

        for (PingServiceHealth ph : pr.services()) {
            assertEquals(PingServiceHealth.PingState.OK, ph.state());
            assertTrue(ph.latency() > 0);
            assertNotNull(ph.id());
            assertNotNull(ph.remote());
            assertNotNull(ph.local());
            if (ph.type() == ServiceType.BINARY) {
                assertNotNull(ph.scope());
            } else {
                assertNull(ph.scope());
            }
        }
    }
}
