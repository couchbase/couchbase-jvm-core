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

import com.couchbase.client.core.endpoint.ResponseStatusConverter;
import com.couchbase.client.core.endpoint.kv.ErrorMap;
import com.couchbase.client.core.util.ClusterDependentTest;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.assertNotNull;

/**
 * Basic error map test
 *
 * @author Subhashni Balakrishnan
 * @since 1.4.5
 */
public class KeyValueErrorMapTest extends ClusterDependentTest {

    @BeforeClass
    public static void setup() throws Exception {
        connect(true);
    }

    @Test
    public void checkIfTheErrorMapIsRead() throws Exception {
        ErrorMap errMap = ResponseStatusConverter.getBinaryErrorMap();
        assertNotNull(errMap);
    }
}
