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
package com.couchbase.client.core.cluster;

import com.couchbase.client.core.BucketClosedException;
import com.couchbase.client.core.message.cluster.CloseBucketRequest;
import com.couchbase.client.core.message.kv.GetRequest;
import com.couchbase.client.core.util.ClusterDependentTest;
import org.junit.Test;

import static org.junit.Assert.fail;

/**
 * Short test ensuring that requests on a closed bucket fail fast with a
 * {@link BucketClosedException}
 *
 * @author Simon Baslé
 * @since 1.0.1
 */
public class BucketClosedTest extends ClusterDependentTest {

    @Test(expected = BucketClosedException.class)
    public void shouldFailFastOnRequestOnClosedBucket() {
        cluster().send(new CloseBucketRequest(bucket())).toBlocking().single();

        cluster().send(new GetRequest("someid", bucket())).toBlocking().first();
        fail();
    }

}
