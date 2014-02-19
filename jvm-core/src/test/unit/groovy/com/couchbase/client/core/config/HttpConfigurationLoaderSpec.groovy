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

package com.couchbase.client.core.config

import com.couchbase.client.core.cluster.Cluster
import com.couchbase.client.core.environment.CouchbaseEnvironment
import com.couchbase.client.core.message.config.GetBucketConfigRequest
import com.couchbase.client.core.message.config.GetBucketConfigResponse
import reactor.core.composable.spec.Promises
import spock.lang.Specification

class HttpConfigurationLoaderSpec extends Specification {

    def env = new CouchbaseEnvironment()
    def node = new InetSocketAddress(8091)
    def bucket = "foo"
    def password = "bar"
    def clusterMock = Mock(Cluster)
    def loader = new HttpConfigurationLoader(env, node, bucket, password, clusterMock)

    def "A HttpConfigurationLoader should load the raw config from the cluster"() {
        when:
        def rawConfigPromise = loader.loadRawConfig()

        then:
        1 * clusterMock.send(_ as GetBucketConfigRequest) >> Promises.success(new GetBucketConfigResponse("foo")).get()
        rawConfigPromise.await() == "foo"
    }

}
