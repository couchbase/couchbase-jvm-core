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
import com.couchbase.client.core.cluster.CouchbaseCluster
import com.couchbase.client.core.environment.CouchbaseEnvironment
import com.couchbase.client.core.environment.Environment
import com.couchbase.client.core.message.internal.AddNodeRequest
import com.couchbase.client.core.message.internal.AddNodeResponse
import com.couchbase.client.core.message.internal.EnableServiceRequest
import com.couchbase.client.core.message.internal.EnableServiceResponse
import com.couchbase.client.core.service.ServiceType
import reactor.core.composable.Promise
import reactor.core.composable.spec.Promises
import reactor.function.Function;
import spock.lang.Specification;

public class AbstractConfigurationLoaderSpec extends Specification {

    def env = new CouchbaseEnvironment()
    def serviceType = ServiceType.BINARY
    def node = new InetSocketAddress(11210)
    def bucket = "default"
    def password = ""
    def cluster = Mock(Cluster)
    def loader = new DummyConfigurationLoader(env, serviceType, node, bucket, password, cluster)

    def "A ConfigurationLoader should add nodes and enable services"() {
        when:
        def configPromise = loader.load()

        then:
        1 * cluster.send(_ as AddNodeRequest) >> Promises.success(AddNodeResponse.nodeAdded()).get()
        1 * cluster.send(_ as EnableServiceRequest) >> Promises.success(EnableServiceResponse.serviceEnabled()).get()
        def config = configPromise.await()
        config == "content"
    }


    def "aa"() {
        setup:
        def cluster = new CouchbaseCluster(env)
        def loader = new DummyConfigurationLoader(env, serviceType, node, bucket, password, cluster)

        expect:
        loader.load()

    }


    /*
    def "A ConfigurationLoader should fail when a node could not be added"() {
        when:
        def configPromise = loader.load()

        then:
        1 * cluster.send(_ as AddNodeRequest) >> Promises.error(new Exception("Something went wrong")).get()
       // 1 * cluster.send(_ as AddNodeRequest) >> Promises.success(AddNodeResponse.nodeAdded()).get()
        System.out.println(configPromise.await())
    }


    def "A ConfigurationLoader should fail when a service could not be enabled"() {
        when:
        def configPromise = loader.load()

        then:
        1 * cluster.send(_ as AddNodeRequest) >> Promises.success(AddNodeResponse.nodeAdded()).get()
        1 * cluster.send(_ as EnableServiceRequest) >> Promises.error(new Exception("Something went wrong")).get()
    }
    */

    static class DummyConfigurationLoader extends AbstractConfigurationLoader {

        DummyConfigurationLoader(Environment env, ServiceType serviceType, InetSocketAddress node, String bucket,
            String password, Cluster cluster) {
            super(env, serviceType, node, bucket, password, cluster)
        }

        @Override
        def Promise<String> loadRawConfig() {
            return Promises.success("content").get()
        }
    }
}
