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

package com.couchbase.client.core.cluster

import com.couchbase.client.core.config.Configuration
import com.couchbase.client.core.config.ConfigurationManager
import com.couchbase.client.core.environment.CouchbaseEnvironment
import com.couchbase.client.core.message.common.ConnectRequest
import com.couchbase.client.core.message.common.ConnectResponse
import com.couchbase.client.core.message.internal.AddNodeRequest
import com.couchbase.client.core.message.internal.AddNodeResponse
import com.couchbase.client.core.message.internal.DisableServiceRequest
import com.couchbase.client.core.message.internal.DisableServiceResponse
import com.couchbase.client.core.message.internal.EnableServiceRequest
import com.couchbase.client.core.message.internal.EnableServiceResponse
import com.couchbase.client.core.message.internal.RemoveNodeRequest
import com.couchbase.client.core.message.internal.RemoveNodeResponse
import com.couchbase.client.core.node.CouchbaseNode
import com.couchbase.client.core.service.ServiceType
import com.couchbase.client.core.state.LifecycleState
import reactor.core.composable.Promise
import reactor.core.composable.spec.Promises
import reactor.event.registry.Registration
import reactor.event.registry.Registry
import spock.lang.Specification

class CouchbaseClusterSpec extends Specification {

    def env = new CouchbaseEnvironment()
    def configManager = Mock(ConfigurationManager)
    def registry = Mock(Registry)
    def cluster = new CouchbaseCluster(env, configManager, registry)

    def "The initial state should be DISCONNECTED"() {
        expect:
        cluster.state() == LifecycleState.DISCONNECTED
    }

    def "A CouchbaseCluster should add a node if instructed"() {
        when:
        def responsePromise = cluster.send(new AddNodeRequest(new InetSocketAddress(11210)));

        then:
        1 * registry.select(_) >> new ArrayList<Registration<Node>>()
        1 * registry.register(_, _)
        def response = (AddNodeResponse) responsePromise.await()
        response.status == AddNodeResponse.Status.ADDED
        response.message == "Successfully added node to registry."
    }

    def "A CouchbaseCluster should not add a node twice"() {
        setup:
        def registries = new ArrayList<Registration<Node>>()
        def request = new AddNodeRequest(new InetSocketAddress(11210))

        when:
        def responsePromise = cluster.send(request);

        then:
        1 * registry.select(_) >> registries
        1 * registry.register(_, _) >> {
            def reg = Mock(Registration)
            registries.add(reg)
            reg
        }

        when:
        responsePromise = cluster.send(request);

        then:
        1 * registry.select(_) >> registries
        0 * registry.register(_, _)
        def response = (AddNodeResponse) responsePromise.await()
        response.status == AddNodeResponse.Status.ADDED
        response.message == "Successfully added node to registry."
    }

    def "A CouchbaseCluster should ignore node removal if it has not been added before"() {
        setup:
        def request = new RemoveNodeRequest(new InetSocketAddress(11210))

        when:
        def responsePromise = cluster.send(request)

        then:
        1 * registry.select(_) >> new ArrayList<Registration<Node>>()
        def response = (RemoveNodeResponse) responsePromise.await()
        response.status == RemoveNodeResponse.Status.REMOVED
        response.message == "Successfully removed node from registry."
    }

    def "A CouchbaseCluster should remove and shutdown a node"() {
        setup:
        def registration = Mock(Registration)
        def node = Mock(CouchbaseNode)
        def registrations = new ArrayList<Registration<Node>>()
        registrations.add(registration)
        def request = new RemoveNodeRequest(new InetSocketAddress(11210))

        when:
        def responsePromise = cluster.send(request)

        then:
        2 * registry.select(_) >> registrations
        1 * registration.getObject() >> node
        1 * node.shutdown() >> Promises.success(new Boolean(true)).get()
        1 * registry.unregister(_) >> true
        def response = (RemoveNodeResponse) responsePromise.await()
        response.status == RemoveNodeResponse.Status.REMOVED
        response.message == "Successfully removed node from registry."
    }

    def "A CouchbaseCluster should change its state with the node states"() {
        // handle cluster state changes first!
    }

    def "A CouchbaseCluster should dispatch a connect message"() {
        // to be added with mocking of connect bootstrap process (succes and failure!)
    }

    def "A CouchbaseCluster should return with message if no node is found for service enable"() {
        setup:
        def request = new EnableServiceRequest(new InetSocketAddress(11210), ServiceType.BINARY)

        when:
        def responsePromise = cluster.send(request)

        then:
        1 * registry.select(_) >> new ArrayList<Registration<Node>>()
        def response = (EnableServiceResponse) responsePromise.await()
        response.status == EnableServiceResponse.Status.NOT_ENABLED
        response.message == "No node found to enable the service for."
    }

    def "A CouchbaseCluster should pass on enable service to found node"() {
        setup:
        def registration = Mock(Registration)
        def node = Mock(CouchbaseNode)
        def registrations = new ArrayList<Registration<Node>>()
        registrations.add(registration)

        when:
        def enablePromise = cluster.send(new EnableServiceRequest(new InetSocketAddress(11210), ServiceType.BINARY))

        then:
        2 * registry.select(_) >> registrations
        1 * registration.getObject() >> node
        1 * node.enableService(_) >> Promises.success(EnableServiceResponse.serviceEnabled()).get()
        def response = (EnableServiceResponse) enablePromise.await()
        response.status == EnableServiceResponse.Status.ENABLED
        response.message == "Successfully enabled service."
    }

    def "A CouchbaseCluster should return with message if no node is found for service disable"() {
        setup:
        def request = new DisableServiceRequest(new InetSocketAddress(11210))

        when:
        def responsePromise = cluster.send(request)

        then:
        1 * registry.select(_) >> new ArrayList<Registration<Node>>()
        def response = (DisableServiceResponse) responsePromise.await()
        response.status == DisableServiceResponse.Status.NOT_DISABLED
        response.message == "No node found to disable the service for."
    }

    def "A CouchbaseCluster should pass on disable service to found node"() {
        setup:
        def registration = Mock(Registration)
        def node = Mock(CouchbaseNode)
        def registrations = new ArrayList<Registration<Node>>()
        registrations.add(registration)

        when:
        def disablePromise = cluster.send(new DisableServiceRequest(new InetSocketAddress(11210)))

        then:
        2 * registry.select(_) >> registrations
        1 * registration.getObject() >> node
        1 * node.disableService(_) >> Promises.success(DisableServiceResponse.serviceDisabled()).get()
        def response = (DisableServiceResponse) disablePromise.await()
        response.status == DisableServiceResponse.Status.DISABLED
        response.message == "Successfully disabled service."
    }

    def "A CouchbaseCluster should respond with success to a bucket connect attempt"() {
        setup:
        def config = Mock(Configuration)

        when:
        def connectPromise = cluster.send(new ConnectRequest())

        then:
        1 * configManager.connect(_, _, _) >> Promises.success(config).get()
        def response = (ConnectResponse) connectPromise.await()
        response.message == "Successfully connected to this bucket."
        response.status == ConnectResponse.Status.CONNECTED
    }

    def "A CouchbaseCluster should propagate a bucket connect failure to the caller"() {
        setup:
        def config = Mock(Configuration)

        when:
        cluster.send(new ConnectRequest()).await()

        then:
        1 * configManager.connect(_, _, _) >> Promises.error(new Exception("Some Error"))
        thrown(Exception)
    }

}
