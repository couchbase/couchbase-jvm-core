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

package com.couchbase.client.core.service

import com.couchbase.client.core.endpoint.AbstractEndpointSpec
import com.couchbase.client.core.endpoint.BootstrapAdapter
import com.couchbase.client.core.endpoint.Endpoint
import com.couchbase.client.core.environment.CouchbaseEnvironment
import com.couchbase.client.core.environment.Environment
import com.couchbase.client.core.state.LifecycleState
import reactor.core.composable.spec.Promises
import reactor.event.registry.CachingRegistry
import reactor.event.registry.Registration
import reactor.event.registry.Registry
import reactor.event.selector.Selector
import spock.lang.Ignore;
import spock.lang.Specification


/**
 * Unit tests for a {@link Service}.
 */
public class AbstractServiceSpec extends Specification {

    def env = new CouchbaseEnvironment()
    def address = new InetSocketAddress(1234)
    def strategy = Mock(SelectionStrategy)
    def registry = Mock(CachingRegistry)
    def mockAdapter = Mock(BootstrapAdapter)
    def service = new DummyService(address, env, 1, strategy, registry, mockAdapter)

    def "A Service should be in a disconnect state after construction"() {
        expect:
        service.isState(LifecycleState.DISCONNECTED) == true
    }

    def "A Service should fail if no endpoint is configured to connect"() {
        setup:
        def registrations = new ArrayList<Registration<Endpoint>>()

        when:
        def state = service.connect()

        then:
        1 * registry.iterator() >> registrations.iterator()
        state.await() == LifecycleState.DISCONNECTED
    }

    def "A Service should connect successfully to one endpoint"() {
        setup:
        def registrations = new ArrayList<Registration<Endpoint>>()
        def registrationMock = Mock(Registration)
        def endpointMock = Mock(Endpoint)
        registrations.add(registrationMock)

        when:
        def state = service.connect()

        then:
        1 * registry.iterator() >> registrations.iterator()
        1 * registrationMock.object >> endpointMock
        1 * endpointMock.connect() >> Promises.success(LifecycleState.CONNECTED).get()
        state.await() == LifecycleState.CONNECTED
    }

    def "A Service should connect successfully to three endpoints"() {
        setup:
        def service = new DummyService(address, env, 3, strategy, registry, mockAdapter)
        def registrations = new ArrayList<Registration<Endpoint>>()
        def registrationMock1 = Mock(Registration)
        def registrationMock2 = Mock(Registration)
        def registrationMock3 = Mock(Registration)
        def endpointMock = Mock(Endpoint)
        registrations.add(registrationMock1)
        registrations.add(registrationMock2)
        registrations.add(registrationMock3)

        when:
        def state = service.connect()

        then:
        1 * registry.iterator() >> registrations.iterator()
        1 * registrationMock1.object >> endpointMock
        1 * registrationMock2.object >> endpointMock
        1 * registrationMock3.object >> endpointMock
        3 * endpointMock.connect() >> Promises.success(LifecycleState.CONNECTED).get()
        state.await() == LifecycleState.CONNECTED
    }

    @Ignore
    def "A Service should be in degraded state when not all endpoints are connected"() {

    }

    @Ignore
    def "A Service should disconnect all endpoints on disconnect"() {

    }

    @Ignore
    def "A Service should be degraded and again connected if one endpoint has to reconnect"() {

    }

    @Ignore
    def "A Service should swallow a duplicate connect attempt"() {

    }

    @Ignore
    def "A Service should swallow a connect attempt when already connected"() {

    }

    @Ignore
    def "A Service should swallow a duplicate disconnect attempt"() {

    }

    @Ignore
    def "A Service should swallow a disconnect attempt when already disconnected"() {

    }

    class DummyService extends AbstractService {

        private BootstrapAdapter adapter;

        DummyService(InetSocketAddress address, Environment env, int endpointCount, SelectionStrategy strategy,
            Registry registry, BootstrapAdapter adapter) {
            super(address, env, endpointCount, strategy, registry)
            this.adapter = adapter;
        }

        @Override
        protected Endpoint newEndpoint() {
            return new AbstractEndpointSpec.DummyEndpoint(adapter, environment())
        }
    }

}
