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

package com.couchbase.client.core.endpoint

import com.couchbase.client.core.environment.CouchbaseEnvironment
import com.couchbase.client.core.environment.Environment
import com.couchbase.client.core.message.CouchbaseRequest
import com.couchbase.client.core.state.LifecycleState
import com.couchbase.client.core.state.NotConnectedException
import io.netty.channel.ChannelPipeline
import io.netty.channel.embedded.EmbeddedChannel
import reactor.event.Event
import reactor.function.Consumer
import spock.lang.Ignore
import spock.lang.Specification

import java.util.concurrent.CountDownLatch

/**
 * Verifies generic functionality needed for all {@link Endpoint}s and provided through the {@link AbstractEndpoint}.
 */
class AbstractEndpointSpec extends Specification {

    def mockedBootstrap = Mock(BootstrapAdapter)
    def env = new CouchbaseEnvironment()
    def endpoint = new DummyEndpoint(mockedBootstrap, env)
    def channel = new EmbeddedChannel()

    def "A Endpoint should be disconnect after creation"() {
        expect:
        endpoint.state() == LifecycleState.DISCONNECTED
    }

    def "A Endpoint should connect successfully to the underlying channel"() {
        when:
        def connectPromise = endpoint.connect()

        then:
        1 * mockedBootstrap.connect() >> channel.newSucceededFuture()
        connectPromise.await() == LifecycleState.CONNECTED
    }

    def "A Endpoint should swallow duplicate connect attempts"() {
        setup:
        def promise = channel.newPromise()

        when:
        def firstAttempt = endpoint.connect()
        def secondAttempt = endpoint.connect()
        Thread.start {
            sleep(500)
            promise.setSuccess()
        }

        then:
        1 * mockedBootstrap.connect() >> promise
        firstAttempt.await() == LifecycleState.CONNECTED
        secondAttempt.await() == LifecycleState.CONNECTING
    }

    def "A Endpoint should swallow a connect attempt if already connected"() {
        when:
        def firstAttempt = endpoint.connect()

        then:
        1 * mockedBootstrap.connect() >> channel.newSucceededFuture()
        firstAttempt.await() == LifecycleState.CONNECTED

        when:
        def secondAttempt = endpoint.connect()

        then:
        0 * mockedBootstrap.connect()
        secondAttempt.await() == LifecycleState.CONNECTED
    }

    def "A Endpoint should disconnect if currently connected"() {
        when:
        def connectPromise = endpoint.connect()

        then:
        1 * mockedBootstrap.connect() >> channel.newSucceededFuture()
        connectPromise.await() == LifecycleState.CONNECTED

        when:
        def disconnectPromise = endpoint.disconnect()

        then:
        disconnectPromise.await() == LifecycleState.DISCONNECTED
    }

    def "A Endpoint should stop a connect attempt if disconnect overrides it"() {
        setup:
        def channelPromise = channel.newPromise()

        when:
        def connectPromise = endpoint.connect()
        def disconnectPromise = endpoint.disconnect()
        Thread.start {
            sleep(500)
            channelPromise.setSuccess()
        }

        then:
        1 * mockedBootstrap.connect() >> channelPromise
        connectPromise.await() == LifecycleState.DISCONNECTED
        disconnectPromise.await() == LifecycleState.DISCONNECTED
    }

    // TODO: implement either mocking or schedule in the embedded channel
    @Ignore
    def "A Endpoint should reconnect with backoff"() {
        expect:
        true == false
    }

    // TODO: implement either mocking or schedule in the embedded channel
    @Ignore
    def "A Endpoint should connect with backoff until disconnect stops it"() {
        expect:
        true == false
    }

    // TODO: implement either mocking or schedule in the embedded channel
    @Ignore
    def "A Endpoint should start reconnecting once notified from the underlying channel that it is inactive"() {
        expect:
        true == false
    }

    def "A Endpoint should send message into the channel if connected"() {
        when:
        def connectPromise = endpoint.connect()

        then:
        1 * mockedBootstrap.connect() >> channel.newSucceededFuture()
        connectPromise.await()

        when:
        def sendPromise = endpoint.send(new DummyRequest())
        channel.flush()

        then:
        channel.outboundMessages().size() == 1
        channel.readOutbound() instanceof Event
    }

    def "A Endpoint should respond with an exception if not connected but send is called"() {
        when:
        endpoint.send(new DummyRequest()).await()

        then:
        thrown(NotConnectedException)
    }

    def "A Endpoint should stream lifecycle states to its consumers"() {
        setup:
        def expectedTransitions = 4
        def states = endpoint.stateStream()
        def consumed = Collections.synchronizedList(new ArrayList<LifecycleState>())
        def latch = new CountDownLatch(expectedTransitions)
        states.consume(new Consumer<LifecycleState>() {
            @Override
            void accept(LifecycleState state) {
                consumed.add(state)
                latch.countDown()
            }
        })

        when:
        def connectState = endpoint.connect().await()
        def disconnectState = endpoint.disconnect().await()

        then:
        1 * mockedBootstrap.connect() >> channel.newSucceededFuture()
        latch.await()
        consumed == [LifecycleState.CONNECTING, LifecycleState.CONNECTED, LifecycleState.DISCONNECTING,
            LifecycleState.DISCONNECTED]
    }

    /**
     * A simple endpoint suitable for unit testing the specific parts.
     */
    static class DummyEndpoint extends AbstractEndpoint {

        DummyEndpoint(final BootstrapAdapter bootstrap, final Environment env) {
            super(bootstrap, env)
        }

        @Override
        protected void customEndpointHandlers(ChannelPipeline pipeline) { }

        @Override
        protected long flushInterval(Environment env) {
            return 0
        }
    }

    static class DummyRequest implements CouchbaseRequest {

    }
}
