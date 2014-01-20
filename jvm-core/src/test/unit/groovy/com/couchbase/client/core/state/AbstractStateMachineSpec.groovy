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

package com.couchbase.client.core.state

import com.couchbase.client.core.environment.CouchbaseEnvironment
import com.couchbase.client.core.environment.Environment
import reactor.function.Consumer
import spock.lang.Specification

import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

class AbstractStateMachineSpec extends Specification {

    def env = new CouchbaseEnvironment()
    def sm = new SimpleStateMachine(LifecycleState.DISCONNECTED, env)

    def "A StateMachine should be in an initial state"() {
        expect:
        sm.state() == LifecycleState.DISCONNECTED
        sm.isState(LifecycleState.DISCONNECTED) == true
        sm.isState(LifecycleState.DEGRADED) == false
    }

    def "A StateMachine should transition into a different state"() {
        when:
        sm.changeState(LifecycleState.CONNECTING)

        then:
        sm.state() == LifecycleState.CONNECTING
        sm.isState(LifecycleState.CONNECTING) == true
        sm.isState(LifecycleState.DISCONNECTED) == false
    }

    def "A StateMachien should send stream updates on transitions"() {
        setup:
        def expectedTransitions = 3
        def actualTransitions = new AtomicInteger(0);
        def latch = new CountDownLatch(1)
        def actualStates = Collections.synchronizedList(new ArrayList<LifecycleState>())
        sm.stateStream().collect(expectedTransitions).consume(new Consumer<List<LifecycleState>>() {
            @Override
            void accept(List<LifecycleState> lifecycleStates) {
                actualTransitions.set(lifecycleStates.size())
                actualStates.addAll(lifecycleStates)
                latch.countDown()
            }
        })

        when:
        sm.changeState(LifecycleState.CONNECTING)
        sm.changeState(LifecycleState.CONNECTED)
        sm.changeState(LifecycleState.DEGRADED)

        then:
        latch.await(1, TimeUnit.SECONDS) == true
        actualTransitions.get() == expectedTransitions
        actualStates == [LifecycleState.CONNECTING, LifecycleState.CONNECTED, LifecycleState.DEGRADED]
    }

    class SimpleStateMachine extends AbstractStateMachine<LifecycleState> {
        SimpleStateMachine(LifecycleState initialState, Environment env) {
            super(initialState, env)
        }

        public changeState(LifecycleState to) {
            transitionState(to);
        }
    }
}
