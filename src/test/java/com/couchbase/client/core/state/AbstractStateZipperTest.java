/**
 * Copyright (c) 2015 Couchbase, Inc.
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
package com.couchbase.client.core.state;

import org.junit.Test;
import rx.Subscription;

import java.util.Collection;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Verifies the functionality of the {@link AbstractStateZipper}.
 *
 * @author Michael Nitschinger
 * @since 1.1.0
 */
public class AbstractStateZipperTest {

    @Test
    public void shouldStartWithInitialState() {
        DummyStateZipper zipper = new DummyStateZipper(LifecycleState.DISCONNECTED);
        assertEquals(LifecycleState.DISCONNECTED, zipper.state());
        assertTrue(zipper.currentStates().isEmpty());
        assertTrue(zipper.currentSubscriptions().isEmpty());
    }

    @Test
    public void shouldRegisterObservable() throws Exception {
        DummyStateZipper zipper = new DummyStateZipper(LifecycleState.DISCONNECTED);

        Stateful<LifecycleState> first = new ModifyableStateMachine(LifecycleState.CONNECTED);
        assertFalse(first.hasSubscribers());

        zipper.register("first", first);

        assertTrue(first.hasSubscribers());
        assertEquals(1, zipper.currentStates().size());
        assertEquals(1, zipper.currentSubscriptions().size());
        assertEquals(LifecycleState.CONNECTED, zipper.state());
    }

    @Test
    public void shouldRegisterObservables() {
        DummyStateZipper zipper = new DummyStateZipper(LifecycleState.DISCONNECTED);

        Stateful<LifecycleState> first = new ModifyableStateMachine(LifecycleState.DISCONNECTED);
        zipper.register("first", first);

        assertEquals(1, zipper.currentStates().size());
        assertEquals(1, zipper.currentSubscriptions().size());
        assertEquals(LifecycleState.DISCONNECTED, zipper.state());

        Stateful<LifecycleState> second = new ModifyableStateMachine(LifecycleState.CONNECTING);
        zipper.register("second", second);

        assertEquals(2, zipper.currentStates().size());
        assertEquals(2, zipper.currentSubscriptions().size());
        assertEquals(LifecycleState.CONNECTING, zipper.state());
    }

    @Test
    public void shouldDeregisterObservable() {
        DummyStateZipper zipper = new DummyStateZipper(LifecycleState.DISCONNECTED);

        Stateful<LifecycleState> first = new ModifyableStateMachine(LifecycleState.CONNECTING);
        Stateful<LifecycleState> second = new ModifyableStateMachine(LifecycleState.CONNECTED);
        zipper.register("first", first);
        zipper.register("second", second);

        assertEquals(2, zipper.currentStates().size());
        assertEquals(2, zipper.currentSubscriptions().size());
        assertEquals(LifecycleState.CONNECTED, zipper.state());
        assertTrue(first.hasSubscribers());
        assertTrue(second.hasSubscribers());

        zipper.deregister("second");

        assertFalse(second.hasSubscribers());
        assertEquals(1, zipper.currentStates().size());
        assertEquals(1, zipper.currentSubscriptions().size());
        assertEquals(LifecycleState.CONNECTING, zipper.state());

        zipper.deregister("first");

        assertFalse(first.hasSubscribers());
        assertEquals(0, zipper.currentStates().size());
        assertEquals(0, zipper.currentSubscriptions().size());
        assertEquals(LifecycleState.DISCONNECTED, zipper.state());
    }

    @Test
    public void shouldTerminate() {
        DummyStateZipper zipper = new DummyStateZipper(LifecycleState.DEGRADED);

        Stateful<LifecycleState> first = new ModifyableStateMachine(LifecycleState.CONNECTING);
        Stateful<LifecycleState> second = new ModifyableStateMachine(LifecycleState.CONNECTED);
        zipper.register("first", first);
        zipper.register("second", second);

        assertEquals(2, zipper.currentStates().size());
        assertEquals(2, zipper.currentSubscriptions().size());
        assertEquals(LifecycleState.CONNECTED, zipper.state());
        assertTrue(first.hasSubscribers());
        assertTrue(second.hasSubscribers());

        zipper.terminate();

        assertFalse(first.hasSubscribers());
        assertFalse(second.hasSubscribers());
        assertEquals(0, zipper.currentStates().size());
        assertEquals(0, zipper.currentSubscriptions().size());
        assertEquals(LifecycleState.DEGRADED, zipper.state());
    }

    @Test
    public void shouldChangeStateOnPushes() {
        DummyStateZipper zipper = new DummyStateZipper(LifecycleState.DISCONNECTED);

        ModifyableStateMachine first = new ModifyableStateMachine(LifecycleState.DISCONNECTED);
        ModifyableStateMachine second = new ModifyableStateMachine(LifecycleState.DISCONNECTED);
        zipper.register("first", first);
        zipper.register("second", second);

        assertEquals(LifecycleState.DISCONNECTED, zipper.state());

        first.transitionState(LifecycleState.CONNECTING);
        assertEquals(LifecycleState.CONNECTING, zipper.state());

        second.transitionState(LifecycleState.CONNECTED);
        assertEquals(LifecycleState.CONNECTED, zipper.state());
    }

    class DummyStateZipper extends AbstractStateZipper<String, LifecycleState> {

        public DummyStateZipper(LifecycleState initialState) {
            super(initialState);
        }

        @Override
        protected LifecycleState zipWith(Collection<LifecycleState> states) {
            int connected = 0;
            int connecting = 0;
            for (LifecycleState state : states) {
                switch(state) {
                    case CONNECTED:
                        connected++;
                        break;
                    case CONNECTING:
                        connecting++;
                        break;
                }
            }
            if (connected > 0) {
                return LifecycleState.CONNECTED;
            } else if (connecting > 0) {
                return LifecycleState.CONNECTING;
            }
            return LifecycleState.DISCONNECTED;
        }

        @Override
        public Map<String, LifecycleState> currentStates() {
            return super.currentStates();
        }

        @Override
        public Map<String, Subscription> currentSubscriptions() {
            return super.currentSubscriptions();
        }
    }

    class ModifyableStateMachine extends AbstractStateMachine<LifecycleState> {
        public ModifyableStateMachine(LifecycleState initialState) {
            super(initialState);
        }

        @Override
        public void transitionState(LifecycleState newState) {
            super.transitionState(newState);
        }
    }

}