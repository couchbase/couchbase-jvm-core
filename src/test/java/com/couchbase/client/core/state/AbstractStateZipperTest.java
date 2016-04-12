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