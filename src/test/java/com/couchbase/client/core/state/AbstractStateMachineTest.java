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
package com.couchbase.client.core.state;

import org.junit.Test;
import rx.functions.Action1;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Verifies the functionality for the {@link AbstractStateMachine}.
 */
public class AbstractStateMachineTest {

    @Test
    public void shouldBeInInitialState() {
        SimpleStateMachine sm = new SimpleStateMachine(LifecycleState.DISCONNECTED);
        assertEquals(LifecycleState.DISCONNECTED, sm.state());
        assertTrue(sm.isState(LifecycleState.DISCONNECTED));
        assertFalse(sm.isState(LifecycleState.CONNECTING));
    }

    @Test
    public void shouldTransitionIntoDifferentState() {
        SimpleStateMachine sm = new SimpleStateMachine(LifecycleState.DISCONNECTED);

        sm.transitionState(LifecycleState.CONNECTING);
        assertEquals(LifecycleState.CONNECTING, sm.state());
        assertTrue(sm.isState(LifecycleState.CONNECTING));
    }

    @Test
    public void shouldSendTransitionToObserver() throws Exception {
        SimpleStateMachine sm = new SimpleStateMachine(LifecycleState.DISCONNECTED);

        final CountDownLatch latch = new CountDownLatch(3);
        final List<LifecycleState> states = Collections.synchronizedList(new ArrayList<LifecycleState>());
        sm.states().subscribe(new Action1<LifecycleState>() {
            @Override
            public void call(LifecycleState lifecycleState) {
                states.add(lifecycleState);
                latch.countDown();
            }
        });

        sm.transitionState(LifecycleState.CONNECTING);
        sm.transitionState(LifecycleState.CONNECTED);
        sm.transitionState(LifecycleState.DISCONNECTING);

        assertTrue(latch.await(1, TimeUnit.SECONDS));

        assertEquals(LifecycleState.CONNECTING, states.get(0));
        assertEquals(LifecycleState.CONNECTED, states.get(1));
        assertEquals(LifecycleState.DISCONNECTING, states.get(2));
    }

    @Test
    public void shouldNotReceiveOldTransitions() throws Exception {
        SimpleStateMachine sm = new SimpleStateMachine(LifecycleState.DISCONNECTED);

        final CountDownLatch latch = new CountDownLatch(2);
        final List<LifecycleState> states = Collections.synchronizedList(new ArrayList<LifecycleState>());

        sm.transitionState(LifecycleState.CONNECTING);

        sm.states().subscribe(new Action1<LifecycleState>() {
            @Override
            public void call(LifecycleState lifecycleState) {
                states.add(lifecycleState);
                latch.countDown();
            }
        });

        sm.transitionState(LifecycleState.CONNECTED);
        sm.transitionState(LifecycleState.DISCONNECTING);

        assertTrue(latch.await(1, TimeUnit.SECONDS));

        assertEquals(LifecycleState.CONNECTED, states.get(0));
        assertEquals(LifecycleState.DISCONNECTING, states.get(1));
    }

    class SimpleStateMachine extends AbstractStateMachine<LifecycleState> {

        public SimpleStateMachine(LifecycleState initialState) {
            super(initialState);
        }

        @Override
        public void transitionState(LifecycleState newState) {
            super.transitionState(newState);
        }
    }
}
