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

import com.couchbase.client.core.logging.CouchbaseLogger;
import com.couchbase.client.core.logging.CouchbaseLoggerFactory;
import rx.Observable;
import rx.subjects.BehaviorSubject;

/**
 * Abstract {@link Stateful} implementation which acts like a simple state machine.
 *
 * This class is thread safe, so state transitions can be issued from any thread without any further synchronization.
 *
 * @author Michael Nitschinger
 * @since 1.0
 */
public class AbstractStateMachine<S extends Enum> implements Stateful<S> {

    /**
     * The logger used.
     */
    private static final CouchbaseLogger LOGGER = CouchbaseLoggerFactory.getInstance(Stateful.class);

    /**
     * The observable which emits all the subsequent state changes.
     */
    private final BehaviorSubject<S> observable;

    /**
     * The current state of the state machine.
     */
    private volatile S currentState;

    /**
     * Creates a new state machine.
     *
     * @param initialState the initial state of the state machine.
     */
    protected AbstractStateMachine(final S initialState) {
        currentState = initialState;
        observable = BehaviorSubject.create(currentState);
    }

    @Override
    public final Observable<S> states() {
        return observable;
    }

    @Override
    public final S state() {
        return currentState;
    }

    @Override
    public final boolean isState(final S state) {
        return currentState == state;
    }

    /**
     * Transition into a new state.
     *
     * This method is intentionally not public, because the subclass should only be responsible for the actual
     * transitions, the other components only react on those transitions eventually.
     *
     * @param newState the states to transition into.
     */
    protected void transitionState(final S newState) {
        if (newState != currentState) {
            if (LOGGER.isTraceEnabled()) {
                LOGGER.trace("State (" + getClass().getSimpleName() + ") " + currentState + " -> " + newState);
            }
            currentState = newState;
            observable.onNext(newState);
        }
    }

}
