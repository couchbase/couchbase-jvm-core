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

import rx.Subscriber;
import rx.Subscription;

import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * The default implementation of a {@link StateZipper}.
 *
 * The implementing class only needs to provide the zip function, as well as a initial state that is always used
 * when no source stream is registered.
 *
 * @author Michael Nitschinger
 * @since 1.1.0
 */
public abstract class AbstractStateZipper<T, S extends Enum>
    extends AbstractStateMachine<S>
    implements StateZipper<T, S> {

    private final Map<T, Subscription> subscriptions;
    private final Map<T, S> states;
    private final S initialState;

    protected AbstractStateZipper(S initialState) {
        super(initialState);
        this.initialState = initialState;
        this.subscriptions = new ConcurrentHashMap<T, Subscription>();
        this.states = new ConcurrentHashMap<T, S>();
    }

    /**
     * The zip function to map from N states to one that represents the state of the zipper.
     *
     * @param states all subscribed states.
     * @return the zipped state which represents the zipper state.
     */
    protected abstract S zipWith(Collection<S> states);

    @Override
    public void register(final T identifier, final Stateful<S> upstream) {
        Subscription subscription = upstream.states().subscribe(new Subscriber<S>() {
            @Override
            public void onCompleted() {
                deregister(identifier);
            }

            @Override
            public void onError(Throwable error) {
                deregister(identifier);
            }

            @Override
            public void onNext(S state) {
                states.put(identifier, state);
                transitionStateThroughZipper();
            }
        });
        subscriptions.put(identifier, subscription);
    }

    @Override
    public void deregister(final T identifier) {
        if (identifier == null) {
            return;
        }

        Subscription subscription = subscriptions.get(identifier);
        if (subscription != null && !subscription.isUnsubscribed()) {
            subscription.unsubscribe();
            subscriptions.remove(identifier);
            states.remove(identifier);
            transitionStateThroughZipper();
        }
    }

    @Override
    public void terminate() {
        Iterator<T> iterator = subscriptions.keySet().iterator();
        while (iterator.hasNext()) {
            T identifier = iterator.next();
            Subscription subscription = subscriptions.get(identifier);
            if (subscription != null && !subscription.isUnsubscribed()) {
                subscription.unsubscribe();
                iterator.remove();
                states.remove(identifier);
            }
        }
        transitionStateThroughZipper();
    }

    /**
     * Ask the zip function to compute the states and then transition the state of the zipper.
     *
     * When no registrations are available, the zipper immediately transitions into the initial state
     * without asking the zip function for a computation.
     */
    private void transitionStateThroughZipper() {
        Collection<S> currentStates = states.values();
        if (currentStates.isEmpty()) {
            transitionState(initialState);
        } else {
            transitionState(zipWith(currentStates));
        }
    }

    /**
     * Helper method to export the current internal subscriptions.
     *
     * @return the internally stored subscriptions.
     */
    protected Map<T, Subscription> currentSubscriptions() {
        return subscriptions;
    }

    /**
     * Helper method to export the current internal states.
     *
     * Should only be used for testing.
     * @return the internally stored states.
     */
    protected Map<T, S> currentStates() {
        return states;
    }

}
