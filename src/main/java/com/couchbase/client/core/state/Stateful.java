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

import rx.Observable;

/**
 * A stateful component that changes its state and notifies subscribed parties.
 *
 * @author Michael Nitschinger
 * @since 1.0
 */
public interface Stateful<S extends Enum> {

    /**
     * Returns a infinite observable which gets updated when the state of the component changes.
     *
     * @return a {@link Observable} updated with state transitions.
     */
    Observable<S> states();

    /**
     * Returns the current state.
     *
     * @return the current state.
     */
    S state();

    /**
     * Check if the given state is the same as the current one.
     *
     * @param state the stats to check against.
     * @return true if it is the same, false otherwise.
     */
    boolean isState(S state);
}
