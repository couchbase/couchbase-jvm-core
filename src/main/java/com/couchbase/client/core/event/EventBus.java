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
package com.couchbase.client.core.event;

import rx.Observable;

/**
 * Defines the interface for a generic event bus.
 *
 * @author Michael Nitschinger
 * @since 1.1.0
 */
public interface EventBus {

    /**
     * Subscribe to the event bus to retrieve {@link CouchbaseEvent}s.
     *
     * @return the observable where the events are emitted into.
     */
    Observable<CouchbaseEvent> get();

    /**
     * Publish a {@link CouchbaseEvent} into the bus.
     *
     * @param event the event to publish.
     */
    void publish(CouchbaseEvent event);

    /**
     * Checks if the event bus has subscribers.
     *
     * This method can be utilized on the publisher side to avoid complex event creation when there is no one
     * on the other side listening and the event would be discarded immediately afterwards.
     *
     * @return true if it has subscribers, false otherwise.
     */
    boolean hasSubscribers();

}
