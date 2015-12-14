/*
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

package com.couchbase.client.core.dcp;

import com.couchbase.client.core.annotations.InterfaceAudience;
import com.couchbase.client.core.annotations.InterfaceStability;
import rx.Observable;
import rx.subjects.PublishSubject;
import rx.subjects.Subject;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * State of the stream aggregator.
 *
 * It contains list of the stream states.
 *
 * @author Sergey Avseyev
 * @since 1.2.0
 */
@InterfaceStability.Experimental
@InterfaceAudience.Public
public class BucketStreamAggregatorState implements Iterable<BucketStreamState> {
    private final Subject<BucketStreamStateUpdatedEvent, BucketStreamStateUpdatedEvent> updates =
            PublishSubject.<BucketStreamStateUpdatedEvent>create().toSerialized();
    private final Map<Short, BucketStreamState> feeds = new HashMap<Short, BucketStreamState>(1024);

    /**
     * Creates a new {@link BucketStreamAggregatorState}.
     */
    public BucketStreamAggregatorState() {
    }

    /**
     * @return subject where all state updates posted.
     */
    public Observable<BucketStreamStateUpdatedEvent> updates() {
        return updates;
    }

    /**
     * Sets state for particular vBucket and notifies listener.
     *
     * @param state stream state
     */
    public void put(final BucketStreamState state) {
        put(state, true);
    }

    /**
     * Sets state for particular vBucket and optionally notifies listener.
     *
     * @param state  stream state
     * @param notify false if state notification should be skipped
     */
    public void put(final BucketStreamState state, boolean notify) {
        feeds.put(state.partition(), state);
        if (notify) {
            updates.onNext(new BucketStreamStateUpdatedEvent(this, state));
        }
    }

    /**
     * Returns state for the partition
     *
     * @param partition vBucketID (partition number).
     * @return state or null if state of partition not tracked.
     */
    public BucketStreamState get(short partition) {
        return feeds.get(partition);
    }

    /**
     * Removes state for the partition.
     *
     * @param partition vBucketID (partition number).
     * @return state or null if state of partition not tracked.
     */
    public BucketStreamState remove(short partition) {
        return feeds.remove(partition);
    }

    public int size() {
        return feeds.size();
    }

    @Override
    public Iterator<BucketStreamState> iterator() {
        return feeds.values().iterator();
    }

    @Override
    public String toString() {
        return feeds.values().toString();
    }
}
