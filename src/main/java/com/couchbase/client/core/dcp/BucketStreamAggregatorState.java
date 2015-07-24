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

import java.util.Arrays;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * State of the stream aggregator.
 * <p/>
 * It basically contains list of the stream states.
 *
 * @author Sergey Avseyev
 * @since 1.2.0
 */
@InterfaceStability.Experimental
@InterfaceAudience.Public
public class BucketStreamAggregatorState implements Iterable<BucketStreamState> {
    /**
     * Default state, which matches all changes in the stream.
     */
    public static final BucketStreamAggregatorState BLANK = new BucketStreamAggregatorState(0);

    private final PublishSubject<BucketStreamStateUpdatedEvent> updates;
    private BucketStreamState[] feeds;

    /**
     * Creates a new {@link BucketStreamAggregatorState}.
     *
     * @param feeds list containing state of each vBucket
     */
    public BucketStreamAggregatorState(final BucketStreamState[] feeds) {
        this.feeds = feeds;
        updates = PublishSubject.create();
    }

    /**
     * Creates a new {@link BucketStreamAggregatorState}.
     * <p/>
     * Initializes each entry with empty state BucketStreamState.BLANK. Note that it will throw
     * {@link IndexOutOfBoundsException} during set if requested partition index will not fit
     * the underlying container.
     *
     * @param numPartitions total number of states.
     */
    public BucketStreamAggregatorState(int numPartitions) {
        updates = PublishSubject.create();
        feeds = new BucketStreamState[numPartitions];
        Arrays.fill(feeds, BucketStreamState.BLANK);
    }

    /**
     * @return subject where all state updates posted.
     */
    public Observable<BucketStreamStateUpdatedEvent> updates() {
        return updates;
    }

    /**
     * Returns number of aggregated partitions.
     *
     * @return number of partitions.
     */
    public int numPartitions() {
        return feeds.length;
    }

    /**
     * Sets state for particular vBucket and notifies listener.
     *
     * @param partition vBucketID (partition number)
     * @param state     stream state
     * @throws IndexOutOfBoundsException if the state holder is BLANK, or allocated less
     *                                   partition slots then requested index.
     */
    public void set(int partition, final BucketStreamState state) {
        set(partition, state, true);
    }

    /**
     * Sets state for particular vBucket and optionally notifies listener.
     *
     * @param partition vBucketID (partition number)
     * @param state     stream state
     * @param notify    false if state notification should be skipped
     * @throws IndexOutOfBoundsException if the state holder is BLANK, or allocated less
     *                                   partition slots then requested index.
     */
    public void set(int partition, final BucketStreamState state, boolean notify) {
        feeds[partition] = state;
        if (notify) {
            updates.onNext(new BucketStreamStateUpdatedEvent(this, partition));
        }
    }

    /**
     * Replaces whole aggregator state and optionally notifies listener.
     *
     * @param feeds new state of partitions.
     */
    public void replace(final BucketStreamState[] feeds) {
        replace(feeds, true);
    }

    /**
     * Replaces whole aggregator state and optionally notifies listener.
     *
     * @param feeds  new state of partitions.
     * @param notify false if state notification should be skipped
     */
    public void replace(final BucketStreamState[] feeds, boolean notify) {
        this.feeds = feeds;
        if (notify) {
            updates.onNext(new BucketStreamStateUpdatedEvent(this));
        }
    }

    @Override
    public Iterator<BucketStreamState> iterator() {
        return new BucketStreamAggregatorStateIterator(feeds);
    }

    /**
     * Returns state for the vBucket
     *
     * @param partition vBucketID (partition number).
     * @return state or BucketStreamState.BLANK
     */
    public BucketStreamState get(int partition) {
        if (feeds.length > partition) {
            return feeds[partition];
        } else {
            return BucketStreamState.BLANK;
        }
    }

    /**
     * Helper class to iterate over {@link BucketStreamAggregatorState}.
     */
    public class BucketStreamAggregatorStateIterator implements Iterator<BucketStreamState> {
        private final BucketStreamState[] feeds;
        private int index;

        public BucketStreamAggregatorStateIterator(final BucketStreamState[] feeds) {
            this.feeds = feeds;
            this.index = 0;
        }

        @Override
        public boolean hasNext() {
            return index != feeds.length;
        }

        @Override
        public BucketStreamState next() {
            if (hasNext()) {
                return feeds[index++];
            } else {
                throw new NoSuchElementException("There are no elements. size = " + feeds.length);
            }
        }
    }
}
