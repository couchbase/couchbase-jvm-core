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

/**
 * This event generated when {@link BucketStreamAggregatorState} is being updated.
 *
 * @author Sergey Avseyev
 * @since 1.2.0
 */
@InterfaceStability.Experimental
@InterfaceAudience.Public
public class BucketStreamStateUpdatedEvent {
    private final BucketStreamAggregatorState aggregatorState;
    private final BucketStreamState partitionState;

    /**
     * Creates {@link BucketStreamStateUpdatedEvent} which describes full update.
     *
     * @param aggregatorState state of the {@link BucketStreamAggregator}
     */
    public BucketStreamStateUpdatedEvent(final BucketStreamAggregatorState aggregatorState) {
        this(aggregatorState,  null);
    }

    /**
     * Creates {@link BucketStreamStateUpdatedEvent} which describes partial update.
     *
     * @param aggregatorState state of the {@link BucketStreamAggregator}
     * @param partitionState       new state for particular partition
     */
    public BucketStreamStateUpdatedEvent(final BucketStreamAggregatorState aggregatorState,
                                         final BucketStreamState partitionState) {
        this.aggregatorState = aggregatorState;
        this.partitionState = partitionState;
    }

    /**
     * Returns true event carries partial update.
     *
     * @return true for partial update
     */
    public boolean partialUpdate() {
        return partitionState != null;
    }

    /**
     * @return state of the {@link BucketStreamAggregator}
     */
    public BucketStreamAggregatorState aggregatorState() {
        return aggregatorState;
    }

    /**
     * @return index of partition which corresponds to updated stream state
     */
    public BucketStreamState partitionState() {
        return partitionState;
    }
}
