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

package com.couchbase.client.core.message.kv;

import com.couchbase.client.core.annotations.InterfaceStability;
import com.couchbase.client.core.message.observe.ObserveViaMutationToken;

/**
 * Get the current high sequence numbers one could initialize streams
 * for all partitions that are located on the server, or use in {@link ObserveViaMutationToken}.
 *
 * You may retrict the returned values to a certain vBucket state.
 * The state is supplied as an extra field and is fully optional.
 *
 * @author Sergey Avseyev
 * @since 1.2.2
 */
@InterfaceStability.Experimental
public class GetAllMutationTokensRequest extends AbstractKeyValueRequest {
    private final PartitionState partitionState;

    public GetAllMutationTokensRequest(final String bucket) {
        this(PartitionState.ANY, bucket);
    }

    public GetAllMutationTokensRequest(final PartitionState partitionState, final String bucket) {
        super("", bucket, null);
        this.partitionState = partitionState;
    }

    public PartitionState partitionState() {
        return partitionState;
    }

    @Override
    public short partition() {
        return DEFAULT_PARTITION;
    }

    public enum PartitionState {
        /** Any live state (except DEAD). */
        ANY(0),
        /** Actively servicing a partition. */
        ACTIVE(1),
        /** Servicing a partition as a replica only. */
        REPLICA(2),
        /** Pending active. */
        PENDING(3),
        /** Not in use, pending deletion. */
        DEAD(4);

        private final int value;

        PartitionState(int value) {
            this.value = value;
        }

        public int value() {
            return value;
        }
    }
}
