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

package com.couchbase.client.core.message.kv;

import com.couchbase.client.core.annotations.InterfaceStability;
import com.couchbase.client.core.message.observe.ObserveViaMutationToken;

import java.net.InetAddress;

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
    private final InetAddress hostname;

    public GetAllMutationTokensRequest(final InetAddress hostname, final String bucket) {
        this(PartitionState.ANY, hostname, bucket);
    }

    public GetAllMutationTokensRequest(final PartitionState partitionState, final InetAddress hostname, final String bucket) {
        super("", bucket);
        this.partitionState = partitionState;
        this.hostname = hostname;
    }

    public PartitionState partitionState() {
        return partitionState;
    }

    public InetAddress hostname() {
        return hostname;
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
