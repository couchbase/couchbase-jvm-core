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

package com.couchbase.client.core.message.observe;

import com.couchbase.client.core.ClusterFacade;
import com.couchbase.client.core.annotations.InterfaceAudience;
import com.couchbase.client.core.annotations.InterfaceStability;
import com.couchbase.client.core.message.kv.MutationToken;
import com.couchbase.client.core.retry.RetryStrategy;
import com.couchbase.client.core.time.Delay;
import io.opentracing.Span;
import rx.Observable;

import java.util.concurrent.TimeUnit;

/**
 * Utility class to handle observe calls and polling logic.
 *
 * @author Michael Nitschinger
 * @since 1.0.1
 */
@InterfaceStability.Uncommitted
@InterfaceAudience.Private
public class Observe {

    /**
     * The default observe delay for backwards compatibility.
     */
    private static final Delay DEFAULT_DELAY = Delay.fixed(10, TimeUnit.MILLISECONDS);

    /**
     * Defines the possible disk persistence constraints to observe.
     *
     * @author Michael Nitschinger
     * @since 1.0.1
     */
    public enum PersistTo {
        /**
         * Observe disk persistence to the master node of the document only.
         */
        MASTER((short) -1),

        /**
         * Do not observe any disk persistence constraint.
         */
        NONE((short) 0),

        /**
         * Observe disk persistence of one node (master or replica).
         */
        ONE((short) 1),

        /**
         * Observe disk persistence of two nodes (master or replica).
         */
        TWO((short) 2),

        /**
         * Observe disk persistence of three nodes (master or replica).
         */
        THREE((short) 3),

        /**
         * Observe disk persistence of four nodes (one master and three replicas).
         */
        FOUR((short) 4);

        /**
         * Contains the internal value to map onto.
         */
        private final short value;

        /**
         * Internal constructor for the enum.
         *
         * @param value the value of the persistence constraint.
         */
        PersistTo(short value) {
            this.value = value;
        }

        /**
         * Returns the actual internal persistence representation for the enum.
         *
         * @return the internal persistence representation.
         */
        public short value() {
            return value;
        }

        /**
         * Identifies if this enum property will touch a replica or just the master.
         *
         * @return true if it includes a replica, false if not.
         */
        public boolean touchesReplica() {
            return value > 0;
        }
    }

    /**
     * Defines the possible replication constraints to observe.
     *
     * @author Michael Nitschinger
     * @since 1.0.1
     */
    public enum ReplicateTo {

        /**
         * Do not observe any replication constraint.
         */
        NONE((short) 0),

        /**
         * Observe replication to one replica.
         */
        ONE((short) 1),

        /**
         * Observe replication to two replicas.
         */
        TWO((short) 2),

        /**
         * Observe replication to three replicas.
         */
        THREE((short) 3);

        /**
         * Contains the internal value to map onto.
         */
        private final short value;

        /**
         * Internal constructor for the enum.
         *
         * @param value the value of the replication constraint.
         */
        ReplicateTo(short value) {
            this.value = value;
        }

        /**
         * Returns the actual internal replication representation for the enum.
         *
         * @return the internal replication representation.
         */
        public short value() {
            return value;
        }

        /**
         * Identifies if this enum property will touch a replica or just the master.
         *
         * @return true if it includes a replica, false if not.
         */
        public boolean touchesReplica() {
            return value > 0;
        }
    }

    public static Observable<Boolean> call(final ClusterFacade core, final String bucket, final String id,
        final long cas, final boolean remove, final PersistTo persistTo, final ReplicateTo replicateTo,
        final RetryStrategy retryStrategy) {
        return call(core, bucket, id, cas, remove, persistTo, replicateTo, DEFAULT_DELAY, retryStrategy);
    }

    public static Observable<Boolean> call(final ClusterFacade core, final String bucket, final String id,
        final long cas, final boolean remove, final PersistTo persistTo, final ReplicateTo replicateTo,
        final Delay delay, final RetryStrategy retryStrategy) {
        return call(core, bucket, id, cas, remove, null, persistTo, replicateTo, delay, retryStrategy);
    }

    public static Observable<Boolean> call(final ClusterFacade core, final String bucket, final String id,
        final long cas, final boolean remove, MutationToken token, final PersistTo persistTo, final ReplicateTo replicateTo,
        final Delay delay, final RetryStrategy retryStrategy) {
        return call(core, bucket, id, cas, remove, token, persistTo, replicateTo, delay, retryStrategy, null);
    }

    public static Observable<Boolean> call(final ClusterFacade core, final String bucket, final String id,
        final long cas, final boolean remove, final PersistTo persistTo, final ReplicateTo replicateTo,
        final RetryStrategy retryStrategy, Span parent) {
        return call(core, bucket, id, cas, remove, persistTo, replicateTo, DEFAULT_DELAY, retryStrategy, parent);
    }

    public static Observable<Boolean> call(final ClusterFacade core, final String bucket, final String id,
        final long cas, final boolean remove, final PersistTo persistTo, final ReplicateTo replicateTo,
        final Delay delay, final RetryStrategy retryStrategy, Span parent) {
        return call(core, bucket, id, cas, remove, null, persistTo, replicateTo, delay, retryStrategy, parent);
    }

    public static Observable<Boolean> call(final ClusterFacade core, final String bucket, final String id,
       final long cas, final boolean remove, MutationToken token, final PersistTo persistTo, final ReplicateTo replicateTo,
       final Delay delay, final RetryStrategy retryStrategy, Span parent) {
        if (token == null) {
            return ObserveViaCAS.call(core, bucket, id, cas, remove, persistTo, replicateTo, delay, retryStrategy, parent);
        } else {
            return ObserveViaMutationToken.call(core, bucket, id, token, persistTo, replicateTo, delay, retryStrategy, parent, cas);
        }
    }


}
