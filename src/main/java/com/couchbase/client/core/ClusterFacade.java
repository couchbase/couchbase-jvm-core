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
package com.couchbase.client.core;

import com.couchbase.client.core.annotations.InterfaceAudience;
import com.couchbase.client.core.annotations.InterfaceStability;
import com.couchbase.client.core.env.CoreEnvironment;
import com.couchbase.client.core.message.CouchbaseRequest;
import com.couchbase.client.core.message.CouchbaseResponse;
import rx.Observable;

/**
 * Represents a Couchbase Cluster.
 *
 * @author Michael Nitschinger
 * @since 1.0
 */
public interface ClusterFacade {

    /**
     * Sends a {@link CouchbaseRequest} into the cluster and eventually returns a {@link CouchbaseResponse}.
     *
     * The {@link CouchbaseResponse} is not returned directly, but is wrapped into a {@link Observable}.
     *
     * @param request the request to send.
     * @param <R> the generic response type.
     * @return the {@link CouchbaseResponse} wrapped into a {@link Observable}.
     */
    @InterfaceStability.Committed
    @InterfaceAudience.Public
    <R extends CouchbaseResponse> Observable<R> send(CouchbaseRequest request);

    /**
     * The core id is unique per core instance.
     *
     * @return returns the ID for this core.
     * @deprecated Use {@link #ctx()} which also contains the ID.
     */
    @Deprecated
    long id();

    /**
     * Exposes the currently used environment.
     *
     * @return the environment used.
     */
    CoreContext ctx();
}
