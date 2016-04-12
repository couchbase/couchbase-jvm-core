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
package com.couchbase.client.core.retry;

import com.couchbase.client.core.env.CoreEnvironment;
import com.couchbase.client.core.message.CouchbaseRequest;
import com.couchbase.client.core.message.kv.ObserveRequest;

/**
 * Base interface for all {@link RetryStrategy} implementations.
 *
 * @author Michael Nitschinger
 * @since 1.1.0
 */
public interface RetryStrategy {

    /**
     * Decides whether the given {@link CouchbaseRequest} should be retried or cancelled.
     *
     * @param request the request in question.
     * @param environment the environment for more context.
     * @return true if it should be retried, false otherwise.
     */
    boolean shouldRetry(CouchbaseRequest request, CoreEnvironment environment);

    /**
     * Decides whether {@link ObserveRequest}s should be retried or cancelled when an error happens.
     *
     * When false is returned, as soon as an error happens (for example one of the nodes that need to be reached
     * does not have an active partition because of a node failure) the whole observe sequence is aborted. If
     * retried, errors are swallowed and the observe cycle will start again.
     *
     * @return true if it should be retried, false otherwise.
     */
    boolean shouldRetryObserve();

}
