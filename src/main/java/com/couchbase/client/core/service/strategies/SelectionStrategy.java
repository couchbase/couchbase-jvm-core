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
package com.couchbase.client.core.service.strategies;

import com.couchbase.client.core.endpoint.Endpoint;
import com.couchbase.client.core.message.CouchbaseRequest;

import java.util.List;

/**
 * Interface which defines a generic selection strategy to select a {@link Endpoint}.
 *
 * @author Michael Nitschinger
 * @since 1.0
 */
public interface SelectionStrategy {

    /**
     * Selects an {@link Endpoint} for the given {@link CouchbaseRequest}.
     *
     * If null is returned, it means that no endpoint could be selected and it is up to the calling party
     * to decide what to do next.
     *
     * @param request the input request.
     * @param endpoints all the available endpoints.
     * @return the selected endpoint.
     */
    Endpoint select(CouchbaseRequest request, List<Endpoint> endpoints);

}
