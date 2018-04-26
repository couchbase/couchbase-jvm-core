/*
 * Copyright (c) 2018 Couchbase, Inc.
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

/**
 * Represents mutation metadata which can be inspected after
 * a mutation and during/after is respective observe poll
 * cycles.
 */
public interface OriginalMutationResult {

    /**
     * If the originating operation used to be a mutation and was
     * successful, this getter allows to retrieve the cas value
     * returned.
     *
     * @throws IllegalStateException if the originating operation
     *         was not a mutation operation in the first place.
     * @return a long case value or an exception otherwise.
     */
    long mutationCas();

}
