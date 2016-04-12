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
package com.couchbase.client.core.state;

/**
 * Describes a generic {@link StateZipper}.
 *
 * See the actual implementation for the {@link AbstractStateZipper} for more details and usage information.
 *
 * @author Michael Nitschinger
 * @since 1.1.0
 */
public interface StateZipper<T, S extends Enum> extends Stateful<S> {

    /**
     * Register the given stream to be zipped into the state computation.
     *
     * @param identifier the identifier used to uniquely identify the stream.
     * @param stateful the stateful compontent to be registered.
     */
    void register(T identifier, Stateful<S> stateful);

    /**
     * Deregisters a stream identified by the identifier from the state computation.
     *
     * @param identifier the identifier used to uniquely identify the stream.
     */
    void deregister(T identifier);

    /**
     * Terminate the zipper and deregister all registered streams.
     */
    void terminate();
}
