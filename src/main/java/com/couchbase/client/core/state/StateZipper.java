/**
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
