/**
 * Copyright (C) 2014 Couchbase, Inc.
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

package com.couchbase.client.core.environment;

import io.netty.channel.EventLoopGroup;

/**
 * A {@link Environment} provides all the core building blocks like environment settings and thread pools so that the
 * application can work with it properly.
 *
 * This interface defines the contract. How does properties are loaded is chosen by the implementation. See the
 * {@link CouchbaseEnvironment} class for the default implementation.
 *
 * Note that the {@link Environment} is stateful, so be sure to call {@link #shutdown()} properly.
 */
public interface Environment {

	/**
	 * Shutdown the environment and the free the resources it holds.
	 */
	void shutdown();

	/**
	 * Returns the configured IO pool size.
	 *
	 * @return the pool size (number of threads to use).
	 */
	int ioPoolSize();

	/**
	 * The thread pool size for blocking tasks.
	 *
	 * @return the pool size (number of threads to use).
	 */
	int blockingPoolSize();

	/**
	 * The ring buffer size for the non blocking dispatcher.
	 *
	 * @return the number of slots in the ring buffer.
	 */
	int nonBlockingPoolBacklog();

	/**
	 * The warmup backlog size for the blocking thread pool.
	 *
	 * @return the backlog size.
	 */
	int blockingPoolBacklog();

	/**
	 * The underlying reactor environment.
	 *
	 * @return the reactor environment.
	 */
	reactor.core.Environment reactorEnv();

	/**
	 * Returns the configure IO pool.
	 *
	 * @return the configured IO pool.
	 */
	EventLoopGroup ioPool();
}
