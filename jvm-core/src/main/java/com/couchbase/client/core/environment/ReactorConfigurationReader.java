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

import reactor.core.Environment;
import reactor.core.configuration.ConfigurationReader;
import reactor.core.configuration.DispatcherConfiguration;
import reactor.core.configuration.DispatcherType;
import reactor.core.configuration.ReactorConfiguration;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class ReactorConfigurationReader implements ConfigurationReader {

	private final List<DispatcherConfiguration> dispatcherConfigurations;
	private final String defaultDispatcher;
	private final Properties properties;

	public ReactorConfigurationReader(int blockingPoolSize, int blockingPoolBacklog, int nonBlockingPoolBacklog) {
		dispatcherConfigurations = new ArrayList<DispatcherConfiguration>();

		dispatcherConfigurations.add(new DispatcherConfiguration(
				Environment.RING_BUFFER, DispatcherType.RING_BUFFER, nonBlockingPoolBacklog, 0
		));

		dispatcherConfigurations.add(new DispatcherConfiguration(
				Environment.THREAD_POOL, DispatcherType.THREAD_POOL_EXECUTOR, blockingPoolBacklog,
				blockingPoolSize
		));

		defaultDispatcher = Environment.RING_BUFFER;
		properties = new Properties();
	}

	@Override
	public ReactorConfiguration read() {
		return new ReactorConfiguration(dispatcherConfigurations, defaultDispatcher, properties);
	}
}
