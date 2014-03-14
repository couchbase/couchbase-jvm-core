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

import com.lmax.disruptor.BlockingWaitStrategy;
import com.lmax.disruptor.dsl.ProducerType;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.util.concurrent.DefaultThreadFactory;
import reactor.core.configuration.ConfigurationReader;
import reactor.core.configuration.DispatcherConfiguration;
import reactor.core.configuration.ReactorConfiguration;
import reactor.event.dispatch.Dispatcher;
import reactor.event.dispatch.RingBufferDispatcher;
import reactor.event.dispatch.ThreadPoolExecutorDispatcher;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * The {@link CouchbaseEnvironment} wraps the underlying environment retreival mechanisms and provides convenient
 * access methods to commonly used properties.
 *
 * It is intended to be shared throughout the application and should only be instantiated once.
 */
public class CouchbaseEnvironment implements Environment {

	/**
	 * The default namespace to use for settings if not instructed otherwise.
	 */
	private static final String DEFAULT_NAMESPACE = "com.couchbase.client";

	/**
	 * Namespace for the blocking pool size.
	 */
	private static final String BLOCKING_POOL_SIZE_PATH = "core.blockingPoolSize";

	/**
	 * Namespace for the IO pool size.
	 */
	private static final String IO_POOL_SIZE_PATH = "core.io.poolSize";

	/**
	 * Namespace for the blocking pool backlog.
	 */
	private static final String BLOCKING_POOL_BACKLOG_PATH = "core.blockingPoolBacklog";

	/**
	 * Namespace for the non blocking pool backlog.
	 */
	private static final String NON_BLOCKING_POOL_BACKLOG_PATH = "core.nonBlockingPoolBacklog";

    /**
     * Namespace for the blocking pool prefix.
     */
    private static final String BLOCKING_POOL_PREFIX = "core.blockingPoolPrefix";

    /**
     * Namespace for the non blocking pool prefix.
     */
    private static final String NON_BLOCKING_POOL_PREFIX = "core.nonBlockingPoolPrefix";

    /**
     * Namespace for the io pool prefix.
     */
    private static final String IO_POOL_PREFIX = "core.io.poolPrefix";

    /**
     * Namespace for the binary flush interval.
     */
    private static final String IO_BINARY_FLUSH_INTERVAL = "core.io.binary.flushInterval";

    /**
     * Namespace for the config flush interval.
     */
    private static final String IO_CONFIG_FLUSH_INTERVAL = "core.io.config.flushInterval";

	/**
	 * The global namespace used.
	 */
	private final String namespace;

	/**
	 * References the underlying loaded configuration.
	 */
	private final Config config;

	/**
	 * The reactor environment which is wrapped.
	 */
	private final reactor.core.Environment reactorEnv;

	/**
	 * The netty event loop group to use.
	 */
	private final EventLoopGroup eventLoopGroup;

	/**
	 * Create a new configuration with the default namespace.
	 */
	public CouchbaseEnvironment() {
		this(ConfigFactory.load(), DEFAULT_NAMESPACE);
	}

	/**
	 * Create a new configuration with a custom config.
	 *
	 * @param config the custom configuration to use.
	 */
	public CouchbaseEnvironment(Config config) {
		this(config, DEFAULT_NAMESPACE);
	}

	/**
	 * Create a new configuration with a custom namespace.
	 *
	 * @param config the custom configuration to use.
	 * @param namespace the global namespace to use.
	 */
	public CouchbaseEnvironment(Config config, String namespace) {
		this.namespace = namespace;
		this.config = config;

        reactorEnv = new reactor.core.Environment(Collections.<String, List<Dispatcher>>emptyMap(),
            new ConfigurationReader() {
                @Override
                public ReactorConfiguration read() {
                    return new ReactorConfiguration(
                        new ArrayList<DispatcherConfiguration>(),
                        reactor.core.Environment.RING_BUFFER,
                        new Properties()
                    );
                }
            }
        );

        reactorEnv.addDispatcher(reactor.core.Environment.RING_BUFFER, new RingBufferDispatcher(
            nonBlockingPoolPrefix(),
            nonBlockingPoolBacklog(),
            null,
            ProducerType.MULTI,
            new BlockingWaitStrategy()
        ));

        reactorEnv.addDispatcher(reactor.core.Environment.THREAD_POOL, new ThreadPoolExecutorDispatcher(
            blockingPoolSize(),
            blockingPoolBacklog(),
            blockingPoolPrefix(),
            new LinkedBlockingQueue<Runnable>(blockingPoolBacklog()),
            new RejectedExecutionHandler() {
                @Override
                public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
                    r.run();
                }
            }
        ));

		eventLoopGroup = new NioEventLoopGroup(ioPoolSize(), new DefaultThreadFactory(ioPoolPrefix()));
	}

	@Override
	public void shutdown() {
		reactorEnv.shutdown();
		eventLoopGroup.shutdownGracefully();
	}

	@Override
	public int ioPoolSize() {
		int ioPoolSize = getInt(IO_POOL_SIZE_PATH);
		if (ioPoolSize <= 0) {
			return Runtime.getRuntime().availableProcessors();
		}
		return ioPoolSize;
	}

	@Override
	public int blockingPoolSize() {
		int blockingPoolSize = getInt(BLOCKING_POOL_SIZE_PATH);
		if (blockingPoolSize <= 0) {
			return Runtime.getRuntime().availableProcessors();
		}
		return blockingPoolSize;
	}

	@Override
	public int nonBlockingPoolBacklog() {
		return getInt(NON_BLOCKING_POOL_BACKLOG_PATH);
	}

	@Override
	public int blockingPoolBacklog() {
		return getInt(BLOCKING_POOL_BACKLOG_PATH);
	}

	@Override
	public EventLoopGroup ioPool() {
		return eventLoopGroup;
	}

	@Override
	public reactor.core.Environment reactorEnv() {
		return reactorEnv;
	}

    @Override
    public String blockingPoolPrefix() {
        return getString(BLOCKING_POOL_PREFIX);
    }

    @Override
    public String nonBlockingPoolPrefix() {
        return getString(NON_BLOCKING_POOL_PREFIX);
    }

    @Override
    public String ioPoolPrefix() {
        return getString(IO_POOL_PREFIX);
    }

    @Override
    public long ioBinaryFlushInterval() {
        return getLong(IO_BINARY_FLUSH_INTERVAL);
    }

    @Override
    public long ioConfigFlushInterval() {
        return getLong(IO_CONFIG_FLUSH_INTERVAL);
    }

	/**
	 * Return the configuration value as a int, identified by the namespace.
	 *
	 * @param path the relative path to the value.
	 * @return the string value.
	 */
	protected int getInt(String path) {
		try {
			return config.getInt(namespace + '.' + path);
		} catch (Exception e) {
			throw new EnvironmentException("Could not load environment setting " + path + '.', e);
		}
	}

    protected long getLong(String path) {
        try {
            return config.getLong(namespace + '.' + path);
        } catch (Exception e) {
            throw new EnvironmentException("Could not load environment setting " + path + '.', e);
        }
    }

    protected String getString(String path) {
        try {
            return config.getString(namespace + '.' + path);
        } catch (Exception e) {
            throw new EnvironmentException("Could not load environment setting " + path + '.', e);
        }
    }

}
