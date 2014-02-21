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

package com.couchbase.client.core.config;

import com.couchbase.client.core.cluster.Cluster;
import com.couchbase.client.core.environment.Environment;
import com.couchbase.client.core.message.CouchbaseResponse;
import com.couchbase.client.core.message.internal.AddNodeRequest;
import com.couchbase.client.core.message.internal.AddNodeResponse;
import com.couchbase.client.core.message.internal.EnableServiceRequest;
import com.couchbase.client.core.message.internal.EnableServiceResponse;
import com.couchbase.client.core.state.LifecycleState;
import reactor.core.composable.Deferred;
import reactor.core.composable.Promise;
import reactor.core.composable.spec.Promises;
import reactor.function.Consumer;
import reactor.function.Function;
import reactor.function.Predicate;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class DefaultConfigurationManager implements ConfigurationManager {

	/**
	 * The global environment and configuration.
	 */
    private final Environment env;

	/**
	 * A reference to the cluster for all underlying service operations.
	 */
    private final Cluster cluster;

	/**
	 * Holds all current stored configurations, one per bucket.
	 */
	private final Map<String, Configuration> currentConfigs;

	/**
	 * The configuration parser.
	 */
	private final ConfigurationParser configurationParser;

	/**
	 * Create a new DefaultConfigurationManager}.
	 *
	 * @param env the environment.
	 * @param cluster the cluster reference.
	 */
    public DefaultConfigurationManager(Environment env, Cluster cluster) {
        this.env = env;
        this.cluster = cluster;

		configurationParser = new JSONConfigurationParser();
		currentConfigs = new ConcurrentHashMap<String, Configuration>();
    }

    @Override
    public Promise<Configuration> connect(List<InetSocketAddress> seedNodes, String bucket, String password) {
		if (currentConfigs.containsKey(bucket)) {
			return Promises.success(currentConfigs.get(bucket)).get();
		}

		return loadConfig(seedNodes, bucket, password);
    }

	/**
	 * Try to load a config from all the given seed nodes.
	 *
	 * @param seedNodes
	 * @param bucket
	 * @param password
	 * @return
	 */
	private Promise<Configuration> loadConfig(List<InetSocketAddress> seedNodes, final String bucket, String password) {
		List<Promise<Configuration>> configPromises = new ArrayList<Promise<Configuration>>(seedNodes.size());
		for (InetSocketAddress node : seedNodes) {
			configPromises.add(loadConfigForNode(node, bucket, password));
		}

		return Promises.when(configPromises).filter(new Predicate<List<Configuration>>() {
			@Override
			public boolean test(List<Configuration> configurations) {
				return configurations.size() > 0;
			}
		}).map(new Function<List<Configuration>, Configuration>() {
			@Override
			public Configuration apply(List<Configuration> configurations) {
				return configurations.get(0);
			}
		}).consume(new Consumer<Configuration>() {
			@Override
			public void accept(final Configuration configuration) {
				currentConfigs.put(bucket, configuration);
			}
		});
	}

	/**
	 * Load a config from a specific node in the cluster.
	 *
	 * @param seedNode
	 * @param bucket
	 * @param password
	 * @return
	 */
	private Promise<Configuration> loadConfigForNode(final InetSocketAddress seedNode, final String bucket,
		final String password) {
		final Deferred<Configuration,Promise<Configuration>> configPromise =
				Promises.defer(env.reactorEnv());

		Promise<Configuration> binaryConfig = loadConfigOverBinary(seedNode, bucket, password);
		binaryConfig
			.consume(configPromise)
			.when(Exception.class, new Consumer<Exception>() {
				@Override
				public void accept(Exception e) {
					Promise<Configuration> httpConfig = loadConfigOverHttp(seedNode, bucket, password);
					httpConfig
						.consume(configPromise)
						.when(Exception.class, new Consumer<Exception>() {
							@Override
							public void accept(Exception e) {
								configPromise.accept(e);
							}
						});
				}
			});

		return configPromise.compose();
	}

	/**
	 * Try to load the config over HTTP from the given node.
	 *
	 * @param seedNode
	 * @param bucket
	 * @param password
	 * @return
	 */
	private Promise<Configuration> loadConfigOverHttp(final InetSocketAddress seedNode, final String bucket,
		final String password) {
		HttpConfigurationLoader configurationLoader = new HttpConfigurationLoader(env, seedNode, bucket, password,
			cluster);

		return configurationLoader.loadRawConfig().map(new Function<String, Configuration>() {
			@Override
			public Configuration apply(String rawConfig) {
				return configurationParser.parse(rawConfig);
			}
		});
	}

	/**
	 * Try to load the config over BINARY from the given node.
	 *
	 * @param seedNode
	 * @param bucket
	 * @param password
	 * @return
	 */
	private Promise<Configuration> loadConfigOverBinary(final InetSocketAddress seedNode, final String bucket,
		final String password) {
		BinaryConfigurationLoader configurationLoader = new BinaryConfigurationLoader(env, seedNode, bucket, password,
			cluster);

		return configurationLoader.load().map(new Function<String, Configuration>() {
			@Override
			public Configuration apply(String rawConfig) {
				return configurationParser.parse(rawConfig);
			}
		});
	}

}
