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

package com.couchbase.client.core.cluster;

import com.couchbase.client.core.config.Configuration;
import com.couchbase.client.core.config.ConfigurationManager;
import com.couchbase.client.core.config.DefaultConfigurationManager;
import com.couchbase.client.core.environment.CouchbaseEnvironment;
import com.couchbase.client.core.environment.Environment;
import com.couchbase.client.core.message.CouchbaseRequest;
import com.couchbase.client.core.message.CouchbaseResponse;
import com.couchbase.client.core.message.common.CommonRequest;
import com.couchbase.client.core.message.common.CommonResponse;
import com.couchbase.client.core.message.common.ConnectRequest;
import com.couchbase.client.core.message.common.ConnectResponse;
import com.couchbase.client.core.message.config.ConfigRequest;
import com.couchbase.client.core.message.config.ConfigResponse;
import com.couchbase.client.core.message.config.GetBucketConfigRequest;
import com.couchbase.client.core.message.config.GetBucketConfigResponse;
import com.couchbase.client.core.message.internal.AddNodeRequest;
import com.couchbase.client.core.message.internal.AddNodeResponse;
import com.couchbase.client.core.message.internal.DisableServiceRequest;
import com.couchbase.client.core.message.internal.DisableServiceResponse;
import com.couchbase.client.core.message.internal.EnableServiceRequest;
import com.couchbase.client.core.message.internal.EnableServiceResponse;
import com.couchbase.client.core.message.internal.InternalRequest;
import com.couchbase.client.core.message.internal.InternalResponse;
import com.couchbase.client.core.message.internal.RemoveNodeRequest;
import com.couchbase.client.core.message.internal.RemoveNodeResponse;
import com.couchbase.client.core.node.CouchbaseNode;
import com.couchbase.client.core.node.Node;
import com.couchbase.client.core.state.AbstractStateMachine;
import com.couchbase.client.core.state.LifecycleState;
import reactor.core.composable.Deferred;
import reactor.core.composable.Promise;
import reactor.core.composable.spec.Promises;
import reactor.event.registry.CachingRegistry;
import reactor.event.registry.Registration;
import reactor.event.registry.Registry;
import reactor.function.Consumer;
import reactor.function.Function;

import java.net.InetSocketAddress;
import java.util.Iterator;
import java.util.List;

import static reactor.event.selector.Selectors.$;

/**
 * The default implementation of a {@link Cluster}.
 *
 * TODO:
 *  - attach streams to all connected nodes, react on changes and build the states for the cluster.
 *
 *  - implement mock test of config loading from connecting
 *
 *  - Implement basic bootstrapping in config manager (+ cccp)
 *
 *  - implement node layer
 *
 *  - implement service layer
 *
 *  - implement endpoint layer + netty transition
 *
 */
public class CouchbaseCluster extends AbstractStateMachine<LifecycleState> implements Cluster {

    private final Environment env;
    private final ConfigurationManager configurationManager;
    private final Registry<Node> nodeRegistry;

    public CouchbaseCluster() {
        this(new CouchbaseEnvironment());
    }

	public CouchbaseCluster(final Environment env) {
		super(LifecycleState.DISCONNECTED, env);
        this.env = env;
        configurationManager = new DefaultConfigurationManager(env, this);
        nodeRegistry = new CachingRegistry<Node>();
	}

	CouchbaseCluster(final Environment env, final ConfigurationManager manager, final Registry<Node> registry) {
		super(LifecycleState.DISCONNECTED, env);
		this.env = env;
		configurationManager = manager;
		nodeRegistry = registry;
	}

	@Override
	public Promise<? extends CouchbaseResponse> send(final CouchbaseRequest request) {
        if (request instanceof CommonRequest) {
            return dispatchCommon((CommonRequest) request);
        } else if (request instanceof InternalRequest) {
            return dispatchInternal((InternalRequest) request);
		} else if (request instanceof ConfigRequest) {
			return dispatchConfig((ConfigRequest) request);
        } else {
			throw new UnsupportedOperationException("Unsupported CouchbaseRequest type: " + request);
		}
	}

    /**
     * Helper method to dispatch common messages.
     *
     * @param request
     * @return
     */
    private Promise<? extends CommonResponse> dispatchCommon(final CommonRequest request) {
        if (request instanceof ConnectRequest) {
            return handleConnect((ConnectRequest) request);
        } else {
            throw new UnsupportedOperationException("Unsupported CouchbaseRequest type: " + request);
        }
    }

    /**
     * Helper method to dispatch internal messages.
     *
     * @param request
     * @return
     */
    private Promise<? extends InternalResponse> dispatchInternal(final InternalRequest request) {
        if (request instanceof AddNodeRequest) {
            return handleAddNode((AddNodeRequest) request);
        } else if (request instanceof EnableServiceRequest) {
            return handleEnableService((EnableServiceRequest) request);
        } else if (request instanceof RemoveNodeRequest) {
            return handleRemoveNode((RemoveNodeRequest) request);
        } else if (request instanceof DisableServiceRequest) {
            return handleDisableService((DisableServiceRequest) request);
        } else {
            throw new UnsupportedOperationException("Unsupported CouchbaseRequest type: " + request);
        }
    }

	/**
	 * Helper method to dispatch config service messages.
	 *
	 * @param request
	 * @return
	 */
	private Promise<ConfigResponse> dispatchConfig(final ConfigRequest request) {
		if (request instanceof GetBucketConfigRequest) {
			GetBucketConfigRequest req = (GetBucketConfigRequest) request;
            Registration<? extends Node> registration = nodeRegistry.select(req.node()).get(0);
			return registration.getObject().send(request);
		}
		// TODO: fixme when not found
		return null;
	}

    /**
     * Handle a {@link ConnectRequest}.
     *
     * @param request
     * @return
     */
	private Promise<ConnectResponse> handleConnect(final ConnectRequest request) {
        Promise<Configuration> connectPromise = configurationManager.connect(
            request.getSeedNodes(), request.getBucketName(), request.getBucketPassword()
        );

        final Deferred<ConnectResponse,Promise<ConnectResponse>> deferred = Promises.defer(
            env.reactorEnv(), reactor.core.Environment.THREAD_POOL
        );

        connectPromise.onComplete(new Consumer<Promise<Configuration>>() {
            @Override
            public void accept(final Promise<Configuration> configPromise) {
                if (configPromise.isSuccess()) {
                    deferred.accept(ConnectResponse.connected());
                } else {
                    deferred.accept(configPromise.reason());
                }
            }
        });

        return deferred.compose();
	}

    /**
     * Add a node if it isn't added already.
     *
     * @param request
     * @return
     */
    private Promise<AddNodeResponse> handleAddNode(final AddNodeRequest request) {
        InetSocketAddress address = request.getAddress();

		if (!hasNodeRegistered(address)) {
			Node node = new CouchbaseNode(env, address);
			nodeRegistry.register($(address), node);
		}

        return Promises.success(AddNodeResponse.nodeAdded()).get();
    }

    /**
     * Remove a node if one has been added with that selector.
     *
     * @param request
     * @return
     */
    private Promise<RemoveNodeResponse> handleRemoveNode(final RemoveNodeRequest request) {
        final Deferred<RemoveNodeResponse, Promise<RemoveNodeResponse>> deferredResponse =
            Promises.defer(env.reactorEnv());

		InetSocketAddress address = request.getAddress();
		if (hasNodeRegistered(address)) {
			Node node = nodeRegistry.select(address).get(0).getObject();
            nodeRegistry.unregister(request.getAddress());
			node.shutdown().onComplete(new Consumer<Promise<Boolean>>() {
				@Override
				public void accept(Promise<Boolean> shutdownPromise) {
					if (shutdownPromise.isSuccess()) {
						deferredResponse.accept(RemoveNodeResponse.nodeRemoved());
					} else {
						deferredResponse.accept(shutdownPromise.reason());
					}
				}
			});
		} else {
			deferredResponse.accept(RemoveNodeResponse.nodeRemoved());
		}

		return deferredResponse.compose();
    }

    /**
     * Enable the service for the given node (if registered).
	 *
     * @param request
     * @return
     */
    private Promise<EnableServiceResponse> handleEnableService(final EnableServiceRequest request) {
		InetSocketAddress address = request.address();

		if (hasNodeRegistered(address)) {
			Node node = nodeRegistry.select(address).get(0).getObject();
			return node.enableService(request);
		} else {
			return Promises.success(EnableServiceResponse.noNodeFound()).get();
		}
    }

    /**
     * Disable the service for the given node (if registered).
	 *
     * @param request
     * @return
     */
    private Promise<DisableServiceResponse> handleDisableService(final DisableServiceRequest request) {
		InetSocketAddress address = request.address();

		if (hasNodeRegistered(address)) {
			Node node = nodeRegistry.select(address).get(0).getObject();
			return node.disableService(request);
		} else {
			return Promises.success(DisableServiceResponse.noNodeFound()).get();
		}
    }

	/**
	 * Helper method to see if the given node is registered already.
	 *
	 * @param address the address to validate against.
	 * @return true if it is registered, false otherwise.
	 */
	private boolean hasNodeRegistered(InetSocketAddress address) {
		List<Registration<? extends Node>> registrations = nodeRegistry.select(address);
		for (Registration<? extends Node> registration : registrations) {
			if (!registration.isCancelled()) {
				return true;
			}
		}
		return false;
	}

}
