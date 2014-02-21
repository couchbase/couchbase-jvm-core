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

package com.couchbase.client.core.node;

import com.couchbase.client.core.environment.Environment;
import com.couchbase.client.core.message.CouchbaseRequest;
import com.couchbase.client.core.message.CouchbaseResponse;
import com.couchbase.client.core.message.binary.BinaryRequest;
import com.couchbase.client.core.message.binary.BinaryResponse;
import com.couchbase.client.core.message.config.ConfigRequest;
import com.couchbase.client.core.message.config.ConfigResponse;
import com.couchbase.client.core.message.internal.*;
import com.couchbase.client.core.service.ConfigService;
import com.couchbase.client.core.service.Service;
import com.couchbase.client.core.service.ServiceSpec;
import com.couchbase.client.core.service.ServiceType;
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
import java.util.List;

import static reactor.event.selector.Selectors.U;

/**
 *
 */
public class CouchbaseNode extends AbstractStateMachine<LifecycleState> implements Node {


    /**
     *
     */
    private final InetSocketAddress address;

    /**
     *
     */
    private final Environment env;

    /**
     *
     */
    private final Registry<Service> serviceRegistry;

    /**
     *
     * @param env
     * @param address
     */
    public CouchbaseNode(final Environment env, final InetSocketAddress address) {
        super(LifecycleState.DISCONNECTED, env);
        this.env = env;
        this.address = address;
        serviceRegistry = new CachingRegistry<Service>();
    }

	@Override
	public Promise<EnableServiceResponse> enableService(final EnableServiceRequest request) {
        if (!hasServiceRegistered(request.type(), request.bucket())) {
			final Service service = new ServiceSpec(request.type(), request.address(), env).get();
            return service.connect().map(new Function<LifecycleState, EnableServiceResponse>() {
                @Override
                public EnableServiceResponse apply(LifecycleState lifecycleState) {
                    serviceRegistry.register(U(selector(request.type(), request.bucket())), service);
                    return EnableServiceResponse.serviceEnabled();
                }
            });
        } else {
		    return Promises.success(EnableServiceResponse.serviceEnabled()).get();
        }
	}

	@Override
	public Promise<DisableServiceResponse> disableService(final DisableServiceRequest request) {
        final Deferred<DisableServiceResponse, Promise<DisableServiceResponse>> deferredResponse =
             Promises.defer(env.reactorEnv());

        if (hasServiceRegistered(request.type(), request.bucket())) {
            Service service = serviceRegistry.select(selector(request.type(), request.bucket())).get(0).getObject();
            serviceRegistry.unregister(selector(request.type(), request.bucket()));
            service.shutdown().onComplete(new Consumer<Promise<Boolean>>() {
                @Override
                public void accept(Promise<Boolean> shutdownPromise) {
                    if (shutdownPromise.isSuccess()) {
                        deferredResponse.accept(DisableServiceResponse.serviceDisabled());
                    } else {
                        deferredResponse.accept(shutdownPromise.reason());
                    }
                }
            });
        } else {
            deferredResponse.accept(DisableServiceResponse.serviceDisabled());
        }

        return deferredResponse.compose();
	}

    @Override
    public Promise<? extends CouchbaseResponse> send(final CouchbaseRequest request) {
        if (request instanceof ConfigRequest) {
            return handleConfigRequest((ConfigRequest) request);
		} else if (request instanceof BinaryRequest) {
			return handleBinaryRequest((BinaryRequest) request);
        } else {
            throw new IllegalStateException("Node doesn't understand the given request message: " + request);
        }
    }

    private Promise<ConfigResponse> handleConfigRequest(final ConfigRequest request) {
        if (hasServiceRegistered(request.type(), request.bucket())) {
            Service service = serviceRegistry.select(selector(request.type(), request.bucket())).get(0).getObject();
            return service.send(request);
        } else {
            // FAIL! service not registered
        }
        return null;
    }

	private Promise<BinaryResponse> handleBinaryRequest(final BinaryRequest request) {
		if (hasServiceRegistered(request.type(), request.bucket())) {
			Service service = serviceRegistry.select(selector(request.type(), request.bucket())).get(0).getObject();
			return service.send(request);
		} else {
			// FAIL! service not registered
		}
		return null;
	}

    @Override
    public Promise<Boolean> shutdown() {
        return null;
    }

    /**
     *
     * @param type
     * @return
     */
    private boolean hasServiceRegistered(final ServiceType type, final String bucket) {
        List<Registration<? extends Service>> registrations = serviceRegistry.select(selector(type, bucket));
        for (Registration<? extends Service> registration : registrations) {
            if (!registration.isCancelled()) {
                return true;
            }
        }
        return false;
    }

    /**
     *
     * @param type
     * @param bucket
     * @return
     */
    private String selector(final ServiceType type, final String bucket) {
        switch(type.mapping()) {
            case ONE_BY_ONE:
                return "/" + type + "/" + bucket;
            case ONE_FOR_ALL:
                return "/" + type;
            default:
                throw new IllegalStateException("I dont speak service type: " + type);
        }
    }

}
