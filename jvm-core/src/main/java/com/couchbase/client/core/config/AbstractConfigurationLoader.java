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
import com.couchbase.client.core.message.internal.AddNodeRequest;
import com.couchbase.client.core.message.internal.AddNodeResponse;
import com.couchbase.client.core.message.internal.EnableServiceRequest;
import com.couchbase.client.core.message.internal.EnableServiceResponse;
import com.couchbase.client.core.service.ServiceType;
import reactor.core.composable.Composable;
import reactor.core.composable.Promise;
import reactor.core.composable.spec.Promises;
import reactor.function.Consumer;
import reactor.function.Function;

import java.net.InetSocketAddress;

public abstract class AbstractConfigurationLoader {

	private final InetSocketAddress node;
	private final Environment env;
	private final String bucket;
	private final String password;
	private final ServiceType serviceType;
	private final Cluster cluster;

	public AbstractConfigurationLoader(Environment env, ServiceType serviceType, InetSocketAddress node, String bucket,
		String password, Cluster cluster) {
		this.serviceType = serviceType;
		this.env = env;
		this.node = node;
		this.bucket = bucket;
		this.password = password;
		this.cluster = cluster;
	}

	abstract Promise<String> loadRawConfig();

	public Promise<String> load() {
		return addNode().mapMany(new Function<Boolean, Composable<Boolean>>() {
			@Override
			public Composable<Boolean> apply(Boolean success) {
				return addService();
			}
		}).mapMany(new Function<Boolean, Composable<String>>() {
			@Override
			public Composable<String> apply(Boolean success) {
				return loadRawConfig();
			}
		});
	}

	Promise<Boolean> addNode() {
		return cluster.<AddNodeResponse>send(new AddNodeRequest(node)).map(new Function<AddNodeResponse, Boolean>() {
			@Override
			public Boolean apply(AddNodeResponse response) {
				return response.getStatus() == AddNodeResponse.Status.ADDED;
			}
		});
	}

	private Promise<Boolean> addService() {
		return cluster.<EnableServiceResponse>send(new EnableServiceRequest(node, serviceType, bucket))
			.map(new Function<EnableServiceResponse, Boolean>() {
				@Override
				public Boolean apply(EnableServiceResponse response) {
					return response.getStatus() == EnableServiceResponse.Status.ENABLED;
				}
			});
	}

	protected InetSocketAddress node() {
		return node;
	}

	protected String bucket() {
		return bucket;
	}

	protected String password() {
		return password;
	}

	protected Cluster cluster() {
		return cluster;
	}
}
