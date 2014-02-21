package com.couchbase.client.core.service;

import com.couchbase.client.core.endpoint.binary.BinaryEndpoint;
import com.couchbase.client.core.environment.Environment;
import com.couchbase.client.core.message.CouchbaseRequest;
import com.couchbase.client.core.message.CouchbaseResponse;
import com.couchbase.client.core.state.LifecycleState;
import reactor.core.composable.Promise;

import java.net.InetSocketAddress;

public class BinaryService extends AbstractService {

	// FIXME: we need a registry here too, N endpoints and multiplexing
	private final BinaryEndpoint endpoint;

	public BinaryService(InetSocketAddress address, Environment env) {
		super(env);
		endpoint = new BinaryEndpoint(address, env);
	}

	@Override
	public Promise<Boolean> shutdown() {
		return null;
	}

	@Override
	public Promise<LifecycleState> connect() {
		return endpoint.connect();
	}

	@Override
	public  Promise<? extends CouchbaseResponse> send(final CouchbaseRequest request) {
		return endpoint.send(request);
	}
}
