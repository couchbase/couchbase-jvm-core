package com.couchbase.client.core.service;

import com.couchbase.client.core.endpoint.Endpoint;
import com.couchbase.client.core.endpoint.binary.BinaryEndpoint;
import com.couchbase.client.core.environment.Environment;
import com.couchbase.client.core.message.CouchbaseRequest;
import com.couchbase.client.core.message.CouchbaseResponse;
import com.couchbase.client.core.service.strategies.KeyHashSelectionStrategy;
import com.couchbase.client.core.state.LifecycleState;
import reactor.core.composable.Promise;

import java.net.InetSocketAddress;

public class BinaryService extends AbstractService {

    private static final SelectionStrategy strategy = new KeyHashSelectionStrategy();

	public BinaryService(InetSocketAddress address, Environment env) {
		super(address, env, 3, strategy);
	}

    @Override
    protected Endpoint newEndpoint() {
        return new BinaryEndpoint(address(), environment());
    }

}
