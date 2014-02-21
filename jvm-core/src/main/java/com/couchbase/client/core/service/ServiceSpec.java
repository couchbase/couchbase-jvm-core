package com.couchbase.client.core.service;

import com.couchbase.client.core.environment.Environment;
import reactor.function.Supplier;

import java.net.InetSocketAddress;

public class ServiceSpec implements Supplier<Service> {

	private final ServiceType type;
	private final InetSocketAddress address;
	private final Environment env;

	public ServiceSpec(ServiceType type, InetSocketAddress address, Environment env) {
		this.type = type;
		this.address = address;
		this.env = env;
	}

	@Override
	public Service get() {
		switch(type) {
			case CONFIG:
				return new ConfigService(address, env);
			case BINARY:
				return new BinaryService(address, env);
			default:
				throw new IllegalArgumentException("I dont understand the given service");
		}
	}
}
