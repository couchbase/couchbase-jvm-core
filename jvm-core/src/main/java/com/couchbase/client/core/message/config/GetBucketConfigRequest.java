package com.couchbase.client.core.message.config;

import com.couchbase.client.core.service.ServiceType;

import java.net.InetSocketAddress;

public class GetBucketConfigRequest implements ConfigRequest {

    private static final ServiceType type = ServiceType.CONFIG;

	private final InetSocketAddress node;
	private final String bucket;
	private final String password;

	public GetBucketConfigRequest(final InetSocketAddress node, final String bucket, final String password) {
		this.node = node;
		this.bucket = bucket;
		this.password = password;
	}


	public InetSocketAddress node() {
		return node;
	}

	public String bucket() {
		return bucket;
	}

	public String password() {
		return password;
	}

    @Override
    public ServiceType type() {
        return type;
    }

}
