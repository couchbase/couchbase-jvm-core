package com.couchbase.client.core.message.binary;

import com.couchbase.client.core.service.ServiceType;

public abstract class AbstractBinaryRequest implements BinaryRequest {

	private static final ServiceType TYPE = ServiceType.BINARY;
	private final String bucket;
	private final String password;

	protected AbstractBinaryRequest(final String bucket, final String password) {
		this.bucket = bucket;
		this.password = password;
	}

	@Override
	public ServiceType type() {
		return TYPE;
	}

	@Override
	public String bucket() {
		return bucket;
	}

	@Override
	public String password() {
		return password;
	}
}
