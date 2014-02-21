package com.couchbase.client.core.message.binary;

import java.net.InetSocketAddress;

public class GetBucketConfigRequest extends AbstractBinaryRequest {

	private final InetSocketAddress node;

	public GetBucketConfigRequest(final InetSocketAddress node, final String bucket, final String password) {
		super(bucket, password);
		this.node = node;
	}

	public InetSocketAddress node() {
		return node;
	}
}
