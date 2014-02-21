package com.couchbase.client.core.message.binary;

public class GetRequest extends AbstractBinaryRequest {

	private final String key;

	public GetRequest(String key, String bucket, String password) {
		super(bucket, password);
		this.key = key;
	}

	public String key() {
		return key;
	}
}
