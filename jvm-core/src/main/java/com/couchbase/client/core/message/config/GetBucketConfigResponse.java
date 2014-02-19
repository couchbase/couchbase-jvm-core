package com.couchbase.client.core.message.config;

public class GetBucketConfigResponse implements ConfigResponse {

	private final String content;

	public GetBucketConfigResponse(final String content) {
		this.content = content;
	}

	public String content() {
		return content;
	}
}
