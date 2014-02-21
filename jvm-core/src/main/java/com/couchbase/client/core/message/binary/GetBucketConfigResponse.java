package com.couchbase.client.core.message.binary;

/**
 * Created by michael on 21/02/14.
 */
public class GetBucketConfigResponse implements BinaryResponse {

	private final String content;

	public GetBucketConfigResponse(final String content) {
		this.content = content;
	}

	public String content() {
		return content;
	}

}
