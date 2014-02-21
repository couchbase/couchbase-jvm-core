package com.couchbase.client.core.message.binary;

import io.netty.buffer.ByteBuf;

public class UpsertRequest extends AbstractBinaryRequest {

	private final String key;
	private final ByteBuf content;

	public UpsertRequest(String key, ByteBuf content, String bucket, String password) {
		super(bucket, password);
		this.key = key;
		this.content = content;
	}

	public String key() {
		return key;
	}

	public ByteBuf content() {
		return content;
	}

}
