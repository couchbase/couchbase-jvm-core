package com.couchbase.client.core.message.binary;

import com.couchbase.client.core.message.CouchbaseRequest;
import com.couchbase.client.core.service.ServiceType;

public interface BinaryRequest extends CouchbaseRequest {

	ServiceType type();
	String bucket();
	String password();
}
