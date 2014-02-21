package com.couchbase.client.core.message.binary;

import com.couchbase.client.core.message.CouchbaseRequest;
import com.couchbase.client.core.service.ServiceType;

/**
 * Created by michael on 21/02/14.
 */
public interface BinaryRequest extends CouchbaseRequest {

	ServiceType type();
	String bucket();
}
