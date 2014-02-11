package com.couchbase.client.core.service;

/**
 * Represents the different {@link ServiceType}s and how they map onto buckets.
 */
public enum ServiceType {

	/**
	 * Views and Design Documents.
	 */
	DESIGN(BucketServiceMapping.ONE_FOR_ALL),

	/**
	 * Memcache type operations.
	 */
	BINARY(BucketServiceMapping.ONE_BY_ONE),

	/**
	 * UPR and TAP stream operations.
	 */
	STREAM(BucketServiceMapping.ONE_BY_ONE),

	/**
	 * 8091 config operations.
	 */
	CONFIG(BucketServiceMapping.ONE_FOR_ALL);

	private final BucketServiceMapping mapping;

	private ServiceType(BucketServiceMapping mapping) {
		this.mapping = mapping;
	}

	public BucketServiceMapping mapping() {
		return mapping;
	}
}
