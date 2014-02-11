package com.couchbase.client.core.service;


public enum BucketServiceMapping {

	/**
	 * The Service can handle all buckets at once.
	 */
	ONE_FOR_ALL,

	/**
	 * Every bucket needs its own service.
	 */
	ONE_BY_ONE

}
