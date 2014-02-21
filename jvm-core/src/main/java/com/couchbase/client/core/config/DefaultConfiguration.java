package com.couchbase.client.core.config;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;
import java.util.Map;

/**
 * The default implementation of a {@link Configuration}.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class DefaultConfiguration implements Configuration {

	private final String uri;
	private final String streamingUri;
	private final List<Node> nodes;
	private final String locatorType;
	private final PartitionMap partitionMap;

	@JsonCreator
	public DefaultConfiguration(
		@JsonProperty("uri") String uri,
		@JsonProperty("streamingUri") String streamingUri,
		@JsonProperty("nodes") List<Node> nodes,
		@JsonProperty("nodeLocator") String locatorType,
		@JsonProperty("vBucketServerMap") PartitionMap partitionMap
	) {
		this.uri = uri;
		this.streamingUri = streamingUri;
		this.nodes = nodes;
		this.locatorType = locatorType;
		this.partitionMap = partitionMap;
	}

	@Override
	public String uri() {
		return uri;
	}

	@Override
	public String streamingUri() {
		return streamingUri;
	}

	@Override
	public List<Node> nodes() {
		return nodes;
	}

	@Override
	public String locatorType() {
		return locatorType;
	}

	@Override
	public PartitionMap partitionMap() {
		return partitionMap;
	}

	@JsonIgnoreProperties(ignoreUnknown = true)
	public static class Node {

		private final String viewUri;
		private final String hostname;
		private final Map<String, Integer> ports;

		public Node(
			@JsonProperty("couchApiBase") String viewUri,
			@JsonProperty("hostname") String hostname,
			@JsonProperty("ports") Map<String, Integer> ports
		) {
			this.viewUri = viewUri;
			this.hostname = hostname;
			this.ports = ports;
		}

		public String viewUri() {
			return viewUri;
		}

		public String hostname() {
			return hostname;
		}

		public Map<String, Integer> ports() {
			return ports;
		}
	}

	@JsonIgnoreProperties(ignoreUnknown = true)
	public static class PartitionMap {

		private final String algorithm;
		private final int replicaCount;
		private final List<String> nodes;
		private final List<List<Integer>> partitions;

		public PartitionMap(
			@JsonProperty("hashAlgorithm") String algorithm,
			@JsonProperty("numReplicas") int replicaCount,
			@JsonProperty("serverList") List<String> nodes,
			@JsonProperty("vBucketMap") List<List<Integer>> partitions
		) {
			this.algorithm = algorithm;
			this.replicaCount = replicaCount;
			this.nodes = nodes;
			this.partitions = partitions;
		}

		public String algorithm() {
			return algorithm;
		}

		public int replicaCount() {
			return replicaCount;
		}

		public List<String> nodes() {
			return nodes;
		}

		public List<List<Integer>> partitions() {
			return partitions;
		}
	}

}
