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

	@JsonCreator
	public DefaultConfiguration(
		@JsonProperty("uri") String uri,
		@JsonProperty("streamingUri") String streamingUri,
		@JsonProperty("nodes") List<Node> nodes,
		@JsonProperty("nodeLocator") String locatorType
	) {
		this.uri = uri;
		this.streamingUri = streamingUri;
		this.nodes = nodes;
		this.locatorType = locatorType;
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

	public String uri() {
		return uri;
	}

	public String streamingUri() {
		return streamingUri;
	}

	public List<Node> nodes() {
		return nodes;
	}

	public String locatorType() {
		return locatorType;
	}
}
