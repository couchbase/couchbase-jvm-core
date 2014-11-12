package com.couchbase.client.core.config;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.netty.util.CharsetUtil;

import java.net.InetAddress;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;

@JsonIgnoreProperties(ignoreUnknown = true)
public class DefaultMemcachedBucketConfig extends AbstractBucketConfig implements MemcachedBucketConfig {

    private final long rev;
    private final TreeMap<Long, NodeInfo> ketamaNodes;

    /**
     * Creates a new {@link MemcachedBucketConfig}.
     *
     * @param rev the revision of the config.
     * @param name the name of the bucket.
     * @param locator the locator for this bucket.
     * @param uri the URI for this bucket.
     * @param streamingUri the streaming URI for this bucket.
     * @param nodeInfos related node information.
     * @param portInfos port info for the nodes, including services.
     */
    @JsonCreator
    public DefaultMemcachedBucketConfig(
        @JsonProperty("rev") long rev,
        @JsonProperty("name") String name,
        @JsonProperty("nodeLocator") String locator,
        @JsonProperty("uri") String uri,
        @JsonProperty("streamingUri") String streamingUri,
        @JsonProperty("nodes") List<NodeInfo> nodeInfos,
        @JsonProperty("nodesExt") List<PortInfo> portInfos) {
        super(name, BucketNodeLocator.fromConfig(locator), uri, streamingUri, nodeInfos, portInfos);
        this.rev = rev;
        this.ketamaNodes = new TreeMap<Long, NodeInfo>();
        populateKetamaNodes();
    }

    @Override
    public boolean tainted() {
        return false;
    }

    @Override
    public long rev() {
        return rev;
    }

    @Override
    public BucketType type() {
        return BucketType.MEMCACHED;
    }

    @Override
    public SortedMap<Long, NodeInfo> ketamaNodes() {
        return ketamaNodes;
    }

    private void populateKetamaNodes() {
        for (NodeInfo node : nodes()) {
            for (int i = 0; i < 40; i++) {
                MessageDigest md5;
                try {
                    md5 = MessageDigest.getInstance("MD5");
                    md5.update(keyForNode(node.hostname(), i).getBytes(CharsetUtil.UTF_8));
                    byte[] digest = md5.digest();
                    for (int j = 0; j < 4; j++) {
                        Long key = ((long) (digest[3 + j * 4] & 0xFF) << 24)
                            | ((long) (digest[2 + j * 4] & 0xFF) << 16)
                            | ((long) (digest[1 + j * 4] & 0xFF) << 8)
                            | (digest[j * 4] & 0xFF);
                        ketamaNodes.put(key, node);
                    }
                } catch (NoSuchAlgorithmException e) {
                    throw new IllegalStateException("Could not populate ketama nodes.", e);
                }
            }
        }
    }

    private String keyForNode(InetAddress hostname, int repetition) {
        return hostname.getHostName() + "-" + repetition;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("DefaultMemcachedBucketConfig{");
        sb.append("rev=").append(rev);
        sb.append(", ketamaNodes=").append(ketamaNodes);
        sb.append('}');
        return sb.toString();
    }
}
