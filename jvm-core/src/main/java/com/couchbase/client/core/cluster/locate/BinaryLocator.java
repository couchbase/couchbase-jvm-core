package com.couchbase.client.core.cluster.locate;

import com.couchbase.client.core.config.Configuration;
import com.couchbase.client.core.config.DefaultConfiguration;
import com.couchbase.client.core.message.binary.BinaryRequest;
import com.couchbase.client.core.message.binary.BinaryResponse;
import com.couchbase.client.core.message.binary.GetBucketConfigRequest;
import com.couchbase.client.core.message.binary.GetRequest;
import com.couchbase.client.core.message.binary.UpsertRequest;
import com.couchbase.client.core.node.Node;
import io.netty.util.CharsetUtil;
import reactor.core.composable.Promise;
import reactor.event.registry.Registration;
import reactor.event.registry.Registry;

import java.util.Iterator;
import java.util.zip.CRC32;

/**
 * A locator that selects the right node for binary requests.
 */
public class BinaryLocator {

	public static Promise<BinaryResponse> locate(BinaryRequest request, Registry<Node> registry, Configuration config) {
		if (request instanceof GetBucketConfigRequest) {
			// TODO: config is null here, handle case when there IS a config already.
			GetBucketConfigRequest req = (GetBucketConfigRequest) request;
			Registration<? extends Node> registration = registry.select(req.node()).get(0);
			return registration.getObject().send(request);
		} else if (request instanceof GetRequest) {
			// TODO: find proper node for the request and set the parition ID

			DefaultConfiguration.PartitionMap partitionMap = config.partitionMap();
			String key = ((GetRequest) request).key();

			CRC32 crc32 = new CRC32();
			crc32.update(key.getBytes(CharsetUtil.UTF_8));
			long rv = (crc32.getValue() >> 16) & 0x7fff;
			int vBucket = (int) rv & partitionMap.partitions().size() - 1;
			int node = partitionMap.partitions().get(vBucket).get(0);
			//System.out.println(partitionMap.nodes().get(node));

			//Iterator<Registration<? extends Node>> iterator = registry.iterator();
			//while(iterator.hasNext()) {
			//	System.out.println(iterator.next());
			//}

			// TODO: register node in cluster with string hostname...
			// then look it up here..
			// Also todo:: replace $HOST with the hostname on bootstrap

			return registry.iterator().next().getObject().send(request);
		} else if (request instanceof UpsertRequest) {
			return registry.iterator().next().getObject().send(request);
		}

		return null;
	}
}
