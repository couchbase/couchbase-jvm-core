package com.couchbase.client.core.config

import com.couchbase.client.core.cluster.CouchbaseCluster
import com.couchbase.client.core.environment.CouchbaseEnvironment
import com.couchbase.client.core.message.internal.AddNodeRequest
import com.couchbase.client.core.message.internal.EnableServiceRequest
import com.couchbase.client.core.service.ServiceType
import reactor.function.Consumer
import spock.lang.Specification

import java.util.concurrent.TimeUnit;

/**
 * Integration tests for Configuration Loading.
 */
public class ConfigurationLoaderSpec extends Specification {

    def env = new CouchbaseEnvironment()
    def bucket = "default"
    def password = ""

    def "The HttpConfigurationLoader should load the raw configuration over http"() {
        setup:
        def node = new InetSocketAddress(8091)
        def cluster = new CouchbaseCluster(env)
        def loader = new HttpConfigurationLoader(env, node, bucket, password, cluster)

        cluster.send(new AddNodeRequest(node)).await()
        cluster.send(new EnableServiceRequest(node, ServiceType.CONFIG, bucket)).await()

        expect:
        def config = loader.loadRawConfig().await()
        config.startsWith("{") == true
        config.endsWith("}") == true
    }

    def "The BinaryConfigurationLoader should load the raw configuration over memcache"() {
        setup:
        def node = new InetSocketAddress(11210)
        def cluster = new CouchbaseCluster(env)
        def loader = new BinaryConfigurationLoader(env, node, bucket, password, cluster)

        cluster.send(new AddNodeRequest(node)).await()
        cluster.send(new EnableServiceRequest(node, ServiceType.BINARY, bucket)).await()

        expect:
        def config = loader.loadRawConfig().await()
        config.startsWith("{") == true
        config.endsWith("}") == true
    }

}
