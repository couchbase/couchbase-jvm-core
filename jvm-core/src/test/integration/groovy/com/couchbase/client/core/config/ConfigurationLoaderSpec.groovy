package com.couchbase.client.core.config

import com.couchbase.client.core.cluster.CouchbaseCluster
import com.couchbase.client.core.environment.CouchbaseEnvironment
import com.couchbase.client.core.message.internal.AddNodeRequest
import com.couchbase.client.core.message.internal.EnableServiceRequest
import com.couchbase.client.core.service.ServiceType
import reactor.function.Consumer
import spock.lang.Specification;

/**
 * Integration tests for Configuration Loading.
 */
public class ConfigurationLoaderSpec extends Specification {

    def env = new CouchbaseEnvironment()
    def node = new InetSocketAddress(8091)
    def bucket = "default"
    def password = ""

    def "The HttpConfigurationLoader should load the raw configuration over http"() {
        setup:
        def cluster = new CouchbaseCluster(env)
        def loader = new HttpConfigurationLoader(env, node, bucket, password, cluster)

        cluster.send(new AddNodeRequest(node)).await()
        cluster.send(new EnableServiceRequest(node, ServiceType.CONFIG, bucket)).await()

        expect:
        while(true) {
            loader.load().onSuccess(new Consumer<String>() {
                @Override
                void accept(String s) {
                    System.out.println(s)
                }
            }).onError(new Consumer<Throwable>() {
                @Override
                void accept(Throwable throwable) {
                    throwable.printStackTrace()
                }
            })

        }
    }

}
