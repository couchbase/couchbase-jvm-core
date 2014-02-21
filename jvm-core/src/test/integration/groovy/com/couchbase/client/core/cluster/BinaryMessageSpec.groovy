package com.couchbase.client.core.cluster

import com.couchbase.client.core.message.binary.GetRequest
import com.couchbase.client.core.message.binary.GetResponse
import com.couchbase.client.core.message.binary.UpsertRequest
import com.couchbase.client.core.message.binary.UpsertResponse
import com.couchbase.client.core.message.common.ConnectRequest
import io.netty.buffer.Unpooled
import io.netty.util.CharsetUtil
import spock.lang.Specification

/**
 * Simple Integration tests for binary message types.
 */
class BinaryMessageSpec extends Specification {

    def "A CouchbaseCluster should set and get a document"() {
        setup:
        def cluster = new CouchbaseCluster()
        cluster.send(new ConnectRequest()).await()
        def id = "key1"
        def content = "Hello Couchbase!"

        when:

        def setResponse = cluster.<UpsertResponse>send(
            new UpsertRequest(id, Unpooled.copiedBuffer(content, CharsetUtil.UTF_8), "default", "")
        )

        then:
        setResponse.await()

        when:
        def getResponse = cluster.<GetResponse>send(new GetRequest(id, "default", ""))

        then:
        getResponse.await().content() == content
    }

}
