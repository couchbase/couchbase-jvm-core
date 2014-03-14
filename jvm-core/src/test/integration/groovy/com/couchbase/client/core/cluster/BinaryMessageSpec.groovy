/**
 * Copyright (C) 2014 Couchbase, Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALING
 * IN THE SOFTWARE.
 */

package com.couchbase.client.core.cluster

import com.couchbase.client.core.message.binary.GetRequest
import com.couchbase.client.core.message.binary.GetResponse
import com.couchbase.client.core.message.binary.UpsertRequest
import com.couchbase.client.core.message.binary.UpsertResponse
import com.couchbase.client.core.message.common.ConnectRequest
import io.netty.buffer.Unpooled
import io.netty.util.CharsetUtil
import reactor.function.Consumer
import reactor.function.Function
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
