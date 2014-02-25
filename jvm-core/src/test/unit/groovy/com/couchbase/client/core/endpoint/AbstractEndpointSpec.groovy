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

package com.couchbase.client.core.endpoint

import spock.lang.Specification

/**
 * Verifies generic functionality needed for all {@link Endpoint}s and provided through the {@link AbstractEndpoint}.
 */
class AbstractEndpointSpec extends Specification {

    def "A Endpoint should be disconnect after creation"() {
        expect:
        true == false
    }

    def "A Endpoint should connect successfully to the underlying channel"() {
        expect:
        true == false
    }

    def "A Endpoint should swallow duplicate connect attempts"() {
        expect:
        true == false
    }

    def "A Endpoint should swallow a connect attempt if already connected"() {
        expect:
        true == false
    }

    def "A Endpoint should disconnect if currently connected"() {
        expect:
        true == false
    }

    def "A Endpoint should stop a connect attempt if disconnect overrides it"() {
        expect:
        true == false
    }

    def "A Endpoint should reconnect with backoff"() {
        expect:
        true == false
    }

    def "A Endpoint should connect with backoff until disconnect stops it"() {
        expect:
        true == false
    }

    def "A Endpoint should start reconnecting once notified from the underlying channel that it is inactive"() {
        expect:
        true == false
    }


    def "A Endpoint should accept a send message if connected"() {
        expect:
        true == false
    }

    def "A Endpoint should not accept a send message if not connected"() {
        expect:
        true == false
    }

    def "A Endpoint should stream lifecycle states to its consumers"() {
        expect:
        true == false
    }

}
