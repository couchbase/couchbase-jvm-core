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

package com.couchbase.client.core.environment

import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import io.netty.channel.nio.NioEventLoopGroup;
import spock.lang.Specification

public class CouchbaseEnvironmentSpec extends Specification {

	def "A Configuration should dynamically calculate the number of IO and blocking threads"() {
        setup:
        ConfigFactory.invalidateCaches()

        when:
        CouchbaseEnvironment env = new CouchbaseEnvironment()

        then:
        env.ioPoolSize() == Runtime.getRuntime().availableProcessors()
        env.blockingPoolSize() == Runtime.getRuntime().availableProcessors()

        cleanup:
        env.shutdown()
	}

    def "A system property should override the default value"() {
        setup:
        System.setProperty("com.couchbase.client.core.io.poolSize", "2")
        System.setProperty("com.couchbase.client.core.blockingPoolSize", "4")
        ConfigFactory.invalidateCaches()

        when:
        CouchbaseEnvironment env = new CouchbaseEnvironment()

        then:
        env.ioPoolSize() == 2
        env.blockingPoolSize() == 4

        cleanup:
        System.clearProperty("com.couchbase.client.core.io.poolSize")
        System.clearProperty("com.couchbase.client.core.blockingPoolSize")
        env.shutdown()
    }

    def "A custom config should override the default one"() {
        setup:
        ConfigFactory.invalidateCaches()
        Config config = ConfigFactory.empty()

        when:
        CouchbaseEnvironment env = new CouchbaseEnvironment(config)

        then:
        thrown(EnvironmentException)
    }

    def "A bootstrapped environment should return the EventLoopGroup"() {
        setup:
        ConfigFactory.invalidateCaches()

        when:
        CouchbaseEnvironment env = new CouchbaseEnvironment()

        then:
        env.ioPool() instanceof NioEventLoopGroup
        env.ioPool().terminated == false

        cleanup:
        env.shutdown()
    }

}
