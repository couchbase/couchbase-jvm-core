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

/**
 * Couchbase Core IO - Domain Model and Architecture
 * =================================================
 *
 * This documentation describes the domain model of the core package, covers important architecture and design
 * decisions and provides a solid introduction on the inner workings. For more detailed descriptions on the
 * individual components, see the package documentation for the corresponding package.
 *
 * Domain Model
 * ------------
 * The domain model of the application is inspired by DDD (Domain Driven Design) and is intended to clearly reflect the
 * domain language of a database client library. There are a few crucial components which need proper definition, so
 * here they are. Afterwards, they are discussed in context:
 *
 * * **Channel:** A {@link io.netty.channel.Channel} is the socket abstraction provided by the Netty framework. It
 *   handles all the underlying raw input and output communication.
 * * **Endpoint:** A {@link com.couchbase.client.core.endpoint.Endpoint} wraps a Channel and is, as far as the upper
 *   layers are concerned the most atomic receiver of a request. It eventually pushes the request into the channel if
 *   it is connected and able to serve it properly. Endpoints always serve a special purpose, for example they only
 *   handle view requests, query requests or binary requests.
 * * **Service:** A {@link com.couchbase.client.core.service.Service} contains 1 to N Endpoints. Its state is always
 *   a culmination of all owned Endpoint states. When it receives a request, it dispatches it - based on a strategy -
 *   into one Endpoint. As an endpoint, a Service only handles a special type of requests (view, query, binary,...).
 * * **Node:** A {@link com.couchbase.client.core.node.Node} is the logical equivalent of a Node in a Couchbase Server
 *   cluster. It has many Services attached to it, but not every node needs to have all the same Services.
 *
 * Here is a simplified diagram that shows the tree-like relations:
 *
 * ![Simplified Architecture](simple.png)
 *
 * Request/Response flow
 * ---------------------
 * The actual implementation is a little more complicated since some parts have been left out in the simplified
 * illustration. Especially the RingBuffers play an important role in the architecture. The following diagram
 * shows a more complete picture, explanation afterwards.
 *
 * ![Request/Response Flow](req-res-flow.png)
 *
 * Dependencies
 * ------------
 * Two dependencies are most crucial contributors to the current architecture:
 *
 * * [Netty](http://netty.io): Netty by far is currently the most scalable and performing IO library available on the
 *   JVM. We are utilizing for all IO traffic and by doing that we can use a central event loop pool and manage
 *   resources very efficiently. It also provides built-in support for functionality like SSL, compression and others,
 *   so we do not need to reinvent the wheel all the time.
 * * [RxJava](https://github.com/Netflix/RxJava): Since the earliest days, the Java SDK did provide asynchronous
 *   interfaces, but the Future shipped with the JDK is broken and does not provide enough flexibility. The reactive
 *   extensions (where RxJava is the canonical JVM implementation) provide a very feature rich set of asynchronous
 *   programming flow. Since it also provides blocking fallback semantics, we can ship the best of both worlds, both
 *   internally and externally facing to the SDK user.
 *
 * Other runtime dependencies are:
 *
 * * [LMAX Disruptor](https://github.com/LMAX-Exchange/disruptor): The Disruptor is the fastest RingBuffer
 *   implementation available on the JVM and used in many latency-sensitive applications. Internally, it is used to
 *   shed load and provide backpressure on both the request and response side, helping with implicit batching and
 *   proper release valves in case the system is overloaded.
 * * [Typesafe Config](https://github.com/typesafehub/config): A very versatile config library, with no dependencies
 *   on its own. It has a well-thought dependency management system and easily accessible syntax both across system
 *   properties and config files.
 * * [Jackson](https://github.com/FasterXML/jackson): One of the most versatile and fast JSON frameworks out there,
 *   widely used.
 *
 * Finally, we use the following dependencies for unit, integration and performance tests:
 *
 * * [JUnit](http://junit.org/): Classic unit testing framework.
 * * [OpenJDK JMH](http://openjdk.java.net/projects/code-tools/jmh/): The one and only sane choice when it comes to
 *   performance testing for predictable and JVM compliant results.
 * * [Logback](http://logback.qos.ch/): Powerful logging framework used for tests together with SLF4J.
 * * [Mockito](https://code.google.com/p/mockito/): A flexible mocking framework.
 *
 *
 * @startuml simple.png
 *
 *     cloud "Netty" {
 *         [View Channel 1]
 *         [View Channel 2]
 *         [Query Channel 1]
 *     }
 *
 *     [Node] --> [View Service]
 *     [Node] --> [Query Service]
 *
 *     [View Service] --> [View Endpoint 1]
 *     [View Service] --> [View Endpoint 2]
 *
 *     [Query Service] --> [Query Endpoint 1]
 *
 *     [View Endpoint 1] --> [View Channel 1]
 *     [View Endpoint 2] --> [View Channel 2]
 *     [Query Endpoint 1] --> [Query Channel 1]
 *
 * @enduml
 *
 * @author Michael Nitschinger
 * @since 1.0
 */
package com.couchbase.client.core;