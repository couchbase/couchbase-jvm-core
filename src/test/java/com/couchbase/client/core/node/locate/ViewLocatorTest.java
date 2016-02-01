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
package com.couchbase.client.core.node.locate;

import com.couchbase.client.core.ServiceNotAvailableException;
import com.couchbase.client.core.config.ClusterConfig;
import com.couchbase.client.core.config.CouchbaseBucketConfig;
import com.couchbase.client.core.config.MemcachedBucketConfig;
import com.couchbase.client.core.message.CouchbaseRequest;
import com.couchbase.client.core.message.CouchbaseResponse;
import com.couchbase.client.core.message.view.ViewQueryRequest;
import com.couchbase.client.core.node.Node;
import com.couchbase.client.core.service.ServiceType;
import org.junit.Test;
import rx.observers.TestSubscriber;
import rx.subjects.AsyncSubject;
import rx.subjects.Subject;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Verifies the functionality of the {@link ViewLocator}.
 *
 * @author Michael Nitschinger
 * @since 1.0.2
 */
public class ViewLocatorTest {

    @Test
    public void shouldSelectNextNode() throws Exception {
        Locator locator = new ViewLocator();

        ViewQueryRequest request = mock(ViewQueryRequest.class);
        when(request.bucket()).thenReturn("default");
        ClusterConfig configMock = mock(ClusterConfig.class);
        CouchbaseBucketConfig bucketConfigMock = mock(CouchbaseBucketConfig.class);
        when(bucketConfigMock.hasPrimaryPartitionsOnNode(any(InetAddress.class))).thenReturn(true);
        when(configMock.bucketConfig("default")).thenReturn(bucketConfigMock);
        List<Node> nodes = new ArrayList<Node>();
        Node node1Mock = mock(Node.class);
        when(node1Mock.hostname()).thenReturn(InetAddress.getByName("192.168.56.101"));
        when(node1Mock.serviceEnabled(ServiceType.VIEW)).thenReturn(true);
        Node node2Mock = mock(Node.class);
        when(node2Mock.hostname()).thenReturn(InetAddress.getByName("192.168.56.102"));
        when(node2Mock.serviceEnabled(ServiceType.VIEW)).thenReturn(true);
        nodes.addAll(Arrays.asList(node1Mock, node2Mock));

        Node[] located = locator.locate(request, nodes, configMock);
        assertEquals(1, located.length);
        InetAddress foundFirst = located[0].hostname();

        located = locator.locate(request, nodes, configMock);
        assertEquals(1, located.length);
        InetAddress foundSecond = located[0].hostname();

        located = locator.locate(request, nodes, configMock);
        assertEquals(1, located.length);
        InetAddress foundLast = located[0].hostname();

        assertEquals(foundFirst, foundLast);
        assertNotEquals(foundFirst, foundSecond);
    }

    @Test
    public void shouldSkipNodeWithoutPartition() throws Exception {
        Locator locator = new ViewLocator();

        ViewQueryRequest request = mock(ViewQueryRequest.class);
        when(request.bucket()).thenReturn("default");
        ClusterConfig configMock = mock(ClusterConfig.class);
        CouchbaseBucketConfig bucketConfigMock = mock(CouchbaseBucketConfig.class);
        when(bucketConfigMock.hasPrimaryPartitionsOnNode(InetAddress.getByName("192.168.56.101"))).thenReturn(false);
        when(bucketConfigMock.hasPrimaryPartitionsOnNode(InetAddress.getByName("192.168.56.102"))).thenReturn(true);

        when(configMock.bucketConfig("default")).thenReturn(bucketConfigMock);
        List<Node> nodes = new ArrayList<Node>();
        Node node1Mock = mock(Node.class);
        when(node1Mock.hostname()).thenReturn(InetAddress.getByName("192.168.56.101"));
        when(node1Mock.serviceEnabled(ServiceType.VIEW)).thenReturn(true);
        Node node2Mock = mock(Node.class);
        when(node2Mock.hostname()).thenReturn(InetAddress.getByName("192.168.56.102"));
        when(node2Mock.serviceEnabled(ServiceType.VIEW)).thenReturn(true);
        nodes.addAll(Arrays.asList(node1Mock, node2Mock));

        Node[] located = locator.locate(request, nodes, configMock);
        assertEquals(1, located.length);
        InetAddress foundFirst = located[0].hostname();

        located = locator.locate(request, nodes, configMock);
        assertEquals(1, located.length);
        InetAddress foundSecond = located[0].hostname();

        located = locator.locate(request, nodes, configMock);
        assertEquals(1, located.length);
        InetAddress foundThird = located[0].hostname();

        assertEquals(foundFirst, InetAddress.getByName("192.168.56.102"));
        assertEquals(foundSecond, InetAddress.getByName("192.168.56.102"));
        assertEquals(foundThird, InetAddress.getByName("192.168.56.102"));
    }

    @Test
    public void shouldSkipNodeWithoutServiceEnabled() throws Exception {
        Locator locator = new ViewLocator();

        ViewQueryRequest request = mock(ViewQueryRequest.class);
        when(request.bucket()).thenReturn("default");
        ClusterConfig configMock = mock(ClusterConfig.class);
        CouchbaseBucketConfig bucketConfigMock = mock(CouchbaseBucketConfig.class);
        when(bucketConfigMock.hasPrimaryPartitionsOnNode(InetAddress.getByName("192.168.56.101"))).thenReturn(true);
        when(bucketConfigMock.hasPrimaryPartitionsOnNode(InetAddress.getByName("192.168.56.102"))).thenReturn(false);
        when(bucketConfigMock.hasPrimaryPartitionsOnNode(InetAddress.getByName("192.168.56.103"))).thenReturn(true);

        when(configMock.bucketConfig("default")).thenReturn(bucketConfigMock);
        List<Node> nodes = new ArrayList<Node>();
        Node node1Mock = mock(Node.class);
        when(node1Mock.hostname()).thenReturn(InetAddress.getByName("192.168.56.101"));
        when(node1Mock.serviceEnabled(ServiceType.VIEW)).thenReturn(true);
        Node node2Mock = mock(Node.class);
        when(node2Mock.hostname()).thenReturn(InetAddress.getByName("192.168.56.102"));
        when(node2Mock.serviceEnabled(ServiceType.VIEW)).thenReturn(false);
        Node node3Mock = mock(Node.class);
        when(node3Mock.hostname()).thenReturn(InetAddress.getByName("192.168.56.103"));
        when(node3Mock.serviceEnabled(ServiceType.VIEW)).thenReturn(true);
        nodes.addAll(Arrays.asList(node1Mock, node2Mock, node3Mock));

        Node[] located = locator.locate(request, nodes, configMock);
        assertEquals(1, located.length);
        InetAddress foundFirst = located[0].hostname();

        located = locator.locate(request, nodes, configMock);
        assertEquals(1, located.length);
        InetAddress foundSecond = located[0].hostname();

        located = locator.locate(request, nodes, configMock);
        assertEquals(1, located.length);
        InetAddress foundThird = located[0].hostname();

        located = locator.locate(request, nodes, configMock);
        assertEquals(1, located.length);
        InetAddress foundFourth = located[0].hostname();

        assertEquals(foundFirst, InetAddress.getByName("192.168.56.101"));
        assertEquals(foundSecond, InetAddress.getByName("192.168.56.103"));
        assertEquals(foundThird, InetAddress.getByName("192.168.56.103"));
        assertEquals(foundFourth, InetAddress.getByName("192.168.56.101"));
    }

    @Test
    public void shouldFailWhenUsedAgainstMemcacheBucket() {
        Locator locator = new ViewLocator();

        ClusterConfig config = mock(ClusterConfig.class);
        when(config.bucketConfig("default")).thenReturn(mock(MemcachedBucketConfig.class));

        CouchbaseRequest request = mock(ViewQueryRequest.class);
        Subject<CouchbaseResponse, CouchbaseResponse> response = AsyncSubject.create();
        when(request.bucket()).thenReturn("default");
        when(request.observable()).thenReturn(response);

        TestSubscriber<CouchbaseResponse> subscriber = new TestSubscriber<CouchbaseResponse>();
        response.subscribe(subscriber);

        Node[] located = locator.locate(request, Collections.<Node>emptyList(), config);

        assertNull(located);

        subscriber.awaitTerminalEvent(1, TimeUnit.SECONDS);
        List<Throwable> errors = subscriber.getOnErrorEvents();
        assertEquals(1, errors.size());
        assertTrue(errors.get(0) instanceof ServiceNotAvailableException);
    }

}
