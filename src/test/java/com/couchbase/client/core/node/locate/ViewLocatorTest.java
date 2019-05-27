/*
 * Copyright (c) 2016 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
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
        Locator locator = new ViewLocator(0);

        ViewQueryRequest request = mock(ViewQueryRequest.class);
        when(request.bucket()).thenReturn("default");
        ClusterConfig configMock = mock(ClusterConfig.class);
        CouchbaseBucketConfig bucketConfigMock = mock(CouchbaseBucketConfig.class);
        when(bucketConfigMock.hasPrimaryPartitionsOnNode(any(String.class))).thenReturn(true);
        when(configMock.bucketConfig("default")).thenReturn(bucketConfigMock);
        List<Node> nodes = new ArrayList<Node>();
        Node node1Mock = mock(Node.class);
        when(node1Mock.hostname()).thenReturn("192.168.56.101");
        when(node1Mock.serviceEnabled(ServiceType.VIEW)).thenReturn(true);
        Node node2Mock = mock(Node.class);
        when(node2Mock.hostname()).thenReturn("192.168.56.102");
        when(node2Mock.serviceEnabled(ServiceType.VIEW)).thenReturn(true);
        nodes.addAll(Arrays.asList(node1Mock, node2Mock));

        locator.locateAndDispatch(request, nodes, configMock, null, null);
        verify(node1Mock, times(1)).send(request);
        verify(node2Mock, never()).send(request);

        locator.locateAndDispatch(request, nodes, configMock, null, null);
        verify(node1Mock, times(1)).send(request);
        verify(node2Mock, times(1)).send(request);

        locator.locateAndDispatch(request, nodes, configMock, null, null);
        verify(node1Mock, times(2)).send(request);
        verify(node2Mock, times(1)).send(request);
    }

    @Test
    public void shouldSkipNodeWithoutPartition() throws Exception {
        Locator locator = new ViewLocator(0);

        ViewQueryRequest request = mock(ViewQueryRequest.class);
        when(request.bucket()).thenReturn("default");
        ClusterConfig configMock = mock(ClusterConfig.class);
        CouchbaseBucketConfig bucketConfigMock = mock(CouchbaseBucketConfig.class);
        when(bucketConfigMock.hasPrimaryPartitionsOnNode("192.168.56.101")).thenReturn(false);
        when(bucketConfigMock.hasPrimaryPartitionsOnNode("192.168.56.102")).thenReturn(true);

        when(configMock.bucketConfig("default")).thenReturn(bucketConfigMock);
        List<Node> nodes = new ArrayList<Node>();
        Node node1Mock = mock(Node.class);
        when(node1Mock.hostname()).thenReturn("192.168.56.101");
        when(node1Mock.serviceEnabled(ServiceType.VIEW)).thenReturn(true);
        Node node2Mock = mock(Node.class);
        when(node2Mock.hostname()).thenReturn("192.168.56.102");
        when(node2Mock.serviceEnabled(ServiceType.VIEW)).thenReturn(true);
        nodes.addAll(Arrays.asList(node1Mock, node2Mock));

        locator.locateAndDispatch(request, nodes, configMock, null, null);
        verify(node1Mock, never()).send(request);
        verify(node2Mock, times(1)).send(request);

        locator.locateAndDispatch(request, nodes, configMock, null, null);
        verify(node1Mock, never()).send(request);
        verify(node2Mock, times(2)).send(request);

        locator.locateAndDispatch(request, nodes, configMock, null, null);
        verify(node1Mock, never()).send(request);
        verify(node2Mock, times(3)).send(request);
    }

    @Test
    public void shouldSkipNodeWithoutServiceEnabled() throws Exception {
        Locator locator = new ViewLocator(0);

        ViewQueryRequest request = mock(ViewQueryRequest.class);
        when(request.bucket()).thenReturn("default");
        ClusterConfig configMock = mock(ClusterConfig.class);
        CouchbaseBucketConfig bucketConfigMock = mock(CouchbaseBucketConfig.class);
        when(bucketConfigMock.hasPrimaryPartitionsOnNode("192.168.56.101")).thenReturn(true);
        when(bucketConfigMock.hasPrimaryPartitionsOnNode("192.168.56.102")).thenReturn(false);
        when(bucketConfigMock.hasPrimaryPartitionsOnNode("192.168.56.103")).thenReturn(true);

        when(configMock.bucketConfig("default")).thenReturn(bucketConfigMock);
        List<Node> nodes = new ArrayList<Node>();
        Node node1Mock = mock(Node.class);
        when(node1Mock.hostname()).thenReturn("192.168.56.101");
        when(node1Mock.serviceEnabled(ServiceType.VIEW)).thenReturn(true);
        Node node2Mock = mock(Node.class);
        when(node2Mock.hostname()).thenReturn("192.168.56.102");
        when(node2Mock.serviceEnabled(ServiceType.VIEW)).thenReturn(false);
        Node node3Mock = mock(Node.class);
        when(node3Mock.hostname()).thenReturn("192.168.56.103");
        when(node3Mock.serviceEnabled(ServiceType.VIEW)).thenReturn(true);
        nodes.addAll(Arrays.asList(node1Mock, node2Mock, node3Mock));

        locator.locateAndDispatch(request, nodes, configMock, null, null);
        verify(node1Mock, times(1)).send(request);
        verify(node2Mock, never()).send(request);
        verify(node3Mock, never()).send(request);

        locator.locateAndDispatch(request, nodes, configMock, null, null);
        verify(node1Mock, times(1)).send(request);
        verify(node2Mock, never()).send(request);
        verify(node3Mock, times(1)).send(request);

        locator.locateAndDispatch(request, nodes, configMock, null, null);
        verify(node1Mock, times(2)).send(request);
        verify(node2Mock, never()).send(request);
        verify(node3Mock, times(1)).send(request);

        locator.locateAndDispatch(request, nodes, configMock, null, null);
        verify(node1Mock, times(2)).send(request);
        verify(node2Mock, never()).send(request);
        verify(node3Mock, times(2)).send(request);
    }

    @Test
    public void shouldFailWhenUsedAgainstMemcacheBucket() {
        Locator locator = new ViewLocator(0);

        ClusterConfig config = mock(ClusterConfig.class);
        when(config.bucketConfig("default")).thenReturn(mock(MemcachedBucketConfig.class));

        CouchbaseRequest request = mock(ViewQueryRequest.class);
        Subject<CouchbaseResponse, CouchbaseResponse> response = AsyncSubject.create();
        when(request.bucket()).thenReturn("default");
        when(request.observable()).thenReturn(response);

        TestSubscriber<CouchbaseResponse> subscriber = new TestSubscriber<CouchbaseResponse>();
        response.subscribe(subscriber);

        locator.locateAndDispatch(request, Collections.<Node>emptyList(), config, null, null);

        subscriber.awaitTerminalEvent(1, TimeUnit.SECONDS);
        List<Throwable> errors = subscriber.getOnErrorEvents();
        assertEquals(1, errors.size());
        assertTrue(errors.get(0) instanceof ServiceNotAvailableException);
    }

    @Test
    public void shouldDistributeFairlyUnderMDS() throws Exception {
        Locator locator = new ViewLocator(0);

        ViewQueryRequest request = mock(ViewQueryRequest.class);
        when(request.bucket()).thenReturn("default");
        ClusterConfig configMock = mock(ClusterConfig.class);
        CouchbaseBucketConfig bucketConfigMock = mock(CouchbaseBucketConfig.class);
        when(bucketConfigMock.hasPrimaryPartitionsOnNode("192.168.56.101")).thenReturn(false);
        when(bucketConfigMock.hasPrimaryPartitionsOnNode("192.168.56.102")).thenReturn(false);
        when(bucketConfigMock.hasPrimaryPartitionsOnNode("192.168.56.103")).thenReturn(true);
        when(bucketConfigMock.hasPrimaryPartitionsOnNode("192.168.56.104")).thenReturn(true);

        when(configMock.bucketConfig("default")).thenReturn(bucketConfigMock);
        List<Node> nodes = new ArrayList<Node>();
        Node node1Mock = mock(Node.class);
        when(node1Mock.hostname()).thenReturn("192.168.56.101");
        when(node1Mock.serviceEnabled(ServiceType.VIEW)).thenReturn(false);
        Node node2Mock = mock(Node.class);
        when(node2Mock.hostname()).thenReturn("192.168.56.102");
        when(node2Mock.serviceEnabled(ServiceType.VIEW)).thenReturn(false);
        Node node3Mock = mock(Node.class);
        when(node3Mock.hostname()).thenReturn("192.168.56.103");
        when(node3Mock.serviceEnabled(ServiceType.VIEW)).thenReturn(true);
        Node node4Mock = mock(Node.class);
        when(node4Mock.hostname()).thenReturn("192.168.56.104");
        when(node4Mock.serviceEnabled(ServiceType.VIEW)).thenReturn(true);
        nodes.addAll(Arrays.asList(node1Mock, node2Mock, node3Mock, node4Mock));


        locator.locateAndDispatch(request, nodes, configMock, null, null);
        verify(node1Mock, never()).send(request);
        verify(node2Mock, never()).send(request);
        verify(node3Mock, times(1)).send(request);
        verify(node4Mock, never()).send(request);


        locator.locateAndDispatch(request, nodes, configMock, null, null);
        verify(node1Mock, never()).send(request);
        verify(node2Mock, never()).send(request);
        verify(node3Mock, times(1)).send(request);
        verify(node4Mock, times(1)).send(request);

        locator.locateAndDispatch(request, nodes, configMock, null, null);
        verify(node1Mock, never()).send(request);
        verify(node2Mock, never()).send(request);
        verify(node3Mock, times(2)).send(request);
        verify(node4Mock, times(1)).send(request);

        locator.locateAndDispatch(request, nodes, configMock, null, null);
        verify(node1Mock, never()).send(request);
        verify(node2Mock, never()).send(request);
        verify(node3Mock, times(2)).send(request);
        verify(node4Mock, times(2)).send(request);
    }

}
