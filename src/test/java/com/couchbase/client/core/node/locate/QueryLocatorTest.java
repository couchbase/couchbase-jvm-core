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

import com.couchbase.client.core.config.ClusterConfig;
import com.couchbase.client.core.message.query.GenericQueryRequest;
import com.couchbase.client.core.message.query.QueryRequest;
import com.couchbase.client.core.node.Node;
import com.couchbase.client.core.service.ServiceType;
import com.couchbase.client.core.utils.NetworkAddress;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Verifies the functionality of the {@link QueryLocator}.
 *
 * @author Michael Nitschinger
 * @since 1.0.2
 */
public class QueryLocatorTest {

    @Test
    public void shouldSelectNextNode() throws Exception {
        Locator locator = new QueryLocator();

        QueryRequest request = mock(GenericQueryRequest.class);
        ClusterConfig configMock = mock(ClusterConfig.class);
        List<Node> nodes = new ArrayList<Node>();

        Node node1Mock = mock(Node.class);
        when(node1Mock.serviceEnabled(ServiceType.QUERY)).thenReturn(true);
        when(node1Mock.hostname()).thenReturn(NetworkAddress.create("192.168.56.101"));
        Node node2Mock = mock(Node.class);
        when(node2Mock.serviceEnabled(ServiceType.QUERY)).thenReturn(true);
        when(node2Mock.hostname()).thenReturn(NetworkAddress.create("192.168.56.102"));
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
    public void shouldSkipNodeWithoutServiceEnabled() throws Exception {
        Locator locator = new QueryLocator();

        GenericQueryRequest request = mock(GenericQueryRequest.class);
        when(request.bucket()).thenReturn("default");
        ClusterConfig configMock = mock(ClusterConfig.class);

        List<Node> nodes = new ArrayList<Node>();
        Node node1Mock = mock(Node.class);
        when(node1Mock.hostname()).thenReturn(NetworkAddress.create("192.168.56.101"));
        when(node1Mock.serviceEnabled(ServiceType.QUERY)).thenReturn(false);
        Node node2Mock = mock(Node.class);
        when(node2Mock.hostname()).thenReturn(NetworkAddress.create("192.168.56.102"));
        when(node2Mock.serviceEnabled(ServiceType.QUERY)).thenReturn(true);
        Node node3Mock = mock(Node.class);
        when(node3Mock.hostname()).thenReturn(NetworkAddress.create("192.168.56.103"));
        when(node3Mock.serviceEnabled(ServiceType.QUERY)).thenReturn(false);
        nodes.addAll(Arrays.asList(node1Mock, node2Mock, node3Mock));

        locator.locateAndDispatch(request, nodes, configMock, null, null);
        verify(node1Mock, never()).send(request);
        verify(node2Mock, times(1)).send(request);
        verify(node3Mock, never()).send(request);

        locator.locateAndDispatch(request, nodes, configMock, null, null);
        verify(node1Mock, never()).send(request);
        verify(node2Mock, times(2)).send(request);
        verify(node3Mock, never()).send(request);

        locator.locateAndDispatch(request, nodes, configMock, null, null);
        verify(node1Mock, never()).send(request);
        verify(node2Mock, times(3)).send(request);
        verify(node3Mock, never()).send(request);

        locator.locateAndDispatch(request, nodes, configMock, null, null);
        verify(node1Mock, never()).send(request);
        verify(node2Mock, times(4)).send(request);
        verify(node3Mock, never()).send(request);
    }

    /**
     * Regression test for JCBC-1007.
     *
     * Considering the case with 2 data nodes and then 2 query nodes, the first query node should not
     * keep the burden of the "next node after" the two data nodes and the traffic should be still
     * shared evenly.
     */
    @Test
    public void shouldDistributeFairlyUnderMDS() throws Exception {
        Locator locator = new QueryLocator();

        GenericQueryRequest request = mock(GenericQueryRequest.class);
        when(request.bucket()).thenReturn("default");
        ClusterConfig configMock = mock(ClusterConfig.class);

        List<Node> nodes = new ArrayList<Node>();
        Node node1Mock = mock(Node.class);
        when(node1Mock.hostname()).thenReturn(NetworkAddress.create("192.168.56.101"));
        when(node1Mock.serviceEnabled(ServiceType.QUERY)).thenReturn(false);
        Node node2Mock = mock(Node.class);
        when(node2Mock.hostname()).thenReturn(NetworkAddress.create("192.168.56.102"));
        when(node2Mock.serviceEnabled(ServiceType.QUERY)).thenReturn(false);
        Node node3Mock = mock(Node.class);
        when(node3Mock.hostname()).thenReturn(NetworkAddress.create("192.168.56.103"));
        when(node3Mock.serviceEnabled(ServiceType.QUERY)).thenReturn(true);
        Node node4Mock = mock(Node.class);
        when(node4Mock.hostname()).thenReturn(NetworkAddress.create("192.168.56.104"));
        when(node4Mock.serviceEnabled(ServiceType.QUERY)).thenReturn(true);
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
