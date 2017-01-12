/*
 * Copyright (c) 2017 Couchbase, Inc.
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
import com.couchbase.client.core.config.CouchbaseBucketConfig;
import com.couchbase.client.core.message.config.BucketConfigRequest;
import com.couchbase.client.core.message.config.GetDesignDocumentsRequest;
import com.couchbase.client.core.node.Node;
import com.couchbase.client.core.service.ServiceType;
import org.junit.Test;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Verifies the functionality of the {@link ConfigLocator}.
 *
 * @author Michael Nitschinger
 * @since 1.4.1
 */
public class ConfigLocatorTest {

    /**
     * This test makes sure that when a BucketConfigRequest comes in without an explicit hostname is set
     * it goes through the regular round-robin routine, leading to eventual completion even if one of the
     * nodes is not available.
     */
    @Test
    public void shouldRerouteBucketConfigRequestWithoutHostname() throws Exception {
        Locator locator = new ConfigLocator();

        BucketConfigRequest request = mock(BucketConfigRequest.class);
        when(request.bucket()).thenReturn("default");
        ClusterConfig configMock = mock(ClusterConfig.class);
        CouchbaseBucketConfig bucketConfigMock = mock(CouchbaseBucketConfig.class);
        when(configMock.bucketConfig("default")).thenReturn(bucketConfigMock);
        List<Node> nodes = new ArrayList<Node>();
        Node node1Mock = mock(Node.class);
        when(node1Mock.hostname()).thenReturn(InetAddress.getByName("192.168.56.101"));
        when(node1Mock.serviceEnabled(ServiceType.CONFIG)).thenReturn(true);
        Node node2Mock = mock(Node.class);
        when(node2Mock.hostname()).thenReturn(InetAddress.getByName("192.168.56.102"));
        when(node2Mock.serviceEnabled(ServiceType.CONFIG)).thenReturn(true);
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
    public void shouldRouteGetDesignDocumentsRequestOnlyToViewNode() throws Exception {
        Locator locator = new ConfigLocator();

        GetDesignDocumentsRequest request = mock(GetDesignDocumentsRequest.class);
        when(request.bucket()).thenReturn("default");
        ClusterConfig configMock = mock(ClusterConfig.class);
        CouchbaseBucketConfig bucketConfigMock = mock(CouchbaseBucketConfig.class);

        when(configMock.bucketConfig("default")).thenReturn(bucketConfigMock);
        List<Node> nodes = new ArrayList<Node>();
        Node node1Mock = mock(Node.class);
        when(node1Mock.hostname()).thenReturn(InetAddress.getByName("192.168.56.101"));
        when(node1Mock.serviceEnabled(ServiceType.CONFIG)).thenReturn(true);
        when(node1Mock.serviceEnabled(ServiceType.VIEW)).thenReturn(false);
        Node node2Mock = mock(Node.class);
        when(node2Mock.hostname()).thenReturn(InetAddress.getByName("192.168.56.102"));
        when(node2Mock.serviceEnabled(ServiceType.CONFIG)).thenReturn(true);
        when(node2Mock.serviceEnabled(ServiceType.VIEW)).thenReturn(true);
        Node node3Mock = mock(Node.class);
        when(node3Mock.hostname()).thenReturn(InetAddress.getByName("192.168.56.103"));
        when(node3Mock.serviceEnabled(ServiceType.CONFIG)).thenReturn(true);
        when(node3Mock.serviceEnabled(ServiceType.VIEW)).thenReturn(true);
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
        verify(node2Mock, times(2)).send(request);
        verify(node3Mock, times(1)).send(request);
    }
}
