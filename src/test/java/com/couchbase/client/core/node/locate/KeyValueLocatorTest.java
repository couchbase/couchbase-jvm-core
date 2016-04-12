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
import com.couchbase.client.core.config.CouchbaseBucketConfig;
import com.couchbase.client.core.config.DefaultNodeInfo;
import com.couchbase.client.core.config.NodeInfo;
import com.couchbase.client.core.message.kv.GetBucketConfigRequest;
import com.couchbase.client.core.message.kv.GetRequest;
import com.couchbase.client.core.node.Node;
import com.couchbase.client.core.state.LifecycleState;
import io.netty.util.CharsetUtil;
import org.junit.Test;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Verifies the functionality of the {@link KeyValueLocator}.
 *
 * @author Michael Nitschinger
 * @since 1.0.0
 */
public class KeyValueLocatorTest {

    @Test
    public void shouldLocateGetRequestForCouchbaseBucket() throws Exception {
        Locator locator = new KeyValueLocator();


        NodeInfo nodeInfo1 = new DefaultNodeInfo("foo", "192.168.56.101:11210", Collections.EMPTY_MAP);
        NodeInfo nodeInfo2 = new DefaultNodeInfo("foo", "192.168.56.102:11210", Collections.EMPTY_MAP);

        GetRequest getRequestMock = mock(GetRequest.class);
        ClusterConfig configMock = mock(ClusterConfig.class);
        List<Node> nodes = new ArrayList<Node>();
        Node node1Mock = mock(Node.class);
        when(node1Mock.hostname()).thenReturn(InetAddress.getByName("192.168.56.101"));
        Node node2Mock = mock(Node.class);
        when(node2Mock.hostname()).thenReturn(InetAddress.getByName("192.168.56.102"));
        nodes.addAll(Arrays.asList(node1Mock, node2Mock));
        CouchbaseBucketConfig bucketMock = mock(CouchbaseBucketConfig.class);
        when(getRequestMock.bucket()).thenReturn("bucket");
        when(getRequestMock.key()).thenReturn("key");
        when(getRequestMock.keyBytes()).thenReturn("key".getBytes(CharsetUtil.UTF_8));
        when(configMock.bucketConfig("bucket")).thenReturn(bucketMock);
        when(bucketMock.nodes()).thenReturn(Arrays.asList(nodeInfo1, nodeInfo2));
        when(bucketMock.numberOfPartitions()).thenReturn(1024);
        when(bucketMock.nodeIndexForMaster(656)).thenReturn((short) 0);
        when(bucketMock.nodeAtIndex(0)).thenReturn(nodeInfo1);

        locator.locateAndDispatch(getRequestMock, nodes, configMock, null, null);
        verify(node1Mock, times(1)).send(getRequestMock);
        verify(node2Mock, never()).send(getRequestMock);
    }

    @Test
    public void shouldPickTheRightNodeForGetBucketConfigRequest() throws Exception {
        Locator locator = new KeyValueLocator();

        List<Node> nodes = new ArrayList<Node>();
        Node node1Mock = mock(Node.class);
        when(node1Mock.hostname()).thenReturn(InetAddress.getByName("192.168.56.101"));
        when(node1Mock.isState(LifecycleState.CONNECTED)).thenReturn(true);
        Node node2Mock = mock(Node.class);
        when(node2Mock.hostname()).thenReturn(InetAddress.getByName("192.168.56.102"));
        when(node2Mock.isState(LifecycleState.CONNECTED)).thenReturn(true);
        nodes.addAll(Arrays.asList(node1Mock, node2Mock));

        GetBucketConfigRequest requestMock = mock(GetBucketConfigRequest.class);
        when(requestMock.hostname()).thenReturn(InetAddress.getByName("192.168.56.102"));

        locator.locateAndDispatch(requestMock, nodes, mock(ClusterConfig.class), null, null);
        verify(node1Mock, never()).send(requestMock);
        verify(node2Mock, times(1)).send(requestMock);
    }

}
