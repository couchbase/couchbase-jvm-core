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

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
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

        Node[] foundNodes = locator.locate(getRequestMock, nodes, configMock);
        assertEquals(node1Mock, foundNodes[0]);
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

        Node[] foundNodes = locator.locate(requestMock, nodes, mock(ClusterConfig.class));
        assertEquals(node2Mock.hostname(), foundNodes[0].hostname());
    }

}
