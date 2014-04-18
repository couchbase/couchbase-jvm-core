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
package com.couchbase.client.core.cluster;

import com.couchbase.client.core.env.CouchbaseEnvironment;
import com.couchbase.client.core.env.Environment;
import com.couchbase.client.core.message.CouchbaseRequest;
import com.couchbase.client.core.message.internal.SignalFlush;
import com.couchbase.client.core.node.Node;
import com.couchbase.client.core.node.locate.Locator;
import com.couchbase.client.core.state.LifecycleState;
import org.junit.Test;
import rx.Observable;

import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Verifies the functionality of {@link ClusterNodeHandler}.
 */
public class ClusterNodeHandlerTest {

    private static final Environment environment = new CouchbaseEnvironment();

    @Test
    public void shouldAddNodes() {
        Set<Node> nodes = new HashSet<Node>();
        ClusterNodeHandler handler = new ClusterNodeHandler(nodes, environment);

        assertEquals(0, nodes.size());
        Node nodeMock = mock(Node.class);
        when(nodeMock.connect()).thenReturn(Observable.just(LifecycleState.CONNECTED));
        handler.addNode(nodeMock).toBlockingObservable().single();
        assertEquals(1, nodes.size());
    }

    @Test
    public void shouldIgnoreAlreadyAddedNode() throws Exception {
        Set<Node> nodes = new HashSet<Node>();
        ClusterNodeHandler handler = new ClusterNodeHandler(nodes, environment);

        assertEquals(0, nodes.size());
        Node nodeMock = mock(Node.class);
        when(nodeMock.connect()).thenReturn(Observable.just(LifecycleState.CONNECTED));
        handler.addNode(nodeMock).toBlockingObservable().single();
        assertEquals(1, nodes.size());
        handler.addNode(nodeMock).toBlockingObservable().single();
        assertEquals(1, nodes.size());
    }

    @Test
    public void shouldRemoveNodes() {
        Set<Node> nodes = new HashSet<Node>();
        ClusterNodeHandler handler = new ClusterNodeHandler(nodes, environment);

        Node node1 = mock(Node.class);
        when(node1.connect()).thenReturn(Observable.just(LifecycleState.CONNECTED));
        when(node1.disconnect()).thenReturn(Observable.just(LifecycleState.DISCONNECTED));
        Node node2 = mock(Node.class);
        when(node2.connect()).thenReturn(Observable.just(LifecycleState.CONNECTED));
        when(node2.disconnect()).thenReturn(Observable.just(LifecycleState.DISCONNECTED));
        Node node3 = mock(Node.class);
        when(node3.connect()).thenReturn(Observable.just(LifecycleState.CONNECTED));
        when(node3.disconnect()).thenReturn(Observable.just(LifecycleState.DISCONNECTED));

        handler.addNode(node1).toBlockingObservable().single();
        handler.addNode(node2).toBlockingObservable().single();
        handler.addNode(node3).toBlockingObservable().single();

        assertEquals(3, nodes.size());
        handler.removeNode(node2).toBlockingObservable().single();
        assertEquals(2, nodes.size());
        assertTrue(nodes.contains(node1));
        assertTrue(nodes.contains(node3));
        assertFalse(nodes.contains(node2));
        handler.removeNode(node1).toBlockingObservable().single();
        assertEquals(1, nodes.size());
        assertTrue(nodes.contains(node3));
        assertFalse(nodes.contains(node2));
        assertFalse(nodes.contains(node1));
        handler.removeNode(node3).toBlockingObservable().single();
        assertEquals(0, nodes.size());
    }

    @Test
    public void shouldRemoveNodeEvenIfNotDisconnected() throws Exception {
        Set<Node> nodes = new HashSet<Node>();
        ClusterNodeHandler handler = new ClusterNodeHandler(nodes, environment);

        Node node1 = mock(Node.class);
        when(node1.connect()).thenReturn(Observable.just(LifecycleState.CONNECTED));
        when(node1.disconnect()).thenReturn(Observable.just(LifecycleState.DISCONNECTING));
        handler.addNode(node1).toBlockingObservable().single();
        assertEquals(1, nodes.size());

        handler.removeNode(node1).toBlockingObservable().single();
        assertEquals(0, nodes.size());
    }

    @Test
    public void shouldRouteEventToNode() throws Exception {
        ClusterNodeHandler handler = new DummyLocatorClusterNodeHandler(environment);
        Node mockNode = mock(Node.class);
        when(mockNode.connect()).thenReturn(Observable.just(LifecycleState.CONNECTED));
        handler.addNode(mockNode).toBlockingObservable().single();

        RequestEvent mockEvent = mock(RequestEvent.class);
        CouchbaseRequest mockRequest = mock(CouchbaseRequest.class);
        when(mockEvent.getRequest()).thenReturn(mockRequest);
        handler.onEvent(mockEvent, 0, true);
        verify(mockNode).send(mockRequest);
        verify(mockNode).send(SignalFlush.INSTANCE);
    }

    /**
     * Helper class which implements a dummy locator for testing purposes.
     */
    class DummyLocatorClusterNodeHandler extends ClusterNodeHandler {

        private Locator LOCATOR = new DummyLocator();

        DummyLocatorClusterNodeHandler(Environment environment) {
            super(environment);
        }

        @Override
        protected Locator locator(CouchbaseRequest request) {
            return LOCATOR;
        }

        class DummyLocator implements Locator {
            @Override
            public Observable<Node> locate(CouchbaseRequest request, Set<Node> nodes) {
                return Observable.from(nodes.iterator().next());
            }
        }
    }

}
