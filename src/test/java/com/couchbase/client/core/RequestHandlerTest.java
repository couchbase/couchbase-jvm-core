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
package com.couchbase.client.core;

import com.couchbase.client.core.config.ClusterConfig;
import com.couchbase.client.core.env.CoreEnvironment;
import com.couchbase.client.core.env.DefaultCoreEnvironment;
import com.couchbase.client.core.message.CouchbaseRequest;
import com.couchbase.client.core.message.dcp.DCPRequest;
import com.couchbase.client.core.message.internal.SignalFlush;
import com.couchbase.client.core.message.kv.GetRequest;
import com.couchbase.client.core.message.query.QueryRequest;
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
import static org.junit.Assert.fail;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Verifies the functionality of {@link RequestHandler}.
 */
public class RequestHandlerTest {

    private static final CoreEnvironment environment = DefaultCoreEnvironment.create();
    private static final Observable<ClusterConfig> configObservable = Observable.empty();

    @Test
    public void shouldAddNodes() {
        Set<Node> nodes = new HashSet<Node>();
        RequestHandler handler = new RequestHandler(nodes, environment, configObservable, null);

        assertEquals(0, nodes.size());
        Node nodeMock = mock(Node.class);
        when(nodeMock.connect()).thenReturn(Observable.just(LifecycleState.CONNECTED));
        handler.addNode(nodeMock).toBlocking().single();
        assertEquals(1, nodes.size());
    }

    @Test
    public void shouldIgnoreAlreadyAddedNode() throws Exception {
        Set<Node> nodes = new HashSet<Node>();
        RequestHandler handler = new RequestHandler(nodes, environment, configObservable, null);

        assertEquals(0, nodes.size());
        Node nodeMock = mock(Node.class);
        when(nodeMock.connect()).thenReturn(Observable.just(LifecycleState.CONNECTED));
        handler.addNode(nodeMock).toBlocking().single();
        assertEquals(1, nodes.size());
        handler.addNode(nodeMock).toBlocking().single();
        assertEquals(1, nodes.size());
    }

    @Test
    public void shouldRemoveNodes() {
        Set<Node> nodes = new HashSet<Node>();
        RequestHandler handler = new RequestHandler(nodes, environment, configObservable, null);

        Node node1 = mock(Node.class);
        when(node1.connect()).thenReturn(Observable.just(LifecycleState.CONNECTED));
        when(node1.disconnect()).thenReturn(Observable.just(LifecycleState.DISCONNECTED));
        Node node2 = mock(Node.class);
        when(node2.connect()).thenReturn(Observable.just(LifecycleState.CONNECTED));
        when(node2.disconnect()).thenReturn(Observable.just(LifecycleState.DISCONNECTED));
        Node node3 = mock(Node.class);
        when(node3.connect()).thenReturn(Observable.just(LifecycleState.CONNECTED));
        when(node3.disconnect()).thenReturn(Observable.just(LifecycleState.DISCONNECTED));

        handler.addNode(node1).toBlocking().single();
        handler.addNode(node2).toBlocking().single();
        handler.addNode(node3).toBlocking().single();

        assertEquals(3, nodes.size());
        handler.removeNode(node2).toBlocking().single();
        assertEquals(2, nodes.size());
        assertTrue(nodes.contains(node1));
        assertTrue(nodes.contains(node3));
        assertFalse(nodes.contains(node2));
        handler.removeNode(node1).toBlocking().single();
        assertEquals(1, nodes.size());
        assertTrue(nodes.contains(node3));
        assertFalse(nodes.contains(node2));
        assertFalse(nodes.contains(node1));
        handler.removeNode(node3).toBlocking().single();
        assertEquals(0, nodes.size());
    }

    @Test
    public void shouldRemoveNodeEvenIfNotDisconnected() throws Exception {
        Set<Node> nodes = new HashSet<Node>();
        RequestHandler handler = new RequestHandler(nodes, environment, configObservable, null);

        Node node1 = mock(Node.class);
        when(node1.connect()).thenReturn(Observable.just(LifecycleState.CONNECTED));
        when(node1.disconnect()).thenReturn(Observable.just(LifecycleState.DISCONNECTING));
        handler.addNode(node1).toBlocking().single();
        assertEquals(1, nodes.size());

        handler.removeNode(node1).toBlocking().single();
        assertEquals(0, nodes.size());
    }

    @Test
    public void shouldRouteEventToNode() throws Exception {
        ClusterConfig mockClusterConfig = mock(ClusterConfig.class);
        when(mockClusterConfig.hasBucket(anyString())).thenReturn(Boolean.TRUE);
        Observable<ClusterConfig> mockConfigObservable = Observable.just(mockClusterConfig);

        RequestHandler handler = new DummyLocatorClusterNodeHandler(environment, mockConfigObservable);
        Node mockNode = mock(Node.class);
        when(mockNode.connect()).thenReturn(Observable.just(LifecycleState.CONNECTED));
        handler.addNode(mockNode).toBlocking().single();

        RequestEvent mockEvent = mock(RequestEvent.class);
        CouchbaseRequest mockRequest = mock(CouchbaseRequest.class);
        when(mockEvent.getRequest()).thenReturn(mockRequest);
        handler.onEvent(mockEvent, 0, true);
        verify(mockNode).send(mockRequest);
        verify(mockNode).send(SignalFlush.INSTANCE);
        verify(mockEvent).setRequest(null);
    }

    private void assertFeatureForRequest(RequestHandler handler, CouchbaseRequest request, boolean expectedOk) {
        try {
            handler.checkFeaturesForRequest(request);
            if (!expectedOk) {
                fail();
            }
        } catch (UnsupportedOperationException e) {
            if (expectedOk) {
                fail();
            }
            assertTrue(e.getMessage().startsWith("Request type needs a feature to be enabled in environment"));
        }
    }

    @Test
    public void shouldPreventFeatureDependentRequestsWhenFeatureDisabled() {
        QueryRequest mockQueryRequest = mock(QueryRequest.class);
        DCPRequest mockDcpRequest = mock(DCPRequest.class);
        CouchbaseRequest mockKeyValueRequest = mock(GetRequest.class);
        CoreEnvironment env = DefaultCoreEnvironment.create();
        RequestHandler handler = new DummyLocatorClusterNodeHandler(env);

        assertFeatureForRequest(handler, mockQueryRequest, false);
        assertFeatureForRequest(handler, mockDcpRequest, false);
        assertFeatureForRequest(handler, mockKeyValueRequest, true);
    }

    @Test
    public void shouldAllowQueryWhenQueryFeatureEnabled() {
        QueryRequest mockQueryRequest = mock(QueryRequest.class);
        DCPRequest mockDcpRequest = mock(DCPRequest.class);
        CouchbaseRequest mockKeyValueRequest = mock(GetRequest.class);
        CoreEnvironment env = DefaultCoreEnvironment.builder()
                .queryEnabled(true)
                .build();
        RequestHandler handler = new DummyLocatorClusterNodeHandler(env);

        assertFeatureForRequest(handler, mockQueryRequest, true);
        assertFeatureForRequest(handler, mockDcpRequest, false);
        assertFeatureForRequest(handler, mockKeyValueRequest, true);
    }

    @Test
    public void shouldAllowDcpWhenDcpFeatureEnabled() {
        QueryRequest mockQueryRequest = mock(QueryRequest.class);
        DCPRequest mockDcpRequest = mock(DCPRequest.class);
        CouchbaseRequest mockKeyValueRequest = mock(GetRequest.class);
        CoreEnvironment env = DefaultCoreEnvironment.builder()
                                                    .dcpEnabled(true)
                                                    .build();
        RequestHandler handler = new DummyLocatorClusterNodeHandler(env);

        assertFeatureForRequest(handler, mockQueryRequest, false);
        assertFeatureForRequest(handler, mockDcpRequest, true);
        assertFeatureForRequest(handler, mockKeyValueRequest, true);
    }

    /**
     * Helper class which implements a dummy locator for testing purposes.
     */
    class DummyLocatorClusterNodeHandler extends RequestHandler {

        private Locator LOCATOR = new DummyLocator();

        DummyLocatorClusterNodeHandler(CoreEnvironment environment) {
            super(environment, configObservable, null);
        }

        DummyLocatorClusterNodeHandler(CoreEnvironment environment,
                Observable<ClusterConfig> specificConfigObservable) {
            super(environment, specificConfigObservable, null);
        }

        @Override
        protected Locator locator(CouchbaseRequest request) {
            return LOCATOR;
        }

        class DummyLocator implements Locator {
            @Override
            public Node[] locate(CouchbaseRequest request, Set<Node> nodes, ClusterConfig config) {
                return new Node[] { nodes.iterator().next() };
            }
        }
    }

}
