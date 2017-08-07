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
package com.couchbase.client.core;

import com.couchbase.client.core.config.BucketConfig;
import com.couchbase.client.core.config.ClusterConfig;
import com.couchbase.client.core.config.DefaultClusterConfig;
import com.couchbase.client.core.env.CoreEnvironment;
import com.couchbase.client.core.env.DefaultCoreEnvironment;
import com.couchbase.client.core.message.CouchbaseRequest;
import com.couchbase.client.core.message.CouchbaseResponse;
import com.couchbase.client.core.message.internal.SignalFlush;
import com.couchbase.client.core.node.Node;
import com.couchbase.client.core.node.locate.Locator;
import com.couchbase.client.core.retry.FailFastRetryStrategy;
import com.couchbase.client.core.retry.RetryHelper;
import com.couchbase.client.core.service.ServiceType;
import com.couchbase.client.core.state.LifecycleState;
import com.lmax.disruptor.RingBuffer;
import org.junit.Test;
import org.mockito.Mockito;
import rx.Observable;
import rx.subjects.AsyncSubject;
import rx.subjects.PublishSubject;
import rx.subjects.Subject;

import java.util.Collections;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
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
        CopyOnWriteArrayList<Node> nodes = new CopyOnWriteArrayList<Node>();
        RequestHandler handler = new RequestHandler(nodes, environment, configObservable, null);

        assertEquals(0, nodes.size());
        Node nodeMock = mock(Node.class);
        when(nodeMock.connect()).thenReturn(Observable.just(LifecycleState.CONNECTED));
        handler.addNode(nodeMock).toBlocking().single();
        assertEquals(1, nodes.size());
    }

    @Test
    public void shouldIgnoreAlreadyAddedNode() throws Exception {
        CopyOnWriteArrayList<Node> nodes = new CopyOnWriteArrayList<Node>();
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
        CopyOnWriteArrayList<Node> nodes = new CopyOnWriteArrayList<Node>();
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
        CopyOnWriteArrayList<Node> nodes = new CopyOnWriteArrayList<Node>();
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
        when(mockNode.state()).thenReturn(LifecycleState.CONNECTED);
        handler.addNode(mockNode).toBlocking().single();

        RequestEvent mockEvent = mock(RequestEvent.class);
        CouchbaseRequest mockRequest = mock(CouchbaseRequest.class);
        when(mockRequest.isActive()).thenReturn(true);
        when(mockEvent.getRequest()).thenReturn(mockRequest);
        handler.onEvent(mockEvent, 0, true);
        verify(mockNode).send(mockRequest);
        verify(mockNode).send(SignalFlush.INSTANCE);
        verify(mockEvent).setRequest(null);
    }

    private void assertFeatureForRequest(RequestHandler handler, CouchbaseRequest request, boolean expectedOk) {
        BucketConfig mockConfig = mock(BucketConfig.class);
        when(mockConfig.serviceEnabled(ServiceType.BINARY)).thenReturn(true);

        try {
            handler.checkFeaturesForRequest(request, mockConfig);
            if (!expectedOk) {
                fail();
            }
        } catch (ServiceNotAvailableException e) {
            if (expectedOk) {
                fail();
            }
            assertTrue(e.getMessage().endsWith("service is not enabled or no node in the cluster supports it."));
        }
    }

    @Test(expected = RequestCancelledException.class)
    public void shouldCancelOnRetryPolicyFailFast() throws Exception {
        CoreEnvironment env = mock(CoreEnvironment.class);
        when(env.retryStrategy()).thenReturn(FailFastRetryStrategy.INSTANCE);
        ClusterConfig mockClusterConfig = mock(ClusterConfig.class);
        when(mockClusterConfig.hasBucket(anyString())).thenReturn(Boolean.TRUE);
        Observable<ClusterConfig> mockConfigObservable = Observable.just(mockClusterConfig);

        RequestHandler handler = new DummyLocatorClusterNodeHandler(env, mockConfigObservable);
        Node mockNode = mock(Node.class);
        when(mockNode.connect()).thenReturn(Observable.just(LifecycleState.DISCONNECTED));
        when(mockNode.state()).thenReturn(LifecycleState.DISCONNECTED);
        handler.addNode(mockNode).toBlocking().single();

        RequestEvent mockEvent = mock(RequestEvent.class);
        CouchbaseRequest mockRequest = mock(CouchbaseRequest.class);
        AsyncSubject<CouchbaseResponse> response = AsyncSubject.create();
        when(mockRequest.isActive()).thenReturn(true);
        when(mockEvent.getRequest()).thenReturn(mockRequest);
        when(mockRequest.observable()).thenReturn(response);
        handler.onEvent(mockEvent, 0, true);

        verify(mockNode, times(1)).send(SignalFlush.INSTANCE);
        verify(mockNode, never()).send(mockRequest);
        verify(mockEvent).setRequest(null);

        response.toBlocking().single();
    }

    @Test
    public void shouldNotFailReconfigureOnRemoveAllRaceCondition() throws InterruptedException {
        final ClusterConfig config = mock(DefaultClusterConfig.class);
        when(config.bucketConfigs()).thenReturn(Collections.<String, BucketConfig>emptyMap());
        final Subject<ClusterConfig, ClusterConfig> configObservable = PublishSubject.<ClusterConfig>create();

        //this simulates the race condition in JVMCBC-231, otherwise calls all methods of HashSet
        CopyOnWriteArrayList<Node> nodes = Mockito.spy(new CopyOnWriteArrayList<Node>());
        when(nodes.isEmpty()).thenReturn(false);

        final RequestHandler handler = new RequestHandler(nodes, environment, configObservable, null);

        //mock and add the nodes
        final Node node1 = mock(Node.class);
        when(node1.connect()).thenReturn(Observable.just(LifecycleState.CONNECTED));
        when(node1.disconnect()).thenReturn(Observable.just(LifecycleState.DISCONNECTED));
        final Node node2 = mock(Node.class);
        when(node2.connect()).thenReturn(Observable.just(LifecycleState.CONNECTED));
        when(node2.disconnect()).thenReturn(Observable.just(LifecycleState.DISCONNECTED));
        final Node node3 = mock(Node.class);
        when(node3.connect()).thenReturn(Observable.just(LifecycleState.CONNECTED));
        when(node3.disconnect()).thenReturn(Observable.just(LifecycleState.DISCONNECTED));
        handler.addNode(node1).toBlocking().single();
        handler.addNode(node2).toBlocking().single();
        handler.addNode(node3).toBlocking().single();

        //first reconfiguration with empty node list triggers the removal of all in the nodes set...
        try {
            handler.reconfigure(config).toBlocking().single();
        } catch (NoSuchElementException e) {
            fail("failed to remove all nodes on first pass - " + e);
        }

        //... yet second empty node list configuration will still go into the branch where the nodes set is
        //seen as empty (race condition previously encountered)
        try {
            handler.reconfigure(config).toBlocking().single();
        } catch (NoSuchElementException e) {
            fail("race condition on removing all during reconfigure - " + e);
        }
        //OK - the effect of the race condition should have been avoided there by dong a snapshot
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
            public void locateAndDispatch(CouchbaseRequest request, List<Node> nodes, ClusterConfig config,
                CoreEnvironment env, RingBuffer<ResponseEvent> responseBuffer) {
                for (Node node : nodes) {
                    if (node.state() == LifecycleState.CONNECTED) {
                        node.send(request);
                        return;
                    }
                }
                RetryHelper.retryOrCancel(env, request, responseBuffer);
            }
        }
    }

}
