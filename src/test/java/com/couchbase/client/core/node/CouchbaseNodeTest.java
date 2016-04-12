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
package com.couchbase.client.core.node;

import com.couchbase.client.core.RequestCancelledException;
import com.couchbase.client.core.env.CoreEnvironment;
import com.couchbase.client.core.env.DefaultCoreEnvironment;
import com.couchbase.client.core.message.CouchbaseRequest;
import com.couchbase.client.core.message.CouchbaseResponse;
import com.couchbase.client.core.message.internal.AddServiceRequest;
import com.couchbase.client.core.message.internal.RemoveServiceRequest;
import com.couchbase.client.core.retry.FailFastRetryStrategy;
import com.couchbase.client.core.service.ConfigService;
import com.couchbase.client.core.service.KeyValueService;
import com.couchbase.client.core.service.Service;
import com.couchbase.client.core.service.ServiceType;
import com.couchbase.client.core.state.LifecycleState;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import rx.Observable;
import rx.subjects.AsyncSubject;
import rx.subjects.BehaviorSubject;

import java.net.InetAddress;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Verifies the functionality of a {@link CouchbaseNode}.
 */
public class CouchbaseNodeTest {

    private static final CoreEnvironment environment = DefaultCoreEnvironment.create();


    private static InetAddress host;

    @BeforeClass
    public static void setup() throws Exception {
        host = InetAddress.getLocalHost();
    }

    @Test
    public void shouldBeDisconnectedIfNoServicesRegisteredOnConnect() {
        CouchbaseNode node = new CouchbaseNode(host, environment, null);
        assertEquals(LifecycleState.DISCONNECTED, node.connect().toBlocking().single());
    }

    @Test
    public void shouldBeEqualOnSameInetAddr() throws Exception {
        CouchbaseNode node1 = new CouchbaseNode(InetAddress.getByName("1.2.3.4"), environment, null);
        CouchbaseNode node2 = new CouchbaseNode(InetAddress.getByName("1.2.3.4"), environment, null);
        assertEquals(node1, node2);
        assertEquals(node1.hashCode(), node2.hashCode());
    }

    @Test
    public void shouldNotBeEqualOnDifferentInetAddr() throws Exception {
        CouchbaseNode node1 = new CouchbaseNode(InetAddress.getByName("1.2.3.4"), environment, null);
        CouchbaseNode node2 = new CouchbaseNode(InetAddress.getByName("2.3.4.5"), environment, null);
        assertNotEquals(node1, node2);
    }

    @Test
    @Ignore
    public void shouldBeConnectedIfAllServicesConnectedOnConnect() {
        ServiceRegistry registryMock = mock(ServiceRegistry.class);
        Service service1Mock = mock(Service.class);
        Service service2Mock = mock(Service.class);
        when(registryMock.services()).thenReturn(new Service[] {service1Mock, service2Mock});
        when(service1Mock.connect()).thenReturn(Observable.just(LifecycleState.CONNECTED));
        when(service2Mock.connect()).thenReturn(Observable.just(LifecycleState.CONNECTED));
        CouchbaseNode node = new CouchbaseNode(host, registryMock, environment, null);

        assertEquals(LifecycleState.CONNECTED, node.connect().toBlocking().single());
    }

    @Test
    @Ignore
    public void shouldBeDegradedIfAtLeastOneServiceConnectedOnConnect() {
        ServiceRegistry registryMock = mock(ServiceRegistry.class);
        Service service1Mock = mock(Service.class);
        Service service2Mock = mock(Service.class);
        when(registryMock.services()).thenReturn(new Service[] {service1Mock, service2Mock});
        when(service1Mock.connect()).thenReturn(Observable.just(LifecycleState.CONNECTED));
        when(service2Mock.connect()).thenReturn(Observable.just(LifecycleState.CONNECTING));
        CouchbaseNode node = new CouchbaseNode(host, registryMock, environment, null);

        assertEquals(LifecycleState.DEGRADED, node.connect().toBlocking().single());
    }

    @Test
    @Ignore
    public void shouldBeConnectingIfAtLeastOneServiceConnectingOnConnect() {
        ServiceRegistry registryMock = mock(ServiceRegistry.class);
        Service service1Mock = mock(Service.class);
        Service service2Mock = mock(Service.class);
        when(registryMock.services()).thenReturn(new Service[] {service1Mock, service2Mock});
        when(service1Mock.connect()).thenReturn(Observable.just(LifecycleState.DISCONNECTED));
        when(service2Mock.connect()).thenReturn(Observable.just(LifecycleState.CONNECTING));
        CouchbaseNode node = new CouchbaseNode(host, registryMock, environment, null);

        assertEquals(LifecycleState.CONNECTING, node.connect().toBlocking().single());
    }

    @Test
    public void shouldBeDisconnectedIfNoServiceConnectingOnConnect() {
        ServiceRegistry registryMock = mock(ServiceRegistry.class);
        Service service1Mock = mock(Service.class);
        Service service2Mock = mock(Service.class);
        when(registryMock.services()).thenReturn(new Service[] {service1Mock, service2Mock});
        when(service1Mock.connect()).thenReturn(Observable.just(LifecycleState.DISCONNECTED));
        when(service2Mock.connect()).thenReturn(Observable.just(LifecycleState.DISCONNECTED));
        CouchbaseNode node = new CouchbaseNode(host, registryMock, environment, null);

        assertEquals(LifecycleState.DISCONNECTED, node.connect().toBlocking().single());
    }

    @Test
    @Ignore
    public void shouldBeDisconnectingIfServicesDisconnectingOnDisconnect() {
        ServiceRegistry registryMock = mock(ServiceRegistry.class);
        Service service1Mock = mock(Service.class);
        Service service2Mock = mock(Service.class);
        when(registryMock.services()).thenReturn(new Service[] {service1Mock, service2Mock});
        when(service1Mock.disconnect()).thenReturn(Observable.just(LifecycleState.DISCONNECTING));
        when(service2Mock.disconnect()).thenReturn(Observable.just(LifecycleState.DISCONNECTED));

        BehaviorSubject<LifecycleState> states1 = BehaviorSubject.create();
        BehaviorSubject<LifecycleState> states2 = BehaviorSubject.create();
        when(service1Mock.states()).thenReturn(states1);
        when(service2Mock.states()).thenReturn(states2);
        CouchbaseNode node = new CouchbaseNode(host, registryMock, environment, null);

        Observable<LifecycleState> disconnect = node.disconnect();
        states1.onNext(LifecycleState.DISCONNECTING);
        states2.onNext(LifecycleState.DISCONNECTED);
        assertEquals(LifecycleState.DISCONNECTING, disconnect.toBlocking().single());
    }

    @Test
    public void shouldBeDisconnectedIfServicesDisconnectedOnDisconnect() {
        ServiceRegistry registryMock = mock(ServiceRegistry.class);
        Service service1Mock = mock(Service.class);
        Service service2Mock = mock(Service.class);
        when(registryMock.services()).thenReturn(new Service[] {service1Mock, service2Mock});
        when(service1Mock.disconnect()).thenReturn(Observable.just(LifecycleState.DISCONNECTED));
        when(service2Mock.disconnect()).thenReturn(Observable.just(LifecycleState.DISCONNECTED));
        CouchbaseNode node = new CouchbaseNode(host, registryMock, environment, null);

        assertEquals(LifecycleState.DISCONNECTED, node.disconnect().toBlocking().single());
    }

    @Test
    public void shouldRegisterGlobalService() {
        ServiceRegistry registryMock = mock(ServiceRegistry.class);
        CouchbaseNode node = new CouchbaseNode(host, registryMock, environment, null);
        Service registered = node.addService(new AddServiceRequest(ServiceType.CONFIG, null, null, 0, host))
            .toBlocking().single();

        verify(registryMock).addService(any(ConfigService.class), anyString());
        assertEquals(ServiceType.CONFIG, registered.type());
    }

    @Test
    public void shouldRegisterLocalService() {
        ServiceRegistry registryMock = mock(ServiceRegistry.class);
        CouchbaseNode node = new CouchbaseNode(host, registryMock, environment, null);
        Service registered = node.addService(new AddServiceRequest(ServiceType.BINARY, "bucket", null, 0, host))
            .toBlocking().single();

        verify(registryMock).addService(any(KeyValueService.class), anyString());
        assertEquals(ServiceType.BINARY, registered.type());
    }

    @Test
    public void shouldRemoveGlobalService() {
        ServiceRegistry registryMock = mock(ServiceRegistry.class);
        Service serviceMock = mock(Service.class);
        when(serviceMock.type()).thenReturn(ServiceType.CONFIG);
        when(registryMock.serviceBy(ServiceType.CONFIG, null)).thenReturn(serviceMock);
        CouchbaseNode node = new CouchbaseNode(host, registryMock, environment, null);

        node.removeService(new RemoveServiceRequest(ServiceType.CONFIG, null, host))
            .toBlocking().single();
        verify(registryMock).removeService(any(Service.class), anyString());
    }

    @Test
    public void shouldRemoveLocalService() {
        ServiceRegistry registryMock = mock(ServiceRegistry.class);
        Service serviceMock = mock(Service.class);
        when(serviceMock.type()).thenReturn(ServiceType.BINARY);
        when(registryMock.serviceBy(ServiceType.BINARY, "bucket")).thenReturn(serviceMock);
        when(serviceMock.states()).thenReturn(Observable.<LifecycleState>empty());
        CouchbaseNode node = new CouchbaseNode(host, registryMock, environment, null);

        node.removeService(new RemoveServiceRequest(ServiceType.BINARY, "bucket", host))
            .toBlocking().single();
        verify(registryMock).removeService(any(Service.class), anyString());
    }

    @Test(expected = RequestCancelledException.class)
    public void shouldCancelIfServiceCouldNotBeLocated() {
        ServiceRegistry registryMock = mock(ServiceRegistry.class);
        Service serviceMock = mock(Service.class);
        when(registryMock.serviceBy(ServiceType.BINARY, "bucket")).thenReturn(serviceMock);
        when(serviceMock.states()).thenReturn(Observable.<LifecycleState>empty());
        CoreEnvironment env = mock(CoreEnvironment.class);
        when(env.retryStrategy()).thenReturn(FailFastRetryStrategy.INSTANCE);

        CouchbaseNode node = new CouchbaseNode(host, registryMock, env, null);

        CouchbaseRequest request = mock(CouchbaseRequest.class);
        AsyncSubject<CouchbaseResponse> response = AsyncSubject.create();
        when(request.observable()).thenReturn(response);
        node.send(request);

        response.toBlocking().single();
    }

    @Test
    public void shouldCacheEnabledServices() {
        ServiceRegistry registryMock = mock(ServiceRegistry.class);

        CouchbaseNode node = new CouchbaseNode(host, registryMock, environment, null);

        assertFalse(node.serviceEnabled(ServiceType.BINARY));
        assertFalse(node.serviceEnabled(ServiceType.CONFIG));
        assertFalse(node.serviceEnabled(ServiceType.QUERY));

        node.addService(new AddServiceRequest(ServiceType.BINARY, "bucket", null, 0, host))
                .toBlocking().single();

        assertTrue(node.serviceEnabled(ServiceType.BINARY));
        assertFalse(node.serviceEnabled(ServiceType.CONFIG));
        assertFalse(node.serviceEnabled(ServiceType.QUERY));

        node.addService(new AddServiceRequest(ServiceType.CONFIG, null, null, 0, host))
                .toBlocking().single();

        assertTrue(node.serviceEnabled(ServiceType.BINARY));
        assertTrue(node.serviceEnabled(ServiceType.CONFIG));
        assertFalse(node.serviceEnabled(ServiceType.QUERY));

        // Pretend services are properly stored in the registry before removal
        Service binaryServiceMock = mock(Service.class);
        when(binaryServiceMock.type()).thenReturn(ServiceType.BINARY);
        when(registryMock.serviceBy(ServiceType.BINARY, "bucket")).thenReturn(binaryServiceMock);
        Service configServiceMock = mock(Service.class);
        when(configServiceMock.type()).thenReturn(ServiceType.CONFIG);
        when(registryMock.serviceBy(ServiceType.CONFIG, null)).thenReturn(configServiceMock);

        node.removeService(new RemoveServiceRequest(ServiceType.BINARY, "bucket", host))
                .toBlocking().single();

        assertFalse(node.serviceEnabled(ServiceType.BINARY));
        assertTrue(node.serviceEnabled(ServiceType.CONFIG));
        assertFalse(node.serviceEnabled(ServiceType.QUERY));


        node.removeService(new RemoveServiceRequest(ServiceType.CONFIG, null, host))
                .toBlocking().single();

        assertFalse(node.serviceEnabled(ServiceType.BINARY));
        assertFalse(node.serviceEnabled(ServiceType.CONFIG));
        assertFalse(node.serviceEnabled(ServiceType.QUERY));

    }
}
