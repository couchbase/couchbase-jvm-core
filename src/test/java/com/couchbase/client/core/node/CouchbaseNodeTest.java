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
package com.couchbase.client.core.node;

import com.couchbase.client.core.env.CouchbaseEnvironment;
import com.couchbase.client.core.env.Environment;
import com.couchbase.client.core.message.internal.AddServiceRequest;
import com.couchbase.client.core.message.internal.RemoveServiceRequest;
import com.couchbase.client.core.service.BinaryService;
import com.couchbase.client.core.service.ConfigService;
import com.couchbase.client.core.service.Service;
import com.couchbase.client.core.service.ServiceType;
import com.couchbase.client.core.state.LifecycleState;
import org.junit.Test;
import rx.Observable;

import java.util.Arrays;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Verifies the functionality of a {@link CouchbaseNode}.
 */
public class CouchbaseNodeTest {

    private static final Environment environment = new CouchbaseEnvironment();

    @Test
    public void shouldReturnConfiguredHostname() {
        CouchbaseNode node = new CouchbaseNode("127.0.0.1", environment, null);
        assertEquals("127.0.0.1", node.hostname());
    }

    @Test
    public void shouldBeDisconnectedIfNoServicesRegisteredOnConnect() {
        CouchbaseNode node = new CouchbaseNode("127.0.0.1", environment, null);
        assertEquals(LifecycleState.DISCONNECTED, node.connect().toBlockingObservable().single());
    }

    @Test
    public void shouldBeConnectedIfAllServicesConnectedOnConnect() {
        ServiceRegistry registryMock = mock(ServiceRegistry.class);
        Service service1Mock = mock(Service.class);
        Service service2Mock = mock(Service.class);
        when(registryMock.services()).thenReturn(Arrays.asList(service1Mock, service2Mock));
        when(service1Mock.connect()).thenReturn(Observable.from(LifecycleState.CONNECTED));
        when(service2Mock.connect()).thenReturn(Observable.from(LifecycleState.CONNECTED));
        CouchbaseNode node = new CouchbaseNode("127.0.0.1", registryMock, environment, null);

        assertEquals(LifecycleState.CONNECTED, node.connect().toBlockingObservable().single());
    }

    @Test
    public void shouldBeDegradedIfAtLeastOneServiceConnectedOnConnect() {
        ServiceRegistry registryMock = mock(ServiceRegistry.class);
        Service service1Mock = mock(Service.class);
        Service service2Mock = mock(Service.class);
        when(registryMock.services()).thenReturn(Arrays.asList(service1Mock, service2Mock));
        when(service1Mock.connect()).thenReturn(Observable.from(LifecycleState.CONNECTED));
        when(service2Mock.connect()).thenReturn(Observable.from(LifecycleState.CONNECTING));
        CouchbaseNode node = new CouchbaseNode("127.0.0.1", registryMock, environment, null);

        assertEquals(LifecycleState.DEGRADED, node.connect().toBlockingObservable().single());
    }

    @Test
    public void shouldBeConnectingIfAtLeastOneServiceConnectingOnConnect() {
        ServiceRegistry registryMock = mock(ServiceRegistry.class);
        Service service1Mock = mock(Service.class);
        Service service2Mock = mock(Service.class);
        when(registryMock.services()).thenReturn(Arrays.asList(service1Mock, service2Mock));
        when(service1Mock.connect()).thenReturn(Observable.from(LifecycleState.DISCONNECTED));
        when(service2Mock.connect()).thenReturn(Observable.from(LifecycleState.CONNECTING));
        CouchbaseNode node = new CouchbaseNode("127.0.0.1", registryMock, environment, null);

        assertEquals(LifecycleState.CONNECTING, node.connect().toBlockingObservable().single());
    }

    @Test
    public void shouldBeDisconnectedIfNoServiceConnectingOnConnect() {
        ServiceRegistry registryMock = mock(ServiceRegistry.class);
        Service service1Mock = mock(Service.class);
        Service service2Mock = mock(Service.class);
        when(registryMock.services()).thenReturn(Arrays.asList(service1Mock, service2Mock));
        when(service1Mock.connect()).thenReturn(Observable.from(LifecycleState.DISCONNECTED));
        when(service2Mock.connect()).thenReturn(Observable.from(LifecycleState.DISCONNECTED));
        CouchbaseNode node = new CouchbaseNode("127.0.0.1", registryMock, environment, null);

        assertEquals(LifecycleState.DISCONNECTED, node.connect().toBlockingObservable().single());
    }

    @Test
    public void shouldBeDisconnectingIfServicesDisconnectingOnDisconnect() {
        ServiceRegistry registryMock = mock(ServiceRegistry.class);
        Service service1Mock = mock(Service.class);
        Service service2Mock = mock(Service.class);
        when(registryMock.services()).thenReturn(Arrays.asList(service1Mock, service2Mock));
        when(service1Mock.disconnect()).thenReturn(Observable.from(LifecycleState.DISCONNECTING));
        when(service2Mock.disconnect()).thenReturn(Observable.from(LifecycleState.DISCONNECTED));
        CouchbaseNode node = new CouchbaseNode("127.0.0.1", registryMock, environment, null);

        assertEquals(LifecycleState.DISCONNECTING, node.disconnect().toBlockingObservable().single());
    }

    @Test
    public void shouldBeDisconnectedIfServicesDisconnectedOnDisconnect() {
        ServiceRegistry registryMock = mock(ServiceRegistry.class);
        Service service1Mock = mock(Service.class);
        Service service2Mock = mock(Service.class);
        when(registryMock.services()).thenReturn(Arrays.asList(service1Mock, service2Mock));
        when(service1Mock.disconnect()).thenReturn(Observable.from(LifecycleState.DISCONNECTED));
        when(service2Mock.disconnect()).thenReturn(Observable.from(LifecycleState.DISCONNECTED));
        CouchbaseNode node = new CouchbaseNode("127.0.0.1", registryMock, environment, null);

        assertEquals(LifecycleState.DISCONNECTED, node.disconnect().toBlockingObservable().single());
    }

    @Test
    public void shouldRegisterGlobalService() {
        ServiceRegistry registryMock = mock(ServiceRegistry.class);
        CouchbaseNode node = new CouchbaseNode("127.0.0.1", registryMock, environment, null);
        Service registered = node.addService(new AddServiceRequest(ServiceType.CONFIG, null, null, 0, "127.0.0.1"))
            .toBlockingObservable().single();

        verify(registryMock).addService(any(ConfigService.class), anyString());
        assertEquals(ServiceType.CONFIG, registered.type());
    }

    @Test
    public void shouldRegisterLocalService() {
        ServiceRegistry registryMock = mock(ServiceRegistry.class);
        CouchbaseNode node = new CouchbaseNode("127.0.0.1", registryMock, environment, null);
        Service registered = node.addService(new AddServiceRequest(ServiceType.BINARY, "bucket", null, 0, "127.0.0.1"))
            .toBlockingObservable().single();

        verify(registryMock).addService(any(BinaryService.class), anyString());
        assertEquals(ServiceType.BINARY, registered.type());
    }

    @Test
    public void shouldRemoveGlobalService() {
        ServiceRegistry registryMock = mock(ServiceRegistry.class);
        Service serviceMock = mock(Service.class);
        when(registryMock.serviceBy(ServiceType.CONFIG, null)).thenReturn(serviceMock);
        CouchbaseNode node = new CouchbaseNode("127.0.0.1", registryMock, environment, null);

        node.removeService(new RemoveServiceRequest(ServiceType.CONFIG, null, "127.0.0.1"))
            .toBlockingObservable().single();
        verify(registryMock).removeService(any(Service.class), anyString());
    }

    @Test
    public void shouldRemoveLocalService() {
        ServiceRegistry registryMock = mock(ServiceRegistry.class);
        Service serviceMock = mock(Service.class);
        when(registryMock.serviceBy(ServiceType.BINARY, "bucket")).thenReturn(serviceMock);
        CouchbaseNode node = new CouchbaseNode("127.0.0.1", registryMock, environment, null);

        node.removeService(new RemoveServiceRequest(ServiceType.CONFIG, "bucket", "127.0.0.1"))
            .toBlockingObservable().single();
        verify(registryMock).removeService(any(Service.class), anyString());
    }
}
