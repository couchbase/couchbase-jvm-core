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

import com.couchbase.client.core.message.binary.BinaryRequest;
import com.couchbase.client.core.message.config.ConfigRequest;
import com.couchbase.client.core.service.Service;
import com.couchbase.client.core.service.ServiceType;
import org.junit.Test;
import rx.functions.Action1;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Verifies the functionality of the {@link DefaultServiceRegistry}.
 */
public class DefaultServiceRegistryTest {

    @Test
    public void shouldAddGlobalService() {
        Map<ServiceType, Service> global =  new HashMap<ServiceType, Service>();
        Map<String, Map<ServiceType, Service>> local =  new HashMap<String, Map<ServiceType, Service>>();
        DefaultServiceRegistry registry = new DefaultServiceRegistry(global, local);

        assertEquals(0, global.size());

        Service service = mock(Service.class);
        when(service.type()).thenReturn(ServiceType.DESIGN);
        when(service.mapping()).thenReturn(ServiceType.DESIGN.mapping());
        registry.addService(service, null);

        assertEquals(1, global.size());
        assertEquals(service, global.get(ServiceType.DESIGN));
    }

    @Test
    public void shouldAddLocalService() {
        Map<ServiceType, Service> global =  new HashMap<ServiceType, Service>();
        Map<String, Map<ServiceType, Service>> local =  new HashMap<String, Map<ServiceType, Service>>();
        DefaultServiceRegistry registry = new DefaultServiceRegistry(global, local);

        assertEquals(0, local.size());
        Service service = mock(Service.class);
        when(service.type()).thenReturn(ServiceType.BINARY);
        when(service.mapping()).thenReturn(ServiceType.BINARY.mapping());
        registry.addService(service, "bucket");

        assertEquals(1, local.size());
        assertEquals(1, local.get("bucket").size());
        assertEquals(service, local.get("bucket").get(ServiceType.BINARY));
    }

    @Test
    public void shouldIgnoreDuplicateAddGlobalService() {
        Map<ServiceType, Service> global =  new HashMap<ServiceType, Service>();
        Map<String, Map<ServiceType, Service>> local =  new HashMap<String, Map<ServiceType, Service>>();
        DefaultServiceRegistry registry = new DefaultServiceRegistry(global, local);

        assertEquals(0, global.size());

        Service service = mock(Service.class);
        when(service.type()).thenReturn(ServiceType.DESIGN);
        when(service.mapping()).thenReturn(ServiceType.DESIGN.mapping());
        registry.addService(service, null);

        assertEquals(1, global.size());
        assertEquals(service, global.get(ServiceType.DESIGN));

        Service service2 = mock(Service.class);
        when(service2.type()).thenReturn(ServiceType.DESIGN);
        when(service2.mapping()).thenReturn(ServiceType.DESIGN.mapping());
        registry.addService(service2, null);

        assertEquals(1, global.size());
        assertEquals(service, global.get(ServiceType.DESIGN));
    }

    @Test
    public void shouldIgnoreDuplicateAddLocalService() {
        Map<ServiceType, Service> global =  new HashMap<ServiceType, Service>();
        Map<String, Map<ServiceType, Service>> local =  new HashMap<String, Map<ServiceType, Service>>();
        DefaultServiceRegistry registry = new DefaultServiceRegistry(global, local);

        assertEquals(0, local.size());
        Service service = mock(Service.class);
        when(service.type()).thenReturn(ServiceType.BINARY);
        when(service.mapping()).thenReturn(ServiceType.BINARY.mapping());
        registry.addService(service, "bucket");

        assertEquals(1, local.size());
        assertEquals(1, local.get("bucket").size());
        assertEquals(service, local.get("bucket").get(ServiceType.BINARY));

        Service service2 = mock(Service.class);
        when(service2.type()).thenReturn(ServiceType.BINARY);
        when(service2.mapping()).thenReturn(ServiceType.BINARY.mapping());
        registry.addService(service, "bucket");

        assertEquals(1, local.size());
        assertEquals(1, local.get("bucket").size());
    }

    @Test
    public void shouldRemoveGlobalService() {
        Map<ServiceType, Service> global =  new HashMap<ServiceType, Service>();
        Map<String, Map<ServiceType, Service>> local =  new HashMap<String, Map<ServiceType, Service>>();
        DefaultServiceRegistry registry = new DefaultServiceRegistry(global, local);

        Service service1 = mock(Service.class);
        when(service1.type()).thenReturn(ServiceType.DESIGN);
        when(service1.mapping()).thenReturn(ServiceType.DESIGN.mapping());
        Service service2 = mock(Service.class);
        when(service2.type()).thenReturn(ServiceType.CONFIG);
        when(service2.mapping()).thenReturn(ServiceType.CONFIG.mapping());

        registry.addService(service1, null);
        registry.addService(service2, null);

        assertEquals(2, global.size());

        registry.removeService(service1, null);
        assertEquals(1, global.size());

        registry.removeService(service2, null);
        assertEquals(0, global.size());
    }

    @Test
    public void shouldRemoveLocalService() {
        Map<ServiceType, Service> global =  new HashMap<ServiceType, Service>();
        Map<String, Map<ServiceType, Service>> local =  new HashMap<String, Map<ServiceType, Service>>();
        DefaultServiceRegistry registry = new DefaultServiceRegistry(global, local);

        Service service1 = mock(Service.class);
        when(service1.type()).thenReturn(ServiceType.BINARY);
        when(service1.mapping()).thenReturn(ServiceType.BINARY.mapping());
        Service service2 = mock(Service.class);
        when(service2.type()).thenReturn(ServiceType.STREAM);
        when(service2.mapping()).thenReturn(ServiceType.STREAM.mapping());

        registry.addService(service1, "bucket");
        registry.addService(service2, "bucket");

        assertEquals(1, local.size());
        assertEquals(2, local.get("bucket").size());

        registry.removeService(service1, "bucket");
        assertEquals(1, local.size());
        assertEquals(1, local.get("bucket").size());

        registry.removeService(service2, "bucket");
        assertEquals(0, local.size());
    }

    @Test
    public void shouldLocateGlobalServiceForRequest() {
        DefaultServiceRegistry registry = new DefaultServiceRegistry();

        Service service1 = mock(Service.class);
        when(service1.type()).thenReturn(ServiceType.CONFIG);
        when(service1.mapping()).thenReturn(ServiceType.CONFIG.mapping());
        registry.addService(service1, null);

        ConfigRequest request = mock(ConfigRequest.class);
        when(request.bucket()).thenReturn("bucket");
        Service service = registry.locate(request);
        assertEquals(service1, service);
    }

    @Test
    public void shouldLocateLocalServiceForRequest() {
        DefaultServiceRegistry registry = new DefaultServiceRegistry();

        Service service1 = mock(Service.class);
        when(service1.type()).thenReturn(ServiceType.BINARY);
        when(service1.mapping()).thenReturn(ServiceType.BINARY.mapping());
        registry.addService(service1, "bucket");

        BinaryRequest request = mock(BinaryRequest.class);
        when(request.bucket()).thenReturn("bucket");
        Service service = registry.locate(request);
        assertEquals(service1, service);
    }

    @Test
    public void shouldListAllStoredServices() throws Exception {
        DefaultServiceRegistry registry = new DefaultServiceRegistry();

        final Service localService = mock(Service.class);
        when(localService.type()).thenReturn(ServiceType.BINARY);
        when(localService.mapping()).thenReturn(ServiceType.BINARY.mapping());
        registry.addService(localService, "bucket");

        final Service globalService = mock(Service.class);
        when(globalService.type()).thenReturn(ServiceType.CONFIG);
        when(globalService.mapping()).thenReturn(ServiceType.CONFIG.mapping());
        registry.addService(globalService, null);

        final CountDownLatch latch = new CountDownLatch(2);
        registry.services().subscribe(new Action1<Service>() {
            @Override
            public void call(Service service) {
                assertTrue(service.equals(localService) || service.equals(globalService));
                latch.countDown();
            }
        });

        assertTrue(latch.await(1, TimeUnit.SECONDS));
    }

}
