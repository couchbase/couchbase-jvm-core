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

import com.couchbase.client.core.message.kv.BinaryRequest;
import com.couchbase.client.core.message.config.ConfigRequest;
import com.couchbase.client.core.service.Service;
import com.couchbase.client.core.service.ServiceType;
import org.junit.Test;
import rx.Observable;
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
        when(service.type()).thenReturn(ServiceType.VIEW);
        when(service.mapping()).thenReturn(ServiceType.VIEW.mapping());
        registry.addService(service, null);

        assertEquals(1, global.size());
        assertEquals(service, global.get(ServiceType.VIEW));
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
        when(service.type()).thenReturn(ServiceType.VIEW);
        when(service.mapping()).thenReturn(ServiceType.VIEW.mapping());
        registry.addService(service, null);

        assertEquals(1, global.size());
        assertEquals(service, global.get(ServiceType.VIEW));

        Service service2 = mock(Service.class);
        when(service2.type()).thenReturn(ServiceType.VIEW);
        when(service2.mapping()).thenReturn(ServiceType.VIEW.mapping());
        registry.addService(service2, null);

        assertEquals(1, global.size());
        assertEquals(service, global.get(ServiceType.VIEW));
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
        when(service1.type()).thenReturn(ServiceType.VIEW);
        when(service1.mapping()).thenReturn(ServiceType.VIEW.mapping());
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

        registry.addService(service1, "bucket");

        assertEquals(1, local.size());
        assertEquals(1, local.get("bucket").size());

        registry.removeService(service1, "bucket");
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
        Observable.from(registry.services()).subscribe(new Action1<Service>() {
            @Override
            public void call(Service service) {
                assertTrue(service.equals(localService) || service.equals(globalService));
                latch.countDown();
            }
        });

        assertTrue(latch.await(1, TimeUnit.SECONDS));
    }

}
