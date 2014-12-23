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

import com.couchbase.client.core.message.CouchbaseRequest;
import com.couchbase.client.core.message.config.ConfigRequest;
import com.couchbase.client.core.message.dcp.DCPRequest;
import com.couchbase.client.core.message.kv.BinaryRequest;
import com.couchbase.client.core.message.query.QueryRequest;
import com.couchbase.client.core.message.view.ViewRequest;
import com.couchbase.client.core.service.BucketServiceMapping;
import com.couchbase.client.core.service.Service;
import com.couchbase.client.core.service.ServiceType;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

/**
 * The default implementation of a {@link ServiceRegistry}.
 */
public class DefaultServiceRegistry implements ServiceRegistry {

    /**
     * Contains services which work across buckets.
     */
    private final Map<ServiceType, Service> globalServices;

    /**
     * Contains bucket-local services.
     */
    private final Map<String, Map<ServiceType, Service>> localServices;

    private AtomicReference<List<Service>> serviceCache;

    /**
     * Create a new {@link DefaultServiceRegistry} with custom containers.
     *
     * This constructor is intended to be used for unit tests only.
     *
     * @param globalServices the global service set.
     * @param localServices the bucket-local services.
     */
    DefaultServiceRegistry(final Map<ServiceType, Service> globalServices,
        final Map<String, Map<ServiceType, Service>> localServices) {
        this.globalServices = globalServices;
        this.localServices = localServices;
        this.serviceCache = new AtomicReference<List<Service>>(new ArrayList<Service>());
    }

    /**
     * Create a new {@link DefaultServiceRegistry}.
     */
    public DefaultServiceRegistry() {
        this(new ConcurrentHashMap<ServiceType, Service>(), new ConcurrentHashMap<String, Map<ServiceType, Service>>());
    }

    @Override
    public Service addService(final Service service, final String bucket) {
        if (service.mapping() == BucketServiceMapping.ONE_BY_ONE) {
            if (!localServices.containsKey(bucket)) {
                localServices.put(bucket, new ConcurrentHashMap<ServiceType, Service>());
            }
            if (!localServices.get(bucket).containsKey(service.type())) {
                localServices.get(bucket).put(service.type(), service);
            }
        } else {
            if (!globalServices.containsKey(service.type())) {
                globalServices.put(service.type(), service);
            }
        }
        recalculateServiceCache();
        return service;
    }

    @Override
    public Service removeService(final Service service, final String bucket) {
        if (service.mapping() == BucketServiceMapping.ONE_BY_ONE) {
            if (localServices.containsKey(bucket) && localServices.get(bucket).containsKey(service.type())) {
                localServices.get(bucket).remove(service.type());
            }
            if (localServices.get(bucket).isEmpty()) {
                localServices.remove(bucket);
            }
        } else {
            if (globalServices.containsKey(service.type())) {
                globalServices.remove(service.type());
            }
        }
        recalculateServiceCache();
        return service;
    }

    @Override
    public Service locate(final CouchbaseRequest request) {
        ServiceType type = serviceTypeFor(request);
        if (type.mapping() == BucketServiceMapping.ONE_BY_ONE) {
            Map<ServiceType, Service> services = localServices.get(request.bucket());
            if (services == null) {
                return null;
            }
            return services.get(type);
        } else {
            return globalServices.get(type);
        }
    }

    @Override
    public List<Service> services() {
        return serviceCache.get();
    }

    private void recalculateServiceCache() {
        List<Service> services = new ArrayList<Service>();
        for (Service service : globalServices.values()) {
            services.add(service);
        }
        for (Map<ServiceType, Service> bucket : localServices.values()) {
            for (Service service : bucket.values()) {
                services.add(service);
            }
        }
        serviceCache.set(services);
    }

    @Override
    public Service serviceBy(final ServiceType type, final String bucket) {
        if (type.mapping() == BucketServiceMapping.ONE_BY_ONE) {
            if (localServices.get(bucket) == null) {
                return null;
            }
            return localServices.get(bucket).get(type);
        } else {
            return globalServices.get(type);
        }
    }

    /**
     * Returns the mapping for a given {@link CouchbaseRequest}.
     *
     * @param request the request to check.
     * @return the mapping for the request.
     */
    private static ServiceType serviceTypeFor(final CouchbaseRequest request) {
        if (request instanceof BinaryRequest) {
            return ServiceType.BINARY;
        } else if (request instanceof ConfigRequest) {
            return ServiceType.CONFIG;
        } else if (request instanceof ViewRequest) {
            return ServiceType.VIEW;
        } else if (request instanceof QueryRequest) {
            return ServiceType.QUERY;
        } else if (request instanceof DCPRequest) {
            return ServiceType.DCP;
        } else {
            throw new IllegalStateException("Unknown Request: " + request);
        }
    }

    @Override
    public String toString() {
        return "DefaultServiceRegistry{"
            + "globalServices=" + globalServices
            + ", localServices=" + localServices
            + ", serviceCache=" + serviceCache
            + '}';
    }
}
