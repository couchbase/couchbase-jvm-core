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
package com.couchbase.client.core.message.internal;

import com.couchbase.client.core.message.AbstractCouchbaseRequest;
import com.couchbase.client.core.service.ServiceType;

import java.net.InetAddress;


public class AddServiceRequest extends AbstractCouchbaseRequest implements InternalRequest {

    private final ServiceType type;
    private final InetAddress hostname;
    private final int port;

    public AddServiceRequest(ServiceType type, String bucket, String password, int port, InetAddress hostname) {
        super(bucket, password);
        this.type = type;
        this.hostname = hostname;
        this.port = port;
    }

    public ServiceType type() {
        return type;
    }

    public InetAddress hostname() {
        return hostname;
    }

    public int port() {
        return port;
    }

    @Override
    public String toString() {
        return "AddServiceRequest{" + "type=" + type + ", hostname='" + hostname + '\'' + ", port=" + port + '}';
    }
}
