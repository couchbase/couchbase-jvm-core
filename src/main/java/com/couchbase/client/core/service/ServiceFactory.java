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
package com.couchbase.client.core.service;

import com.couchbase.client.core.ResponseEvent;
import com.couchbase.client.core.env.CoreEnvironment;
import com.lmax.disruptor.RingBuffer;

public class ServiceFactory {

    private ServiceFactory() {
    }

    public static Service create(String hostname, String bucket, String password, int port, CoreEnvironment env,
        ServiceType type, final RingBuffer<ResponseEvent> responseBuffer) {
        switch (type) {
            case BINARY:
                return new KeyValueService(hostname, bucket, password, port, env, responseBuffer);
            case VIEW:
                return new ViewService(hostname, bucket, password, port, env, responseBuffer);
            case CONFIG:
                return new ConfigService(hostname, bucket, password, port, env, responseBuffer);
            case QUERY:
                return new QueryService(hostname, bucket, password, port, env, responseBuffer);
            default:
                throw new IllegalArgumentException("Unknown Service Type: " + type);
        }
    }

}
