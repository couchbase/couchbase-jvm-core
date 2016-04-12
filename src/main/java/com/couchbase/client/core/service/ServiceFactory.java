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
            case DCP:
                return new DCPService(hostname, bucket, password, port, env, responseBuffer);
            case SEARCH:
                return new SearchService(hostname, bucket, password, port, env, responseBuffer);
            default:
                throw new IllegalArgumentException("Unknown Service Type: " + type);
        }
    }

}
