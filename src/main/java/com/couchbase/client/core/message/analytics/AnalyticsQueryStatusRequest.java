/*
 * Copyright (c) 2018 Couchbase, Inc.
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

package com.couchbase.client.core.message.analytics;

import com.couchbase.client.core.annotations.InterfaceStability;
import com.couchbase.client.core.message.AbstractCouchbaseRequest;
import com.couchbase.client.core.message.PrelocatedRequest;
import java.net.InetAddress;

/**
 * Deferred analytics query status request
 */
@InterfaceStability.Experimental
public class AnalyticsQueryStatusRequest extends AbstractCouchbaseRequest implements AnalyticsRequest, PrelocatedRequest {

    private final InetAddress target;
    private final String statusPath;

    public AnalyticsQueryStatusRequest(String uri, String bucket, String username, String password) {
        this(uri, bucket, username, password, null);
    }

    public AnalyticsQueryStatusRequest(String uri, String bucket, String username, String password, InetAddress target) {
        super(bucket, username, password);
        String requestIdentifier = uri.substring(uri.lastIndexOf('/') + 1);
        this.statusPath = "/analytics/service/status/" + requestIdentifier;
        this.target = target;
    }

    @Override
    public String path() {
        return statusPath;
    }

    @Override
    public InetAddress sendTo() {
        return target;
    }
}