/*
 * Copyright (c) 2017 Couchbase, Inc.
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

package com.couchbase.client.core.message.search;

import com.couchbase.client.core.message.AbstractCouchbaseRequest;
import com.couchbase.client.core.message.DiagnosticRequest;
import com.couchbase.client.core.message.PrelocatedRequest;
import com.couchbase.client.core.tracing.ThresholdLogReporter;
import io.opentracing.Span;
import io.opentracing.tag.Tags;

import java.net.InetAddress;
import java.net.SocketAddress;

public class PingRequest
    extends AbstractCouchbaseRequest
    implements SearchRequest, DiagnosticRequest, PrelocatedRequest {

    private final InetAddress sendTo;
    private volatile SocketAddress local;
    private volatile SocketAddress remote;

    public PingRequest(InetAddress sendTo, String bucket, String password) {
        super(bucket, password);
        this.sendTo = sendTo;
    }

    @Override
    protected void afterSpanSet(Span span) {
        span.setTag(Tags.PEER_SERVICE.getKey(), ThresholdLogReporter.SERVICE_FTS);
    }

    @Override
    public String path() {
        return "/api/ping";
    }

    @Override
    public SocketAddress localSocket() {
        return local;
    }

    @Override
    public PingRequest localSocket(SocketAddress socket) {
        this.local = socket;
        return this;
    }

    @Override
    public SocketAddress remoteSocket() {
        return remote;
    }

    @Override
    public PingRequest remoteSocket(SocketAddress socket) {
        this.remote = socket;
        return this;
    }

    @Override
    public InetAddress sendTo() {
        return sendTo;
    }
}
