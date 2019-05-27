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

package com.couchbase.client.core.message.kv;

import com.couchbase.client.core.message.DiagnosticRequest;
import com.couchbase.client.core.utils.NetworkAddress;

import java.net.SocketAddress;

public class NoopRequest extends AbstractKeyValueRequest implements DiagnosticRequest {

    private volatile SocketAddress local;
    private volatile SocketAddress remote;
    private final String hostname;

    public NoopRequest(String bucket, String hostname) {
        super("", bucket);
        this.hostname = hostname;
        partition((short) 0);
    }

    public String hostname() {
        return hostname;
    }

    @Override
    public SocketAddress localSocket() {
        return local;
    }

    @Override
    public NoopRequest localSocket(SocketAddress local) {
        this.local = local;
        return this;
    }

    @Override
    public SocketAddress remoteSocket() {
        return remote;
    }

    @Override
    public NoopRequest remoteSocket(SocketAddress remote) {
        this.remote = remote;
        return this;
    }
}
