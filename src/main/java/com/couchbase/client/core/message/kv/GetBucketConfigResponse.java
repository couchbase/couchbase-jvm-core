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
package com.couchbase.client.core.message.kv;

import com.couchbase.client.core.message.ResponseStatus;
import io.netty.buffer.ByteBuf;

import java.net.InetAddress;

/**
 * Represents a response with a bucket configuration.
 *
 * @author Michael Nitschinger
 * @since 1.0
 */
public class GetBucketConfigResponse extends AbstractKeyValueResponse {

    private InetAddress hostname;

    public GetBucketConfigResponse(final ResponseStatus status, final short serverStatusCode, final String bucket,
                                   final ByteBuf content, final InetAddress hostname) {
        super(status, serverStatusCode, bucket, content, null);
        this.hostname = hostname;
    }

    public InetAddress hostname() {
        return hostname;
    }
}
