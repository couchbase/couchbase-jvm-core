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

package com.couchbase.client.core.endpoint.dcp;

import com.couchbase.client.core.ResponseEvent;
import com.couchbase.client.core.endpoint.AbstractEndpoint;
import com.couchbase.client.core.endpoint.kv.KeyValueAuthHandler;
import com.couchbase.client.core.env.CoreEnvironment;
import com.couchbase.client.deps.io.netty.handler.codec.memcache.binary.BinaryMemcacheClientCodec;
import com.couchbase.client.deps.io.netty.handler.codec.memcache.binary.BinaryMemcacheObjectAggregator;
import com.lmax.disruptor.RingBuffer;
import io.netty.channel.ChannelPipeline;

/**
 * This endpoint defines the pipeline for DCP requests and responses.
 *
 * @author Sergey Avseyev
 * @since 1.1.0
 */
public class DCPEndpoint extends AbstractEndpoint {

    @Deprecated
    public DCPEndpoint(String hostname, String bucket, String password, int port,
                       CoreEnvironment environment, RingBuffer<ResponseEvent> responseBuffer) {
        this(hostname, bucket, bucket, password, port, environment, responseBuffer);
    }

    public DCPEndpoint(String hostname, String bucket, String username, String password, int port,
                       CoreEnvironment environment, RingBuffer<ResponseEvent> responseBuffer) {
        super(hostname, bucket, username, password, port, environment, responseBuffer, false, environment.ioPool(), true);
    }

    @Override
    protected void customEndpointHandlers(ChannelPipeline pipeline) {
        pipeline
            .addLast(new BinaryMemcacheClientCodec())
            .addLast(new BinaryMemcacheObjectAggregator(Integer.MAX_VALUE))
            .addLast(new KeyValueAuthHandler(username(), password()))
            .addLast(new DCPConnectionHandler(environment()))
            .addLast(new DCPHandler(this, responseBuffer(), false, true));
    }
}
