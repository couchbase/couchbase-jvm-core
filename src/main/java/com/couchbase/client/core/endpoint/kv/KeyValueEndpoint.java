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
package com.couchbase.client.core.endpoint.kv;

import com.couchbase.client.core.ResponseEvent;
import com.couchbase.client.core.endpoint.AbstractEndpoint;
import com.couchbase.client.core.env.CoreEnvironment;
import com.lmax.disruptor.RingBuffer;
import io.netty.channel.ChannelPipeline;
import com.couchbase.client.deps.io.netty.handler.codec.memcache.binary.BinaryMemcacheClientCodec;
import com.couchbase.client.deps.io.netty.handler.codec.memcache.binary.BinaryMemcacheObjectAggregator;
import io.netty.handler.timeout.IdleStateHandler;

import java.util.concurrent.TimeUnit;

/**
 * This endpoint defines the pipeline for binary requests and responses.
 *
 * @author Michael Nitschinger
 * @since 1.0
 */
public class KeyValueEndpoint extends AbstractEndpoint {

    /**
     * Create a new {@link KeyValueEndpoint}.
     *
     * @param hostname the hostname to connect on this endpoint.
     * @param env the couchbase environment.
     */
    @Deprecated
    public KeyValueEndpoint(final String hostname, final String bucket, final String password, int port,
                            final CoreEnvironment env, final RingBuffer<ResponseEvent> responseBuffer) {
        this(hostname, bucket, bucket, password, port, env, responseBuffer);
    }

    /**
     * Create a new {@link KeyValueEndpoint}.
     *
     * @param hostname the hostname to connect on this endpoint.
     * @param env the couchbase environment.
     */
    public KeyValueEndpoint(final String hostname, final String bucket, final String username, final String password, int port,
        final CoreEnvironment env, final RingBuffer<ResponseEvent> responseBuffer) {
        super(hostname, bucket, username, password, port, env, responseBuffer, false,
                env.kvIoPool() == null ? env.ioPool() : env.kvIoPool(), true);
    }


    @Override
    protected void customEndpointHandlers(final ChannelPipeline pipeline) {
        if (environment().keepAliveInterval() > 0) {
            pipeline.addLast(new IdleStateHandler(environment().keepAliveInterval(), 0, 0, TimeUnit.MILLISECONDS));
        }
        pipeline
            .addLast(new BinaryMemcacheClientCodec())
            .addLast(new BinaryMemcacheObjectAggregator(Integer.MAX_VALUE))
            .addLast(new KeyValueFeatureHandler(environment()))
            .addLast(new KeyValueErrorMapHandler())
            .addLast(new KeyValueAuthHandler(username(), password()))
            .addLast(new KeyValueSelectBucketHandler(bucket()))
            .addLast(new KeyValueHandler(this, responseBuffer(), false, true));
    }

}