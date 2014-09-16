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
package com.couchbase.client.core.endpoint.kv;

import com.couchbase.client.core.ResponseEvent;
import com.couchbase.client.core.endpoint.AbstractEndpoint;
import com.couchbase.client.core.env.CoreEnvironment;
import com.lmax.disruptor.RingBuffer;
import io.netty.channel.ChannelPipeline;
import com.couchbase.client.deps.io.netty.handler.codec.memcache.binary.BinaryMemcacheClientCodec;
import com.couchbase.client.deps.io.netty.handler.codec.memcache.binary.BinaryMemcacheObjectAggregator;

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
    public KeyValueEndpoint(final String hostname, final String bucket, final String password, int port, final CoreEnvironment env,
                            final RingBuffer<ResponseEvent> responseBuffer) {
        super(hostname, bucket, password, port, env, responseBuffer);
    }


    @Override
    protected void customEndpointHandlers(final ChannelPipeline pipeline) {
        pipeline
            .addLast(new BinaryMemcacheClientCodec())
            .addLast(new BinaryMemcacheObjectAggregator(Integer.MAX_VALUE))
            .addLast(new KeyValueAuthHandler(bucket(), password()))
            .addLast(new KeyValueHandler(this, responseBuffer()));
    }

}