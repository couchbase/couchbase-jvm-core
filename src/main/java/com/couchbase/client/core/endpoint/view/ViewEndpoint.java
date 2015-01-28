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
package com.couchbase.client.core.endpoint.view;

import com.couchbase.client.core.ResponseEvent;
import com.couchbase.client.core.endpoint.AbstractEndpoint;
import com.couchbase.client.core.env.CoreEnvironment;
import com.lmax.disruptor.RingBuffer;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.codec.http.HttpClientCodec;
import io.netty.handler.timeout.IdleStateHandler;

import java.util.concurrent.TimeUnit;

/**
 * This endpoint defines the pipeline for binary requests and responses.
 */
public class ViewEndpoint extends AbstractEndpoint {

    /**
     * Create a new {@link ViewEndpoint}.
     *
     * @param hostname the hostname to connect on this endpoint.
     * @param env the couchbase environment.
     */
    public ViewEndpoint(final String hostname, String bucket, String password, int port, final CoreEnvironment env,
        final RingBuffer<ResponseEvent> responseBuffer) {
        super(hostname, bucket, password, port, env, responseBuffer, false);
    }

    @Override
    protected void customEndpointHandlers(final ChannelPipeline pipeline) {
        if (environment().keepAliveInterval() > 0) {
            pipeline.addLast(new IdleStateHandler(0, 0, environment().keepAliveInterval(), TimeUnit.MILLISECONDS));
        }
        pipeline
            .addLast(new HttpClientCodec())
            .addLast(new ViewHandler(this, responseBuffer(), false));
    }

}