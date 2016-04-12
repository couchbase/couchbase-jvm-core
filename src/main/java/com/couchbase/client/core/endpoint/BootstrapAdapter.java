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

package com.couchbase.client.core.endpoint;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelFuture;

/**
 * A wrapper for the IO {@link Bootstrap} class.
 *
 * This adapter is needed in order to properly mock the underlying {@link Bootstrap} class, since it is a final class
 * and can't be tested properly otherwise.
 */
public class BootstrapAdapter {

    /**
     * The underlying {@link Bootstrap}.
     */
    private final Bootstrap bootstrap;

    /**
     * Create a new {@link BootstrapAdapter}.
     *
     * @param bootstrap the wrapped bootstrap.
     */
    public BootstrapAdapter(final Bootstrap bootstrap) {
        this.bootstrap = bootstrap;
    }

    /**
     * Connect the underlying {@link Bootstrap} and return a {@link ChannelFuture}.
     *
     * @return the future containing the channel and connect status.
     */
    public ChannelFuture connect() {
        return bootstrap.connect();
    }

}