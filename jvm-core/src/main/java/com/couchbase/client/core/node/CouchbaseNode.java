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

package com.couchbase.client.core.node;

import com.couchbase.client.core.environment.Environment;
import com.couchbase.client.core.message.internal.DisableServiceRequest;
import com.couchbase.client.core.message.internal.DisableServiceResponse;
import com.couchbase.client.core.message.internal.EnableServiceRequest;
import com.couchbase.client.core.message.internal.EnableServiceResponse;
import com.couchbase.client.core.state.AbstractStateMachine;
import com.couchbase.client.core.state.LifecycleState;
import reactor.core.composable.Promise;

import java.net.InetSocketAddress;

public class CouchbaseNode extends AbstractStateMachine<LifecycleState> implements Node {

    private final InetSocketAddress address;
    private final Environment env;

    public CouchbaseNode(Environment env, InetSocketAddress address) {
        super(LifecycleState.DISCONNECTED, env);
        this.env = env;
        this.address = address;
    }

    @Override
    public Promise<Boolean> shutdown() {
        return null;
    }

	@Override
	public Promise<EnableServiceResponse> enableService(EnableServiceRequest request) {
		return null;
	}

	@Override
	public Promise<DisableServiceResponse> disableService(DisableServiceRequest request) {
		return null;
	}
}
