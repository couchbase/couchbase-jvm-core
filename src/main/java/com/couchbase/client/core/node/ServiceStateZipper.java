/*
 * Copyright (c) 2015 Couchbase, Inc.
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

import com.couchbase.client.core.service.Service;
import com.couchbase.client.core.state.AbstractStateZipper;
import com.couchbase.client.core.state.LifecycleState;

import java.util.Collection;

/**
 * Calculates a merged state for all registered services.
 *
 * @author Michael Nitschinger
 * @since 1.1.1
 */
public class ServiceStateZipper extends AbstractStateZipper<Service, LifecycleState>  {

    private final LifecycleState initialState;

    public ServiceStateZipper(LifecycleState initial) {
        super(initial);
        this.initialState = initial;
    }

    @Override
    protected LifecycleState zipWith(Collection<LifecycleState> states) {
        if (states.isEmpty()) {
            return LifecycleState.DISCONNECTED;
        }

        int connected = 0;
        int connecting = 0;
        int disconnecting = 0;
        int idle = 0;
        for (LifecycleState serviceState : states) {
            switch (serviceState) {
                case CONNECTED:
                    connected++;
                    break;
                case CONNECTING:
                    connecting++;
                    break;
                case DISCONNECTING:
                    disconnecting++;
                    break;
                case IDLE:
                    idle++;
                    break;
            }
        }
        if (states.size() == idle) {
            return LifecycleState.IDLE;
        } else if (states.size() == (connected + idle)) {
            return LifecycleState.CONNECTED;
        } else if (connected > 0) {
            return LifecycleState.DEGRADED;
        } else if (connecting > 0) {
            return LifecycleState.CONNECTING;
        } else if (disconnecting > 0) {
            return LifecycleState.DISCONNECTING;
        } else {
            return LifecycleState.DISCONNECTED;
        }
    }

}
