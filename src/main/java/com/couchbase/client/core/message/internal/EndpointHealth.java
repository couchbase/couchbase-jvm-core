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
package com.couchbase.client.core.message.internal;

import com.couchbase.client.core.annotations.InterfaceAudience;
import com.couchbase.client.core.annotations.InterfaceStability;
import com.couchbase.client.core.endpoint.Endpoint;
import com.couchbase.client.core.service.ServiceType;
import com.couchbase.client.core.state.LifecycleState;
import com.couchbase.client.core.utils.NetworkAddress;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.HashMap;
import java.util.Map;

/**
 * Aggregates the health of one specific {@link Endpoint}.
 *
 * @author Michael Nitschinger
 * @since 1.5.0
 */
@InterfaceAudience.Public
@InterfaceStability.Experimental
public class EndpointHealth {

    private final ServiceType type;
    private final LifecycleState state;
    private final InetSocketAddress local;
    private final InetSocketAddress remote;
    private final long lastActivityUs;
    private final String id;

    public EndpointHealth(ServiceType type, LifecycleState state, SocketAddress localAddr, SocketAddress remoteAddr, long lastActivityUs, String id) {
        this.type = type;
        this.state = state;
        this.id = id;

        if (localAddr == null) {
            this.local = null;
        } else if (!(localAddr instanceof InetSocketAddress)) {
            throw new IllegalArgumentException("Right now only InetSocketAddress is supported");
        } else {
            this.local = (InetSocketAddress) localAddr;
        }

        if (remoteAddr == null) {
            this.remote = null;
        } else if (!(remoteAddr instanceof InetSocketAddress)) {
            throw new IllegalArgumentException("Right now only InetSocketAddress is supported");
        } else {
            this.remote = (InetSocketAddress) remoteAddr;
        }

        this.lastActivityUs = lastActivityUs;
    }

    public ServiceType type() {
        return type;
    }

    public LifecycleState state() {
        return state;
    }

    public InetSocketAddress local() {
        return local;
    }

    public InetSocketAddress remote() {
        return remote;
    }

    public long lastActivity() {
        return lastActivityUs;
    }

    public String id() {
        return id;
    }

    public Map<String, Object> toMap() {
        NetworkAddress ra = remote() == null ? null : NetworkAddress.create(remote().getAddress().getHostAddress());
        NetworkAddress la = local() == null ? null : NetworkAddress.create(local().getAddress().getHostAddress());
        Map<String, Object> map = new HashMap<String, Object>();
        map.put("remote", ra == null ? "" : ra.nameOrAddress() + ":" + remote().getPort());
        map.put("local", la == null ? "" : la.nameOrAddress() + ":" + local().getPort());
        map.put("state", state().toString().toLowerCase());
        map.put("last_activity_us", lastActivity());
        map.put("id", id());
        return map;
    }

    @Override
    public String toString() {
        return "EndpointHealth{" +
            "type=" + type +
            ", state=" + state +
            ", local=" + local +
            ", remote=" + remote +
            ", lastActivity=" + lastActivityUs +
            '}';
    }
}
