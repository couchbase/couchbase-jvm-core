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
import com.couchbase.client.core.service.ServiceType;
import com.couchbase.client.core.utils.NetworkAddress;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.HashMap;
import java.util.Map;

/**
 * Represents health for a specific service on ping.
 *
 * @author Michael Nitschinger
 * @since 1.5.4
 */
@InterfaceAudience.Public
@InterfaceStability.Experimental
public class PingServiceHealth {

    private final ServiceType serviceType;
    private final PingState pingState;
    private final String id;
    private final long latency;
    private final InetSocketAddress local;
    private final InetSocketAddress remote;
    private final String scope;


    public PingServiceHealth(
        ServiceType serviceType, PingState pingState, String id,
        long latency, SocketAddress localAddr, SocketAddress remoteAddr, String scope) {
        this.serviceType = serviceType;
        this.pingState = pingState;
        this.id = id;
        this.latency = latency;
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
        this.scope = scope;
    }

    public ServiceType type() {
        return serviceType;
    }

    public String scope() {
        return scope;
    }

    public PingState state() {
        return pingState;
    }

    public String id() {
        return id;
    }

    public long latency() {
        return latency;
    }

    public InetSocketAddress local() {
        return local;
    }

    public InetSocketAddress remote() {
        return remote;
    }

    public Map<String, Object> toMap() {
        NetworkAddress ra = remote() == null ? null : NetworkAddress.create(remote().getAddress().getHostAddress());
        NetworkAddress la = local() == null ? null : NetworkAddress.create(local().getAddress().getHostAddress());
        Map<String, Object> map = new HashMap<String, Object>();
        map.put("remote", ra == null ? "" : ra.nameOrAddress() + ":" + remote().getPort());
        map.put("local", la == null ? "" : la.nameOrAddress() + ":" + local().getPort());
        map.put("state", pingState.asJson());
        map.put("latency_us", latency);
        if (scope != null && !scope.isEmpty()) {
            map.put("scope", scope);
        }
        map.put("id", id());
        return map;
    }

    @Override
    public String toString() {
        return "PingServiceHealth{" +
            "serviceType=" + serviceType +
            ", pingState=" + pingState +
            ", id='" + id + '\'' +
            ", latency=" + latency +
            ", local='" + local + '\'' +
            ", remote='" + remote + '\'' +
            ", scope='" + scope + '\'' +
            '}';
    }

    /**
     *
     */
    public enum PingState {
        /**
         * The ping went fine.
         */
        OK("ok"),
        /**
         * The ping timed out.
         */
        TIMEOUT("timeout"),
        /**
         * The ping didn't time out but a different error happened.
         */
        ERROR("error");

        private final String val;

        PingState(String val) {
            this.val = val;
        }

        /**
         * Returns the json representation for this enum.
         */
        public String asJson() {
            return val;
        }

    }
}