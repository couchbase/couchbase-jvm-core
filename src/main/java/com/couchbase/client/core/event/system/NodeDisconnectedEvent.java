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
package com.couchbase.client.core.event.system;

import com.couchbase.client.core.event.CouchbaseEvent;
import com.couchbase.client.core.event.EventType;
import com.couchbase.client.core.utils.Events;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Map;

/**
 * Event published when a node is disconnected.
 *
 * @author Michael Nitschinger
 * @since 1.1.0
 */
public class NodeDisconnectedEvent implements CouchbaseEvent {

    private final String host;

    public NodeDisconnectedEvent(String host) {
        this.host = host;
    }

    @Override
    public EventType type() {
        return EventType.SYSTEM;
    }

    /**
     * The host address of the disconnected node.
     *
     * @return the inet address of the disconnected node
     */
    public InetAddress host() {
        try {
            return InetAddress.getByName(host);
        } catch (UnknownHostException e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("NodeDisconnectedEvent{");
        sb.append("host=").append(host);
        sb.append('}');
        return sb.toString();
    }

    @Override
    public Map<String, Object> toMap() {
        Map<String, Object> result = Events.identityMap(this);
        result.put("host", host().toString());
        return result;
    }
}
