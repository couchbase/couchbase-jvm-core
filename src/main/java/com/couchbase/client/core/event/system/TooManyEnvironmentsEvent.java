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

import java.util.Map;

/**
 * Event published when more than one Environments are created.
 *
 * @author Michael Nitschinger
 * @since 1.3.2
 */
public class TooManyEnvironmentsEvent implements CouchbaseEvent {

    private final int numEnvs;

    public TooManyEnvironmentsEvent(int numEnvs) {
        this.numEnvs = numEnvs;
    }

    @Override
    public EventType type() {
        return EventType.SYSTEM;
    }

    /**
     * The number of open environments at the time of the event.
     */
    public int numEnvs() {
        return numEnvs;
    }

    @Override
    public Map<String, Object> toMap() {
        Map<String, Object> result = Events.identityMap(this);
        result.put("numEnvs", numEnvs);
        return result;
    }

    @Override
    public String toString() {
        return "TooManyEnvironmentsEvent{" +
                "numEnvs=" + numEnvs +
                '}';
    }
}
