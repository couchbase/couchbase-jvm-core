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
package com.couchbase.client.core;

import com.couchbase.client.core.message.CouchbaseMessage;
import com.couchbase.client.core.message.CouchbaseResponse;
import rx.subjects.Subject;

/**
 * A pre allocated event which carries a {@link CouchbaseResponse} and associated information.
 *
 * @author Michael Nitschinger
 * @since 1.0
 */
public class ResponseEvent {

    /**
     * Contains the current response message.
     */
    private CouchbaseMessage message;

    private Subject<CouchbaseResponse, CouchbaseResponse> observable;

    /**
     * Set the new response as a payload for this event.
     *
     * @param message the response to override.
     * @return the {@link ResponseEvent} for method chaining.
     */
    public ResponseEvent setMessage(final CouchbaseMessage message) {
        this.message = message;
        return this;
    }

    /**
     * Get the response from the payload.
     *
     * @return the actual response.
     */
    public CouchbaseMessage getMessage() {
        return message;
    }

    public Subject<CouchbaseResponse, CouchbaseResponse> getObservable() {
        return observable;
    }

    public ResponseEvent setObservable(final Subject<CouchbaseResponse, CouchbaseResponse> observable) {
        this.observable = observable;
        return this;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("ResponseEvent{");
        sb.append("message=").append(message);
        sb.append(", observable=").append(observable);
        sb.append('}');
        return sb.toString();
    }
}
