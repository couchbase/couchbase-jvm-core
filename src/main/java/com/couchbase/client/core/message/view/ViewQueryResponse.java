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
package com.couchbase.client.core.message.view;

import com.couchbase.client.core.message.AbstractCouchbaseResponse;
import com.couchbase.client.core.message.CouchbaseRequest;
import com.couchbase.client.core.message.ResponseStatus;
import io.netty.buffer.ByteBuf;
import rx.Observable;

public class ViewQueryResponse extends AbstractCouchbaseResponse {

    private final Observable<ByteBuf> rows;
    private final Observable<ByteBuf> info;
    private final Observable<String> error;
    private final int responseCode;
    private final String responsePhrase;

    public ViewQueryResponse(Observable<ByteBuf> rows, Observable<ByteBuf> info, Observable<String> error, int responseCode,
        String responsePhrase, ResponseStatus status, CouchbaseRequest request) {
        super(status, request);
        this.rows = rows;
        this.info = info;
        this.responseCode = responseCode;
        this.responsePhrase = responsePhrase;
        this.error = error;
    }

    public Observable<ByteBuf> rows() {
        return rows;
    }

    public Observable<ByteBuf> info() {
        return info;
    }

    public Observable<String> error() {
        return error;
    }

    public String responsePhrase() {
        return responsePhrase;
    }

    public int responseCode() {
        return responseCode;
    }
}
