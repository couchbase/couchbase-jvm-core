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

package com.couchbase.client.core.message.config;

import com.couchbase.client.core.message.AbstractCouchbaseResponse;
import com.couchbase.client.core.message.ResponseStatus;

/**
 * @author Subhashni Balakrishnan
 * @since 1.4.4
 */
public class UpsertUserResponse extends AbstractCouchbaseResponse {
    private final String msg;

    public UpsertUserResponse(String msg, ResponseStatus status) {
        super(status, null);
        this.msg = msg;
    }

    public String message() {
        return msg;
    }

}
