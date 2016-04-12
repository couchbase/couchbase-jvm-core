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

import com.couchbase.client.core.message.AbstractCouchbaseRequest;

public class UpsertDesignDocumentRequest extends AbstractCouchbaseRequest implements ViewRequest {

    private final String name;
    private final boolean development;
    private final String body;

    public UpsertDesignDocumentRequest(String name, String body, boolean development, String bucket, String password) {
        super(bucket, password);
        this.name = name;
        this.development = development;
        this.body = body;
    }

    public String name() {
        return name;
    }

    public boolean development() {
        return development;
    }

    public String body() {
        return body;
    }
}
