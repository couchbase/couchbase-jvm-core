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

package com.couchbase.client.core.message.search;


import com.couchbase.client.core.message.AbstractCouchbaseRequest;
import com.couchbase.client.core.message.BootstrapMessage;

/**
 * Runs query against search index.
 *
 * @author Sergey Avseyev
 * @since 1.2.4
 */
public class SearchQueryRequest extends AbstractCouchbaseRequest implements SearchRequest {

    private final String indexName;
    private final String payload;

    public SearchQueryRequest(String indexName, String payload, String bucket, String username, String password) {
        super(bucket, username, password);
        this.indexName = indexName;
        this.payload = payload;
    }

    @Override
    public String path() {
        return "/api/index/" + indexName + "/query";
    }

    public String payload() {
        return payload;
    }
}
