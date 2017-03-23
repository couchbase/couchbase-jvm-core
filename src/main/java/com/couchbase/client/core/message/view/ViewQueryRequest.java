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

public class ViewQueryRequest extends AbstractCouchbaseRequest implements ViewRequest {

    private final String design;
    private final String view;
    private final String query;
    private final String keysJson;
    private final boolean spatial;
    private final boolean development;

    /**
     * @param design the name of the design document.
     * @param view the name of the view.
     * @param development true if development mode.
     * @param query the query parameters, except "keys".
     * @param keys the "keys" parameter as a JSON array, null if not needed.
     * @param bucket the bucket name.
     * @param password the bucket password.
     */
    public ViewQueryRequest(String design, String view, boolean development, String query, String keys, String bucket,
                            String password) {
        this(design, view, development, false, query, keys, bucket, bucket, password);
    }

    /**
     * @param design the name of the design document.
     * @param view the name of the view.
     * @param development true if development mode.
     * @param query the query parameters, except "keys".
     * @param keys the "keys" parameter as a JSON array, null if not needed.
     * @param bucket the bucket name.
     * @param username the user authorized for bucket access.
     * @param password the user password.
     */
    public ViewQueryRequest(String design, String view, boolean development, String query, String keys, String bucket,
                            String username, String password) {
        this(design, view, development, false, query, keys, bucket, username, password);
    }


    /**
     * @param design the name of the design document.
     * @param view the name of the view.
     * @param development true if development mode.
     * @param spatial true if spatial query.
     * @param query the query parameters, except "keys".
     * @param keys the "keys" parameter as a JSON array, null if not needed.
     * @param bucket the bucket name.
     * @param password the bucket password.
     */
    public ViewQueryRequest(String design, String view, boolean development, boolean spatial, String query, String keys,
                            String bucket, String password) {
        this(design, view, development, spatial, query, keys, bucket, bucket, password);
    }

    /**
     * @param design the name of the design document.
     * @param view the name of the view.
     * @param development true if development mode.
     * @param spatial true if spatial query.
     * @param query the query parameters, except "keys".
     * @param keys the "keys" parameter as a JSON array, null if not needed.
     * @param bucket the bucket name.
     * @param username the user authorized for bucket access.
     * @param password the user password.
     */
    public ViewQueryRequest(String design, String view, boolean development, boolean spatial, String query, String keys,
                            String bucket, String username, String password) {
        super(bucket, username, password);
        this.design = design;
        this.view = view;
        this.query = query;
        this.keysJson = keys;
        this.development = development;
        this.spatial = spatial;
    }

    public String design() {
        return design;
    }

    public String view() {
        return view;
    }

    public String query() {
        return query;
    }

    /***
     * @return the keys parameter as a JSON array String.
     */
    public String keys() {
        return keysJson;
    }

    public boolean development() {
        return development;
    }

    public boolean spatial() {
        return spatial;
    }
}
