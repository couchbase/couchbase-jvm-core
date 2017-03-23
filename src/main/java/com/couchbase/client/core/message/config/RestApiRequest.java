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

package com.couchbase.client.core.message.config;

import com.couchbase.client.core.message.AbstractCouchbaseRequest;
import io.netty.handler.codec.http.HttpMethod;

import java.util.Collections;
import java.util.Map;

/**
 * A generic HTTP request to perform on the cluster REST API port (8091).
 *
 * @author Simon Baslé
 * @since 1.3.2
 */
public class RestApiRequest extends AbstractCouchbaseRequest implements ConfigRequest {

    private final String path;
    private final Map<String, String> queryParameters;
    private final Map<String, Object> headers;
    private final HttpMethod method;
    private final String body;

    /**
     * Create a new API request.
     *
     * @param login the authentication login to use.
     * @param password the authentication password to use.
     * @param method the {@link HttpMethod} for the request.
     * @param path the path of the request.
     * @param queryParameters the query parameters to append to the url (key/values must be urlencoded).
     * @param headers the headers for the request.
     * @param body the body of the request (or empty string if not required).
     */
    public RestApiRequest(String login, String password, HttpMethod method, String path,
      Map<String, String> queryParameters, Map<String, Object> headers, String body) {
        super(null, login, password);
        this.path = path != null ? path : "";
        this.queryParameters = queryParameters != null ? queryParameters : Collections.<String, String>emptyMap();
        this.headers = headers != null ? headers : Collections.<String, Object>emptyMap();
        this.method = method;
        this.body = body != null ? body : "";
    }

    @Override
    public String path() {
        return this.path;
    }

    /**
     * @return the {@link HttpMethod} to use for the request.
     */
    public HttpMethod method() {
        return this.method;
    }

    /**
     * @return the body of the request (empty string if none).
     */
    public String body() {
        return this.body;
    }

    /**
     * @return the request's url query parameters, as a Map.
     */
    public Map<String, String> queryParameters() {
        return this.queryParameters;
    }

    /**
     * @return the request headers, as a Map.
     */
    public Map<String, Object> headers() {
        return this.headers;
    }

    /**
     * @return the full path, with query parameters added at the end.
     */
    public String pathWithParameters() {
        Map<String, String> queryParameters = queryParameters();
        if (queryParameters.isEmpty()) {
            return path();
        }
        StringBuilder pwp = new StringBuilder(path() + "?");
        for (Map.Entry<String, String> queryParam : queryParameters.entrySet()) {
            pwp.append(queryParam.getKey())
                    .append('=')
                    .append(queryParam.getValue())
                    .append('&');
        }
        pwp.deleteCharAt(pwp.length() - 1);
        return pwp.toString();
    }
}
