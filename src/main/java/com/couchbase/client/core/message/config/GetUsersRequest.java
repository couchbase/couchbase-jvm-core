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

import com.couchbase.client.core.message.AbstractCouchbaseRequest;

/**
 * Get a user or list of users.
 *
 * This request has three different ways to operate:
 *
 * 1) neither the domain nor the user is supplied: all users for all domains will be returned
 * 2) just the domain is supplied: all the users will be returned
 * 3) domain and user is supplied: only that user is returned
 *
 * Note that if a userId is supplied, the domain is mandatory! Otherwise you'll get a
 * {@link IllegalArgumentException}.
 *
 * @author Subhashni Balakrishnan
 * @author Michael Nitschinger
 * @since 1.4.4
 */
public class GetUsersRequest extends AbstractCouchbaseRequest implements ConfigRequest {

    private static final String PATH_PREFIX = "/settings/rbac/users";

    private final String userId;
    private final String domain;


    public static GetUsersRequest users(String username, String password) {
        return new GetUsersRequest(username, password, null, null);
    }

    public static GetUsersRequest usersFromDomain(String username, String password, String domain) {
        return new GetUsersRequest(username, password, domain, null);
    }

    public static GetUsersRequest user(String username, String password, String domain, String userId) {
        return new GetUsersRequest(username, password, domain, userId);
    }

    private GetUsersRequest(String username, String password, String domain, String userId) {
        super(username, password);
        if (!empty(userId) && empty(domain)) {
            throw new IllegalArgumentException("If a userId is supplied the domain must be supplied too!");
        }
        this.userId = userId;
        this.domain = domain;
    }

    public String path() {
        String result = PATH_PREFIX;
        if (!empty(domain)) {
            result += "/" + domain;
        }
        if (!empty(userId)) {
            result += "/" + userId;
        }
        return result;
    }

    /**
     * Helper method to check if the given input is null or empty.
     *
     * @param input the input string to check.
     * @return true if empty, false otherwise.
     */
    private static boolean empty(final String input) {
        return input == null || input.isEmpty();
    }
}