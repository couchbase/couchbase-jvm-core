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

import org.junit.Test;

import static com.couchbase.client.core.message.config.GetUsersRequest.user;
import static com.couchbase.client.core.message.config.GetUsersRequest.users;
import static com.couchbase.client.core.message.config.GetUsersRequest.usersFromDomain;
import static org.junit.Assert.assertEquals;

/**
 * Verifies the functionality of the {@link GetUsersRequest} init.
 *
 * @author Michael Nitschinger
 * @since 1.5.0
 */
public class GetUsersRequestTest {

    @Test
    public void shouldWorkWithNoUserAndNoDomain() {
        assertEquals("/settings/rbac/users", users("admin", "pass").path());
    }

    @Test
    public void shouldWorkWithDomainOnly() {
        assertEquals("/settings/rbac/users/domain", usersFromDomain("admin", "pass", "domain").path());
    }

    @Test
    public void shouldWorkWithDomainAndUser() {
        assertEquals("/settings/rbac/users/domain/user", user("admin", "pass", "domain", "user").path());
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldFailWithUserAndNullDomain() {
        assertEquals("/settings/rbac/users/domain/user", user("admin", "pass", null, "user").path());
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldFailWithUserAndEmptyDomain() {
        assertEquals("/settings/rbac/users/domain/user", user("admin", "pass", "", "user").path());
    }
}