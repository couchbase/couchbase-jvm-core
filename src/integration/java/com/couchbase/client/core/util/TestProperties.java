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
package com.couchbase.client.core.util;

/**
 * Helper class to centralize test properties that can be modified through system properties.
 *
 * @author Michael Nitschinger
 * @since 1.0
 */
public class TestProperties {

    private static String seedNode;
    private static String bucket;
    private static String password;
    private static String adminUser;
    private static String adminPassword;

    /**
     * Initialize static the properties.
     */
    static {
        seedNode = System.getProperty("seedNode", "127.0.0.1");
        bucket = System.getProperty("bucket", "default");
        password = System.getProperty("password", "");
        adminUser = System.getProperty("adminUser", "Administrator");
        adminPassword = System.getProperty("adminPassword", "password");
    }

    /**
     * The seed node to bootstrap from.
     *
     * @return the seed node.
     */
    public static String seedNode() {
        return seedNode;
    }

    /**
     * The bucket to work against.
     *
     * @return the name of the bucket.
     */
    public static String bucket() {
        return bucket;
    }

    /**
     * The password of the bucket.
     *
     * @return the password of the bucket.
     */
    public static String password() {
        return password;
    }

    /**
     * The admin user of the cluster.
     *
     * @return the admin user of the cluster.
     */
    public static String adminPassword() {
        return adminPassword;
    }

    /**
     * The admin password of the cluster.
     *
     * @return the admin password of the cluster.
     */
    public static String adminUser() {
        return adminUser;
    }
}
