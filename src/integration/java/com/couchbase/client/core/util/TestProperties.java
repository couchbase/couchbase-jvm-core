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

import org.couchbase.mock.Bucket;
import org.couchbase.mock.BucketConfiguration;
import org.couchbase.mock.CouchbaseMock;

import java.util.ArrayList;
import java.util.Properties;

import static org.couchbase.mock.Bucket.BucketType.COUCHBASE;
import static org.couchbase.mock.Bucket.BucketType.MEMCACHED;

/**
 * Helper class to centralize test properties that can be modified through system properties.
 *
 * @author Michael Nitschinger
 * @since 1.0
 */
public class TestProperties {
    private static String seedNode;
    private static String bucket;
    private static String username;
    private static String password;
    private static String adminUser;
    private static String adminPassword;
    private static int mockNodeCount;
    private static int mockReplicaCount;
    private static Bucket.BucketType bucketType;
    private static CouchbaseMock mock;

    private static void createMock() {
        BucketConfiguration bucketConfiguration = new BucketConfiguration();
        bucketConfiguration.numNodes = mockNodeCount;
        bucketConfiguration.numReplicas = mockNodeCount;
        bucketConfiguration.numVBuckets = 1024;
        bucketConfiguration.name = bucket;
        bucketConfiguration.type = bucketType;
        bucketConfiguration.password = password;
        ArrayList<BucketConfiguration> configList = new ArrayList<BucketConfiguration>();
        configList.add(bucketConfiguration);
        try {
            mock = new CouchbaseMock(0, configList);
            mock.start();
            mock.waitForStartup();
        } catch (Exception ex) {
            throw new RuntimeException("Unable to initialize mock" + ex.getMessage(), ex);
        }
    }

    /**
     * Initialize static the properties.
     */
    static {
        Properties properties = new Properties();
        try {
            properties.load(TestProperties.class.getClassLoader().getResourceAsStream("com.couchbase.client.core.integration.properties"));
        } catch (Exception ex) {
            //ignore
        }
        seedNode = properties.getProperty("seedNode", "127.0.0.1");
        bucket = properties.getProperty("bucket", "default");
        username = properties.getProperty("username", "Administrator");
        password = properties.getProperty("password", "password");
        adminUser = properties.getProperty("adminUser", "Administrator");
        adminPassword = properties.getProperty("adminPassword", "password");
        mockNodeCount = Integer.parseInt(properties.getProperty("mockNodeCount", "1"));
        mockReplicaCount = Integer.parseInt(properties.getProperty("mockReplicaCount", "1"));
        bucketType = properties.getProperty("mockBucketType", "couchbase").equalsIgnoreCase("couchbase") ? COUCHBASE : MEMCACHED;
        createMock();
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
     * Username for bucket access applicable for server version 5.0
     *
     * @return the username
     */
    public static String username() {
        return username;
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

    /**
     * Mock node count
     *
     * @return node count configured for mock
     */
    public static CouchbaseMock couchbaseMock() {
        return mock;
    }

    /**
     * Mock node count
     *
     * @return node count configured for mock
     */
    public static int mockNodeCount() {
        return mockNodeCount;
    }

    /**
     * Mock replica count
     *
     * @return replica count configured for mock
     */
    public static int mockReplicaCount() {
        return mockReplicaCount;
    }

    /**
     * Mock bucket type
     *
     * @return bucket type configured for mock
     */
    public static Bucket.BucketType bucketType() {
        return bucketType;
    }
}