/**
 * Copyright (C) 2014 Couchbase, Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALING
 * IN THE SOFTWARE.
 */

package com.couchbase.client.core.message.common;

import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.List;

/**
 * Connect to a bucket on a Couchbase Cluster.
 *
 * The {@link ConnectRequest} can be sent several times for different buckets, connecting to each of them on the
 * fly. Note that if the cluster is already connected to one bucket, the seed nodes are ignored and only the bucket
 * configuration is being loaded.
 */
public class ConnectRequest implements CommonRequest {

    private static final String DEFAULT_HOST = "127.0.0.1";

    private static final int DEFAULT_PORT = 11210;

    private static final String DEFAULT_BUCKET_NAME = "default";

    private static final String DEFAULT_BUCKET_PASSWORD = "";

    private static final List<InetSocketAddress> DEFAULT_SEED_NODES = Arrays.asList(
        new InetSocketAddress(DEFAULT_HOST, DEFAULT_PORT)
    );

    private final List<InetSocketAddress> seedNodes;
    private final String bucketName;
    private final String bucketPassword;

    public ConnectRequest() {
        this(DEFAULT_SEED_NODES, DEFAULT_BUCKET_NAME, DEFAULT_BUCKET_PASSWORD);
    }

    public ConnectRequest(List<InetSocketAddress> seedNodes, String bucketName, String bucketPassword) {
        this.seedNodes = seedNodes;
        this.bucketName = bucketName;
        this.bucketPassword = bucketPassword;
    }

    public String getBucketPassword() {
        return bucketPassword;
    }

    public String getBucketName() {
        return bucketName;
    }

    public List<InetSocketAddress> getSeedNodes() {
        return seedNodes;
    }

}
