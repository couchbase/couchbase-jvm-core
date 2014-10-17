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
package com.couchbase.client.core.message.kv;

import java.net.InetAddress;

import com.couchbase.client.core.message.BootstrapMessage;

/**
 * Request which fetches a bucket configuration through carrier publication.
 *
 * Note that it is not advisable to send such a request from outside of the core. It is used by the configuration
 * handling mechanism to regularly and on bootstrap load new configurations.
 *
 * @author Michael Nitschinger
 * @since 1.0
 */
public class GetBucketConfigRequest extends AbstractKeyValueRequest implements BootstrapMessage {

    /**
     * The hostname from where the config should be loaded.
     */
    private final InetAddress hostname;

    /**
     * Creates a new {@link GetBucketConfigRequest}.
     *
     * @param bucket the name of the bucket.
     * @param hostname the hostname of the node.
     */
    public GetBucketConfigRequest(final String bucket, final InetAddress hostname) {
        super(null, bucket, null);
        this.hostname = hostname;
    }

    /**
     * Returns the hostname of the node from where the config should be loaded.
     *
     * @return the hostname.
     */
    public InetAddress hostname() {
        return hostname;
    }

    @Override
    public short partition() {
        return DEFAULT_PARTITION;
    }

}
