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
        super(null, bucket);
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
