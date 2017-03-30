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

package com.couchbase.client.core.message.dcp;

import com.couchbase.client.core.annotations.InterfaceAudience;
import com.couchbase.client.core.annotations.InterfaceStability;
import com.couchbase.client.core.message.CouchbaseResponse;
import rx.subjects.Subject;

/**
 * Default implementation of {@link DCPRequest}.
 *
 * @author Sergey Avseyev
 * @since 1.2.6
 */
@InterfaceStability.Experimental
@InterfaceAudience.Private
@Deprecated
public abstract class AbstractDCPMessage extends AbstractDCPRequest implements DCPMessage {
    private final String key;
    private final int totalBodyLength;

    /**
     * Creates a new {@link AbstractDCPMessage}.
     *
     * @param totalBodyLength
     * @param partition
     * @param key
     * @param bucket          the bucket of the document.
     * @param password        the optional password of the bucket.
     */
    public AbstractDCPMessage(int totalBodyLength, short partition, String key, final String bucket, final String password) {
        this(totalBodyLength, partition, key, bucket, bucket, password);
    }

    /**
     * Creates a new {@link AbstractDCPMessage}.
     *
     * @param totalBodyLength
     * @param partition
     * @param key
     * @param bucket          the bucket of the document.
     * @param username        the user authorized for bucket access.
     * @param password        the password of the user.
     */
    public AbstractDCPMessage(int totalBodyLength, short partition, String key, final String bucket, final String username, final String password) {
        this(totalBodyLength, partition, key, bucket, username, password, null);
    }


    public AbstractDCPMessage(int totalBodyLength, short partition, String key, final String bucket, final String username, final String password,
                              final Subject<CouchbaseResponse, CouchbaseResponse> observable) {
        super(bucket, username, password, observable);
        this.partition(partition);
        this.key = key;
        this.totalBodyLength = totalBodyLength;
    }

    @Override
    public int totalBodyLength() {
        return totalBodyLength;
    }

    @Override
    public String key() {
        return key;
    }
}
