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
import com.couchbase.client.core.message.AbstractCouchbaseRequest;
import com.couchbase.client.core.message.CouchbaseResponse;
import rx.subjects.Subject;

/**
 * Default implementation of {@link DCPRequest}.
 *
 * @author Sergey Avseyev
 * @since 1.1.0
 */
@InterfaceStability.Experimental
@InterfaceAudience.Private
public abstract class AbstractDCPRequest extends AbstractCouchbaseRequest implements DCPRequest {
    protected static short DEFAULT_PARTITION = 0;

    /**
     * The partition (vBucket) of the document.
     */
    private short partition = DEFAULT_PARTITION;

    /**
     * Creates a new {@link AbstractDCPRequest}.
     *
     * @param bucket   the bucket of the document.
     * @param password the optional password of the bucket.
     */
    public AbstractDCPRequest(String bucket, String password) {
        this(bucket, bucket, password);
    }

    /**
     * Creates a new {@link AbstractDCPRequest}.
     *
     * @param bucket   the bucket of the document.
     * @param username the user authorized for bucket access.
     * @param password the optional password of the user.
     */
    public AbstractDCPRequest(String bucket, String username, String password) {
        super(bucket, username, password);
    }

    public AbstractDCPRequest(String bucket, String password, Subject<CouchbaseResponse,
            CouchbaseResponse> observable) {
        this(bucket, bucket, password, observable);
    }

    public AbstractDCPRequest(String bucket, String username, String password, Subject<CouchbaseResponse,
            CouchbaseResponse> observable) {
        super(bucket, username, password, observable);
    }

    @Override
    public short partition() {
        if (partition == -1) {
            throw new IllegalStateException("Partition requested but not set beforehand");
        }
        return partition;
    }

    @Override
    public DCPRequest partition(short partition) {
        if (partition < 0) {
            throw new IllegalArgumentException("Partition must be larger than or equal to zero");
        }
        this.partition = partition;
        return this;
    }

    public String toString() {
        final StringBuilder sb = new StringBuilder(this.getClass().getSimpleName() + "{");
        sb.append("observable=").append(observable());
        sb.append(", bucket='").append(bucket()).append('\'');
        sb.append(", partition=").append(partition());
        return sb.append('}').toString();
    }
}
