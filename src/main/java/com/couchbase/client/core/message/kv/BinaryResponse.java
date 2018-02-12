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

import com.couchbase.client.core.message.CouchbaseResponse;
import io.netty.buffer.ByteBuf;
import io.netty.util.ReferenceCounted;

/**
 * Marker interface which signals a binary response.
 *
 * @since 1.0.0
 */
public interface BinaryResponse extends CouchbaseResponse, ReferenceCounted {

    /**
     * Contains the content of the response, potentially null or empty.
     *
     * @return the content.
     */
    ByteBuf content();

    /**
     * The name of the bucket where this response is coming from.
     *
     * @return the bucket name.
     */
    String bucket();

    short serverStatusCode();

    /**
     * Returns the reported server duration, if set.
     */
    long serverDuration();

    /**
     * Sets the server duration.
     *
     * @param duration the duration to set.
     */
    BinaryResponse serverDuration(long duration);
}
