/*
 * Copyright (c) 2015 Couchbase, Inc.
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

package com.couchbase.client.core.endpoint.dcp;

import com.couchbase.client.core.env.CoreEnvironment;
import com.couchbase.client.core.message.dcp.DCPRequest;
import com.couchbase.client.core.utils.UnicastAutoReleaseSubject;
import rx.subjects.SerializedSubject;
import rx.subjects.Subject;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * @author Sergey Avseyev
 */
public class DCPConnection {
    private static final Map<Integer, String> streams = new ConcurrentHashMap<Integer, String>();
    /**
     * Counter for stream identifiers.
     */
    private static volatile int nextStreamId = 0;
    private final String name;
    private final SerializedSubject<DCPRequest, DCPRequest> subject;
    private final String bucket;

    public DCPConnection(final CoreEnvironment env, final String name, final String bucket) {
        this.name = name;
        this.bucket = bucket;
        subject = UnicastAutoReleaseSubject.<DCPRequest>create(env.autoreleaseAfter(), TimeUnit.MILLISECONDS, env.scheduler())
                .toSerialized();
    }

    public static int addStream(final String connectionName) {
        int newStreamId = nextStreamId++;
        streams.put(newStreamId, connectionName);
        return newStreamId;
    }

    public static String connectionName(final int streamId) {
        return streams.get(streamId);
    }

    public String name() {
        return name;
    }

    public String bucket() {
        return bucket;
    }

    public Subject<DCPRequest, DCPRequest> subject() {
        return subject;
    }
}
