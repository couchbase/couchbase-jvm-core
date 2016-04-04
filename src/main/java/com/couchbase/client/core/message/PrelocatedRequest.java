/*
 * Copyright (c) 2016 Couchbase, Inc.
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

package com.couchbase.client.core.message;

import com.couchbase.client.core.config.NodeInfo;

import java.net.InetAddress;

/**
 * A {@link CouchbaseRequest} that can be targeted at a specific node through the corresponding
 * {@link java.net.InetAddress}, shortcutting the dispatch usually performed by a
 * {@link com.couchbase.client.core.node.locate.Locator}..
 *
 * Note that only some types of services perform this user-specified dispatch.
 *
 * @author Simon Baslé
 * @since 1.2.7
 */
public interface PrelocatedRequest extends CouchbaseRequest {

    /**
     * The {@link java.net.InetAddress node} to send this request to, or null to use
     * default {@link com.couchbase.client.core.node.locate.Locator node location process}.
     *
     * @return the address of the target node or null to revert to default dispatching.
     */
    InetAddress sendTo();


}
