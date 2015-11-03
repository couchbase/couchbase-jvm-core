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

package com.couchbase.client.core.message.dcp;

/**
 * @author Sergey Avseyev
 */
public enum ControlParameter {
    /**
     * Used to enable to tell the Producer that the Consumer supports detecting
     * dead connections through the use of a noop. The value for this message should
     * be set to either "true" or "false". See the page on dead connections for more
     * details. This parameter is available starting in Couchbase 3.0.
     */
    ENABLE_NOOP("enable_noop"),
    /**
     * Used to tell the Producer the size of the Consumer side buffer in bytes which
     * the Consumer is using for flow control. The value of this parameter should be
     * an integer in string form between 1 and 2^32. See the page on flow control for
     * more details. This parameter is available starting in Couchbase 3.0.
     */
    CONNECTION_BUFFER_SIZE("connection_buffer_size"),
    /**
     * Sets the noop interval on the Producer. Values for this parameter should be
     * an integer in string form between 20 and 10800. This allows the noop interval
     * to be set between 20 seconds and 3 hours. This parameter should always be set
     * when enabling noops to prevent the Consumer and Producer having a different
     * noop interval. This parameter is available starting in Couchbase 3.0.1.
     */
    SET_NOOP_INTERVAL("set_noop_interval"),
    /**
     * Sets the priority that the connection should have when sending data.
     * The priority may be set to "high", "medium", or "low". High priority connections
     * will send messages at a higher rate than medium and low priority connections.
     * This parameter is availale starting in Couchbase 4.0.
     */
    SET_PRIORITY("set_priority"),
    /**
     * Enables sending extended meta data. This meta data is mainly used for internal
     * server processes and will vary between different releases of Couchbase. See
     * the documentation on extended meta data for more information on what will be
     * sent. Each version of Couchbase will support a specific version of extended
     * meta data. This parameter is available starting in Couchbase 4.0.
     */
    ENABLE_EXT_METADATA("enable_ext_metadata"),
    /**
     * Compresses values using snappy compression before sending them. This parameter
     * is available starting in Couchbase 4.5.
     */
    ENABLE_VALUE_COMPRESSION("enable_value_compression"),
    /**
     * Tells the server that the client can tolerate the server dropping the connection.
     * The server will only do this if the client cannot read data from the stream fast
     * enough and it is highly recommended to be used in all situations. We only support
     * disabling cursor dropping for backwards compatibility. This parameter is available
     * starting in Couchbase 4.5.
     */
    SUPPORTS_CURSOR_DROPPING("supports_cursor_dropping");

    private final String value;

    ControlParameter(final String value) {
        this.value = value;
    }

    public String value() {
        return value;
    }
}
