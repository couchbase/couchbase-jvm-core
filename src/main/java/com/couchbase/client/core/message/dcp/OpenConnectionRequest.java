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

package com.couchbase.client.core.message.dcp;

import com.couchbase.client.core.annotations.InterfaceAudience;
import com.couchbase.client.core.annotations.InterfaceStability;

/**
 * Initiate logical DCP channel.
 *
 * @author Sergey Avseyev
 * @since 1.1.0
 */
@InterfaceStability.Experimental
@InterfaceAudience.Private
public class OpenConnectionRequest extends AbstractDCPRequest {
    /**
     * Type of connection the server have to initiate for the client.
     * For example, if the client wants to pull the data from server, it need to
     * choose {@link ConnectionType#CONSUMER}.
     */
    private final ConnectionType type;

    /**
     * The connection name can be used to get statistics about the connection state
     * as well as other useful debugging information. If a connection already exists
     * on the Producer with the same name then the old connection is closed and
     * a new one is opened.
     */
    private final String connectionName;
    private final int sequenceNumber;

    public OpenConnectionRequest(String connectionName, String bucket) {
        this(connectionName, ConnectionType.CONSUMER, 0, bucket, null);
    }

    public OpenConnectionRequest(String connectionName, String bucket, String password) {
        this(connectionName, ConnectionType.CONSUMER, 0, bucket, password);
    }

    public OpenConnectionRequest(String connectionName, ConnectionType type, String bucket) {
        this(connectionName, type, 0, bucket, null);
    }

    public OpenConnectionRequest(String connectionName, ConnectionType type, String bucket, String password) {
        this(connectionName, type, 0, bucket, password);
    }

    public OpenConnectionRequest(String connectionName, ConnectionType type, int sequenceNumber, String bucket, String password) {
        super(bucket, password);
        this.type = type;
        this.sequenceNumber = sequenceNumber;
        this.connectionName = connectionName;
    }

    public int sequenceNumber() {
        return sequenceNumber;
    }

    public String connectionName() {
        return connectionName;
    }

    public ConnectionType type() {
        return type;
    }

}
