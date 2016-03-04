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

import com.couchbase.client.core.endpoint.dcp.DCPConnection;

/**
 * @author Sergey Avseyev
 */
public class StreamEndMessage extends AbstractDCPMessage {
    private final Reason reason;

    public StreamEndMessage(DCPConnection connection, int totalBodyLength, short partition, final Reason reason, String bucket) {
        this(connection, totalBodyLength, partition, reason, bucket, null);
    }

    public StreamEndMessage(DCPConnection connection, int totalBodyLength, short partition, final Reason reason, final String bucket, final String password) {
        super(connection, totalBodyLength, partition, null, bucket, password);
        this.reason = reason;
    }

    /**
     * Specify to the consumer why the stream was closed
     */
    public Reason reason() {
        return reason;
    }

    public enum Reason {
        /**
         * The stream has finished without error.
         */
        OK(0x00),
        /**
         * This indicates that the close stream command was invoked on this stream
         * causing it to be closed by force.
         */
        CLOSED(0x01),
        /**
         * The state of the VBucket that is being streamed has changed to state
         * that the consumer does not want to receive.
         */
        STATE_CHANGED(0x02),
        /**
         * The stream is closing because the connection is being disconnected.
         */
        DISCONNECTED(0x03),
        /**
         * The stream is closing because the client cannot read from the stream
         * fast enough. This is done to prevent the server from running out of
         * resources trying while trying to serve the client. When the client
         * is ready to read from the stream again it should reconnect. This flag
         * is available starting in Couchbase 4.5.
         */
        TOO_SLOW(0x04),

        UNKNOWN(-1);

        private final int flags;

        Reason(int flags) {
            this.flags = flags;
        }

        public static Reason valueOf(int flags) {
            switch (flags) {
                case 0x00:
                    return OK;
                case 0x01:
                    return CLOSED;
                case 0x02:
                    return STATE_CHANGED;
                case 0x03:
                    return DISCONNECTED;
                case 0x04:
                    return TOO_SLOW;
                default:
                    return UNKNOWN;
            }
        }

        public int flags() {
            return flags;
        }
    }
}
