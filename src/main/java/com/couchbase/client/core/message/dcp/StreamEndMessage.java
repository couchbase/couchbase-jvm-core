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

/**
 * @author Sergey Avseyev
 */
public class StreamEndMessage extends AbstractDCPMessage {
    private final Reason reason;

    @Deprecated
    public StreamEndMessage(int totalBodyLength, short partition, final Reason reason, String bucket) {
        this(totalBodyLength, partition, reason, bucket, null);
    }

    @Deprecated
    public StreamEndMessage(int totalBodyLength, short partition, final Reason reason, String bucket, String password) {
        this(totalBodyLength, partition, reason, bucket, bucket, password);
    }

    public StreamEndMessage(int totalBodyLength, short partition, final Reason reason, final String bucket, final String username, final String password) {
        super(totalBodyLength, partition, null, bucket, username, password);
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
