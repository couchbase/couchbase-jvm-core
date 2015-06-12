/**
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
package com.couchbase.client.core.endpoint;

/**
 * Features the client negotiates with the server on a per-connection basis.
 *
 * @author Michael Nitschinger
 * @since 1.2.0
 */
public enum ServerFeatures {

    /**
     * The custom datatype feature.
     *
     * @since Couchbase Server 4.0
     */
    DATATYPE((short) 0x01),

    // TLS used to be in the server headers
    // but was never implemented. Keeping it here
    // explicitly to not confuse anyone on the 0x02 gap.
    //TLS((byte) 0x02),

    /**
     * Enable TCP Nodelay.
     *
     * @since Couchbase Server 4.0
     */
    TCPNODELAY((short) 0x03),

    /**
     * Return the sequence number on every mutation.
     *
     * @since Couchbase Server 4.0
     */
    MUTATION_SEQNO((short) 0x04),

    /**
     * Disable TCP Nodelay.
     *
     * @since Couchbase Server 4.0
     */
    TCPDELAY((short) 0x05);

    /**
     * The actual byte representation on the wire.
     */
    private final short value;

    ServerFeatures(short value) {
        this.value = value;
    }

    /**
     * Returns the actual byte value for the wire protocol.
     *
     * @return the actual wire value.
     */
    public short value() {
        return value;
    }

    public static ServerFeatures fromValue(short input) {
        switch(input) {
            case 0x01: return DATATYPE;
            case 0x03: return TCPNODELAY;
            case 0x04: return MUTATION_SEQNO;
            case 0x05: return TCPDELAY;
            default: throw new IllegalStateException("Unrequested server feature: " + input);
        }
    }
}
