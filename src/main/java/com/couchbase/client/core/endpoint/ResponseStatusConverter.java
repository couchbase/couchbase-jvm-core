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

import com.couchbase.client.core.logging.CouchbaseLogger;
import com.couchbase.client.core.logging.CouchbaseLoggerFactory;
import com.couchbase.client.core.message.ResponseStatus;

/**
 * Helper class to easily convert different handler status types to a common one.
 *
 * @author Michael Nitschinger
 * @since 1.1.2
 */
public class ResponseStatusConverter {

    /**
     * The logger used.
     */
    private static final CouchbaseLogger LOGGER = CouchbaseLoggerFactory.getInstance(ResponseStatusConverter.class);

    public static final int HTTP_OK = 200;
    public static final int HTTP_CREATED = 201;
    public static final int HTTP_ACCEPTED = 202;
    public static final int HTTP_NOT_FOUND = 404;

    public static final short BINARY_SUCCESS = 0x00;
    public static final short BINARY_ERR_NOT_FOUND = 0x01;
    public static final short BINARY_ERR_EXISTS = 0x02;
    public static final short BINARY_ERR_2BIG = 0x03;
    public static final short BINARY_ERR_INVAL = 0x04;
    public static final short BINARY_ERR_NOT_STORED = 0x05;
    public static final short BINARY_ERR_DELTA_BADVAL = 0x06;
    public static final short BINARY_ERR_NOT_MY_VBUCKET = 0x07;
    public static final short BINARY_ERR_UNKNOWN_COMMAND = 0x81;
    public static final short BINARY_ERR_NO_MEM = 0x82;
    public static final short BINARY_ERR_NOT_SUPPORTED = 0x83;
    public static final short BINARY_ERR_INTERNAL = 0x84;
    public static final short BINARY_ERR_BUSY = 0x85;
    public static final short BINARY_ERR_TEMP_FAIL = 0x86;

    /**
     * Convert the binary protocol status in a typesafe enum that can be acted upon later.
     *
     * @param status the status to convert.
     * @return the converted response status.
     */
    public static ResponseStatus fromBinary(final short status) {
        switch (status) {
            case BINARY_SUCCESS:
                return ResponseStatus.SUCCESS;
            case BINARY_ERR_EXISTS:
                return ResponseStatus.EXISTS;
            case BINARY_ERR_NOT_FOUND:
                return ResponseStatus.NOT_EXISTS;
            case BINARY_ERR_NOT_MY_VBUCKET:
                return ResponseStatus.RETRY;
            case BINARY_ERR_NOT_STORED:
                return ResponseStatus.NOT_STORED;
            case BINARY_ERR_2BIG:
                return ResponseStatus.TOO_BIG;
            case BINARY_ERR_TEMP_FAIL:
                return ResponseStatus.TEMPORARY_FAILURE;
            case BINARY_ERR_BUSY:
                return ResponseStatus.SERVER_BUSY;
            case BINARY_ERR_NO_MEM:
                return ResponseStatus.OUT_OF_MEMORY;
            case BINARY_ERR_UNKNOWN_COMMAND:
                return ResponseStatus.COMMAND_UNAVAILABLE;
            case BINARY_ERR_NOT_SUPPORTED:
                return ResponseStatus.COMMAND_UNAVAILABLE;
            case BINARY_ERR_INTERNAL:
                return ResponseStatus.INTERNAL_ERROR;
            case BINARY_ERR_INVAL:
                return ResponseStatus.INVALID_ARGUMENTS;
            case BINARY_ERR_DELTA_BADVAL:
                return ResponseStatus.INVALID_ARGUMENTS;
            default:
                LOGGER.warn("Unknown ResponseStatus with Protocol KeyValue: {}", Integer.toHexString(status));
                return ResponseStatus.FAILURE;
        }
    }

    /**
     * Convert the http protocol status in a typesafe enum that can be acted upon later.
     *
     * @param code the status to convert.
     * @return the converted response status.
     */
    public static ResponseStatus fromHttp(final int code) {
        ResponseStatus status;
        switch (code) {
            case HTTP_OK:
            case HTTP_CREATED:
            case HTTP_ACCEPTED:
                status = ResponseStatus.SUCCESS;
                break;
            case HTTP_NOT_FOUND:
                status = ResponseStatus.NOT_EXISTS;
                break;
            default:
                LOGGER.warn("Unknown ResponseStatus with Protocol HTTP: {}", code);
                status = ResponseStatus.FAILURE;
        }
        return status;
    }

}
