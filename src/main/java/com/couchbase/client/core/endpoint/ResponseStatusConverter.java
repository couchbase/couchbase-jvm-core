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

import com.couchbase.client.core.endpoint.kv.KeyValueStatus;
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
    public static final int HTTP_BAD_REQUEST = 400;
    public static final int HTTP_NOT_FOUND = 404;
    public static final int HTTP_INTERNAL_ERROR = 500;

    /**
     * Convert the binary protocol status in a typesafe enum that can be acted upon later.
     *
     * @param code the status to convert.
     * @return the converted response status.
     */
    public static ResponseStatus fromBinary(final short code) {
        KeyValueStatus status = KeyValueStatus.valueOf(code);
        switch (status) {
            case SUCCESS:
                return ResponseStatus.SUCCESS;
            case ERR_EXISTS:
                return ResponseStatus.EXISTS;
            case ERR_NOT_FOUND:
                return ResponseStatus.NOT_EXISTS;
            case ERR_NOT_MY_VBUCKET:
                return ResponseStatus.RETRY;
            case ERR_NOT_STORED:
                return ResponseStatus.NOT_STORED;
            case ERR_TOO_BIG:
                return ResponseStatus.TOO_BIG;
            case ERR_TEMP_FAIL:
                return ResponseStatus.TEMPORARY_FAILURE;
            case ERR_BUSY:
                return ResponseStatus.SERVER_BUSY;
            case ERR_NO_MEM:
                return ResponseStatus.OUT_OF_MEMORY;
            case ERR_UNKNOWN_COMMAND:
                return ResponseStatus.COMMAND_UNAVAILABLE;
            case ERR_NOT_SUPPORTED:
                return ResponseStatus.COMMAND_UNAVAILABLE;
            case ERR_INTERNAL:
                return ResponseStatus.INTERNAL_ERROR;
            case ERR_INVALID:
                return ResponseStatus.INVALID_ARGUMENTS;
            case ERR_DELTA_BADVAL:
                return ResponseStatus.INVALID_ARGUMENTS;
            default:
                LOGGER.warn("Unexpected ResponseStatus with Protocol KeyValue: {} (0x{}, {})",
                        status, Integer.toHexString(status.code()), status.description());
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
            case HTTP_BAD_REQUEST:
                status = ResponseStatus.INVALID_ARGUMENTS;
                break;
            case HTTP_INTERNAL_ERROR:
                status = ResponseStatus.INTERNAL_ERROR;
                break;
            default:
                LOGGER.warn("Unknown ResponseStatus with Protocol HTTP: {}", code);
                status = ResponseStatus.FAILURE;
        }
        return status;
    }

}
