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
package com.couchbase.client.core.endpoint;

import com.couchbase.client.core.endpoint.kv.ErrorMap;
import com.couchbase.client.core.endpoint.kv.KeyValueStatus;
import com.couchbase.client.core.logging.CouchbaseLogger;
import com.couchbase.client.core.logging.CouchbaseLoggerFactory;
import com.couchbase.client.core.message.ResponseStatus;
import com.couchbase.client.core.message.ResponseStatusDetails;
import io.netty.buffer.ByteBuf;

/**
 * Helper class to easily convert different handler status types to a common one.
 *
 * @author Michael Nitschinger
 * @since 1.1.2
 * @see ResponseStatus
 * @see KeyValueStatus
 */
public class ResponseStatusConverter {

    /**
     * Start with a static empty kv error map.
     */
    private static volatile ErrorMap BINARY_ERROR_MAP = null;

    /**
     * The logger used.
     */
    private static final CouchbaseLogger LOGGER = CouchbaseLoggerFactory.getInstance(ResponseStatusConverter.class);

    public static final int HTTP_OK = 200;
    public static final int HTTP_CREATED = 201;
    public static final int HTTP_ACCEPTED = 202;
    public static final int HTTP_BAD_REQUEST = 400;
    public static final int HTTP_UNAUTHORIZED = 401;
    public static final int HTTP_NOT_FOUND = 404;
    public static final int HTTP_INTERNAL_ERROR = 500;

    private static final byte JSON_DATATYPE = 0x01;


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
            case ERR_ACCESS:
                return ResponseStatus.ACCESS_ERROR;
            case ERR_INTERNAL:
                return ResponseStatus.INTERNAL_ERROR;
            case ERR_INVALID:
                return ResponseStatus.INVALID_ARGUMENTS;
            case ERR_DELTA_BADVAL:
                return ResponseStatus.INVALID_ARGUMENTS;
            case ERR_RANGE:
                return ResponseStatus.RANGE_ERROR;
            case ERR_ROLLBACK:
                return ResponseStatus.ROLLBACK;
            //== the following codes are for subdocument API ==
            case ERR_SUBDOC_PATH_NOT_FOUND:
                return ResponseStatus.SUBDOC_PATH_NOT_FOUND;
            case ERR_SUBDOC_PATH_MISMATCH:
                return ResponseStatus.SUBDOC_PATH_MISMATCH;
            case ERR_SUBDOC_PATH_INVALID:
                return ResponseStatus.SUBDOC_PATH_INVALID;
            case ERR_SUBDOC_PATH_TOO_BIG:
                return ResponseStatus.SUBDOC_PATH_TOO_BIG;
            case ERR_SUBDOC_DOC_TOO_DEEP:
                return ResponseStatus.SUBDOC_DOC_TOO_DEEP;
            case ERR_SUBDOC_VALUE_CANTINSERT:
                return ResponseStatus.SUBDOC_VALUE_CANTINSERT;
            case ERR_SUBDOC_DOC_NOT_JSON:
                return ResponseStatus.SUBDOC_DOC_NOT_JSON;
            case ERR_SUBDOC_NUM_RANGE:
                return ResponseStatus.SUBDOC_NUM_RANGE;
            case ERR_SUBDOC_DELTA_RANGE:
                return ResponseStatus.SUBDOC_DELTA_RANGE;
            case ERR_SUBDOC_PATH_EXISTS:
                return ResponseStatus.SUBDOC_PATH_EXISTS;
            case ERR_SUBDOC_VALUE_TOO_DEEP:
                return ResponseStatus.SUBDOC_VALUE_TOO_DEEP;
            case ERR_SUBDOC_INVALID_COMBO:
                return ResponseStatus.SUBDOC_INVALID_COMBO;
            case ERR_SUBDOC_MULTI_PATH_FAILURE:
                return ResponseStatus.SUBDOC_MULTI_PATH_FAILURE;
            case ERR_SUBDOC_XATTR_INVALID_FLAG_COMBO:
                return ResponseStatus.INTERNAL_ERROR;
            case ERR_SUBDOC_XATTR_UNKNOWN_MACRO:
                return ResponseStatus.SUBDOC_XATTR_UNKNOWN_MACRO;
            case ERR_SUBDOC_XATTR_INVALID_KEY_COMBO:
                return ResponseStatus.SUBDOC_XATTR_INVALID_KEY_COMBO;
            //== end of subdocument API codes ==
        }
        return ResponseStatus.FAILURE;
    }

    /**
     * Get the error code from Key Value error map
     *
     * @param code the status to convert.
     */
    public static ErrorMap.ErrorCode readErrorCodeFromErrorMap(final short code) {
        if (BINARY_ERROR_MAP == null) {
            LOGGER.trace("Binary error map unavailable");
            return null;
        }
        ErrorMap.ErrorCode result = BINARY_ERROR_MAP.errors().get(code);
        return result;
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
            case HTTP_UNAUTHORIZED:
                status = ResponseStatus.ACCESS_ERROR;
                break;
            default:
                LOGGER.warn("Unknown ResponseStatus with Protocol HTTP: {}", code);
                status = ResponseStatus.FAILURE;
        }
        return status;
    }

    public static ResponseStatusDetails detailsFromBinary(final byte dataType, final ByteBuf content) {
        if ((dataType & JSON_DATATYPE) != 0) {
            return ResponseStatusDetails.convert(content.slice());
        } else {
            return null;
        }
    }

    /**
     * Updates the current error map in use for all uses of the response status converter.
     *
     * If the provided one is older than the one stored, this update operation will be ignored.
     *
     * @param map the map in use, it always uses the latest one.
     */
    public static void updateBinaryErrorMap(final ErrorMap map) {
        if (map == null) {
            return;
        }

        if (BINARY_ERROR_MAP == null || map.compareTo(BINARY_ERROR_MAP) > 0) {
            BINARY_ERROR_MAP = map;
        }
    }

    /**
     * Get the current error map used by the response status converter
     *
     * @return error map
     */
    public static ErrorMap getBinaryErrorMap() {
        return BINARY_ERROR_MAP;
    }
}
