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
package com.couchbase.client.core.message;

/**
 * Typesafe status code returned by {@link CouchbaseResponse}s.
 *
 * @author Michael Nitschinger
 * @since 1.0
 * @see com.couchbase.client.core.endpoint.ResponseStatusConverter
 * @see com.couchbase.client.core.endpoint.kv.KeyValueStatus
 */
public enum ResponseStatus {

    /**
     * If the response is successful and finished.
     */
    SUCCESS,

    /**
     * If the request expected the document to not exist, but it existed already.
     */
    EXISTS,

    /**
     * If the request expected the document to exit, but it didn't exist already.
     */
    NOT_EXISTS,

    /**
     * The response indicates that a mutation try did not happen properly.
     */
    NOT_STORED,

    /**
     * The response indicates that the request was too big for some reason.
     */
    TOO_BIG,

    /**
     * Indicates a failure which is considered to be transient.
     */
    TEMPORARY_FAILURE,

    /**
     * Indicates that the server is busy, which is considered to be transient.
     */
    SERVER_BUSY,

    /**
     * Indicates that the request type was dispatched but not known by the server or it is not supported.
     */
    COMMAND_UNAVAILABLE,

    /**
     * The requested service is currently out of memory.
     */
    OUT_OF_MEMORY,

    /**
     * The service reported that the request arguments are invalid.
     */
    INVALID_ARGUMENTS,

    /**
     * Access error. It can also happen if some operations are not supported like subdoc.
     */
    ACCESS_ERROR,
    /**
     * The remote service failed for an internal reason.
     */
    INTERNAL_ERROR,

    /**
     * Generic failure status. Usually indicates a server status code that is
     * unknown to the SDK (for binary responses it can be found in the response).
     */
    FAILURE,

    /**
     * The underlying response indicates retry is in order.
     *
     * This is a internal response and should not bubble up to the user level.
     */
    RETRY,

    /**
     * The server reports that requested vBucketID or sequence number does
     * not fit allowed range.
     */
    RANGE_ERROR,

    /**
     * The server tells client to rollback its view of the DCP stream state.
     * New sequence number passed in the payload.
     */
    ROLLBACK,

    /**
     * Subdocument error indicating the path inside the JSON is invalid.
     */
    SUBDOC_PATH_NOT_FOUND,

    /**
     * Subdocument error indicating one of the path components was denoting a wrong type (eg. trying to access
     * an array index in an entry that isn't an array). Also for arithmetic operations when the value of the
     * path is not a number.
     */
    SUBDOC_PATH_MISMATCH,

    /**
     * Subdocument error indicating that the path provided is invalid. For operations requiring an array index, this
     * is returned if the last component of that path isn't an array. Similarly for operations requiring a dictionary,
     * if the last component isn't a dictionary but eg. an array index.
     */
    SUBDOC_PATH_INVALID,

    /**
     * Subdocument error indicating that the path is too large (ie. the string is too long) or too deep (more that 32 components).
     */
    SUBDOC_PATH_TOO_BIG,

    /**
     * Subdocument error indicating that the target document's level of JSON nesting is too deep to be processed by the subdoc service.
     */
    SUBDOC_DOC_TOO_DEEP,

    /**
     * Subdocument error indicating that a mutation fragment cannot be applied without resulting in invalid JSON.
     */
    SUBDOC_VALUE_CANTINSERT,

    /**
     * Subdocument error indicating that the target document is not flagged or recognized as JSON.
     */
    SUBDOC_DOC_NOT_JSON,

    /**
     * Subdocument error indicating that, for arithmetic subdoc operations, the existing number is already too large.
     */
    SUBDOC_NUM_RANGE,

    /**
     * Subdocument error indicating that for arithmetic subdoc operations, the operation will make the value too large.
     */
    SUBDOC_DELTA_RANGE,

    /**
     * Subdocument error indicating that the last component of the path already exist despite the mutation operation
     * expecting it not to exist (the mutation was expecting to create only the last part of the path and store the
     * fragment there).
     */
    SUBDOC_PATH_EXISTS,

    /**
     * Subdocument error indicating that inserting the fragment would make the document too deep.
     */
    SUBDOC_VALUE_TOO_DEEP,

    /**
     * Subdocument error indicating that, in a multi-specification, an invalid combination of commands were specified,
     * including the case where too many paths were specified.
     */
    SUBDOC_INVALID_COMBO,

    /**
     * Subdocument error indicating that, in a multi-specification, one or more commands failed to execute on a document
     * which exists (ie. the key was valid).
     */
    SUBDOC_MULTI_PATH_FAILURE,

    /**
     * Subdocument extended attribute error indicating the server couldn't expand the supplied macro
     */
    SUBDOC_XATTR_UNKNOWN_MACRO,

    /**
     * Subdocument extended attribute error indicating use of key paths from different namespaces
     */
    SUBDOC_XATTR_INVALID_KEY_COMBO;

    /**
     * Check if the current {@link ResponseStatus} is success.
     */
    public boolean isSuccess() {
        return this == ResponseStatus.SUCCESS;
    }

}
