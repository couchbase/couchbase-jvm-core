/*
 * Copyright (c) 2017 Couchbase, Inc.
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

import com.couchbase.client.core.logging.CouchbaseLogger;
import com.couchbase.client.core.logging.CouchbaseLoggerFactory;
import com.couchbase.client.core.utils.DefaultObjectMapper;
import com.fasterxml.jackson.core.type.TypeReference;
import io.netty.buffer.ByteBuf;

import java.util.HashMap;

/**
 * Container for extended response status information.
 *
 * @author Michael Nitschinger
 * @since 1.4.7
 */
public class ResponseStatusDetails {

    private static final TypeReference<HashMap<String,HashMap<String, String>>> JACKSON_TYPEREF
            = new TypeReference<HashMap<String,HashMap<String, String>>>() {};

    private static final CouchbaseLogger LOGGER = CouchbaseLoggerFactory.getInstance(ResponseStatusDetails.class);

    private final String reference;
    private final String context;

    /**
     * Helper method to convert a {@link ByteBuf} input into the details.
     *
     * It will NOT release the buffer.
     */
    public static ResponseStatusDetails convert(final ByteBuf input) {
        if (input.readableBytes() <= 0) {
            return null;
        }

        try {
            byte[] inputBytes = new byte[input.readableBytes()];
            input.readBytes(inputBytes);
            HashMap<String,HashMap<String, String>> result = DefaultObjectMapper.readValue(inputBytes, JACKSON_TYPEREF);
            HashMap<String, String> errorMap = result.get("error");
            if (errorMap == null) {
                LOGGER.warn("Exception while converting ResponseStatusDetails (no error json object), ignoring.");
                return null;
            }
            return new ResponseStatusDetails(errorMap.get("ref"), errorMap.get("context"));
        } catch (Exception ex) {
            LOGGER.warn("Exception while converting ResponseStatusDetails, ignoring.", ex);
            return null;
        }
    }

    ResponseStatusDetails(final String reference, final String context) {
        this.reference = reference;
        this.context = context;
    }

    public String reference() {
        return reference;
    }

    public String context() {
        return context;
    }

    @Override
    public String toString() {
        return "ResponseStatusDetails{" +
            "reference='" + reference + '\'' +
            ", context='" + context + '\'' +
            '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ResponseStatusDetails that = (ResponseStatusDetails) o;

        if (reference != null ? !reference.equals(that.reference) : that.reference != null) return false;
        return context != null ? context.equals(that.context) : that.context == null;
    }

    @Override
    public int hashCode() {
        int result = reference != null ? reference.hashCode() : 0;
        result = 31 * result + (context != null ? context.hashCode() : 0);
        return result;
    }

    /**
     * Stringify the status details and the status in a best effort manner.
     */
    public static String stringify(final ResponseStatus status, final ResponseStatusDetails details) {
        String result = status.toString();
        if (details != null) {
            result = result + " (Context: " + details.context() + ", Reference: " + details.reference() + ")";
        }
        return result;
    }

}
