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

package com.couchbase.client.core.endpoint.kv;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonValue;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * The {@link ErrorMap} contains mappings from errors to their attributes, negotiated
 * between the client and the server.
 *
 * From the documentation:
 *
 * An Error Map is a mapping of error to their attributes and properties. It is used by
 * connected clients to handle error codes which they may otherwise not be aware of.
 *
 * The error map solves the problem where clients would incorrectly handle newer error codes,
 * and/or where the server would disconnect the client because it needed to send it a new error code.
 *
 * The solution via Error Map is to establish a contract between client and server about certain
 * attributes which each error code may have. These attributes indicate whether an error may be
 * passed through, whether the command is retriable and so on. When a client receives an error code
 * it does not know about it, it can look up its attributes and then determine the next course of
 * action.
 *
 * @author Michael Nitschinger
 * @since 1.4.4
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class ErrorMap implements Comparable<ErrorMap> {

    private final int version;
    private final int revision;
    private final Map<Short, ErrorCode> errors;

    @JsonCreator
    public ErrorMap(
        @JsonProperty("version") int version,
        @JsonProperty("revision") int revision,
        @JsonProperty("errors") Map<String, ErrorCode> errors) {
        this.version = version;
        this.revision = revision;
        this.errors = toShortKeys(errors);
    }

    private static Map<Short, ErrorCode> toShortKeys(Map<String, ErrorCode> errors) {
        Map<Short, ErrorCode> result = new HashMap<Short, ErrorCode>(errors.size());
        for (Map.Entry<String, ErrorCode> entry : errors.entrySet()) {
            result.put(Short.parseShort(entry.getKey(), 16), entry.getValue());
        }
        return result;
    }

    @Override
    public int compareTo(ErrorMap o) {
        if (version < o.version()) {
            return -1;
        } else if (version > o.version()) {
            return 1;
        }

        if (revision < o.revision()) {
            return -1;
        } else if (revision > o.revision()) {
            return 1;
        }

        return 0;
    }

    public int version() {
        return version;
    }

    public int revision() {
        return revision;
    }

    public Map<Short, ErrorCode> errors() {
        return errors;
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class ErrorCode {
        private final String name;
        private final String description;
        private final List<ErrorAttribute> attributes;

        @JsonCreator
        public ErrorCode(
            @JsonProperty("name") String name,
            @JsonProperty("desc") String description,
            @JsonProperty("attrs") List<ErrorAttribute> attributes) {
            this.name = name;
            this.description = description;
            this.attributes = attributes;
        }

        public String name() {
            return name;
        }

        public String description() {
            return description;
        }

        public List<ErrorAttribute> attributes() {
            return attributes;
        }

        @Override
        public String toString() {
            return "ErrorCode{" +
                    "name='" + name + '\'' +
                    ", description='" + description + '\'' +
                    ", attributes=" + attributes +
                    '}';
        }
    }

    public enum ErrorAttribute {
        /**
         * This attribute means that the error is related to a constraint
         * failure regarding the item itself, i.e. the item does not exist,
         * already exists, or its current value makes the current operation
         * impossible. Retrying the operation when the item's value or status
         * has changed may succeed.
         */
        ITEM_ONLY("item-only"),
        /**
         *  This attribute means that a user's input was invalid because it
         *  violates the semantics of the operation, or exceeds some predefined
         *  limit.
         */
        INVALID_INPUT("invalid-input"),
        /**
         * The client's cluster map may be outdated and requires updating. The
         * client should obtain a newer configuration.
         */
        FETCH_CONFIG("fetch-config"),
        /**
         * The current connection is no longer valid. The client must reconnect
         * to the server. Note that the presence of other attributes may indicate
         * an alternate remedy to fixing the connection without a disconnect, but
         * without special remedial action a disconnect is needed.
         */
        CONN_STATE_INVALIDATED("conn-state-invalidated"),
        /**
         *  The operation failed because the client failed to authenticate or is not authorized
         *  to perform this operation. Note that this error in itself does not mean the connection
         *  is invalid, unless conn-state-invalidated is also present.
         */
        AUTH("auth"),
        /**
         * This error code must be handled specially. If it is not handled, the connection must be
         * dropped.
         */
        SPECIAL_HANDLING("special-handling"),
        /**
         * The operation is not supported, possibly because the of server version, bucket type, or
         * current user.
         */
        SUPPORT("support"),
        /**
         * This error is transient. Note that this does not mean the error is retriable.
         */
        TEMP("temp"),
        /**
         *  This is an internal error in the server.
         */
        INTERNAL("internal"),
        /**
         *  The operation may be retried immediately.
         */
        RETRY_NOW("retry-now"),
        /**
         * The operation may be retried after some time.
         */
        RETRY_LATER("retry-later"),
        /**
         * The error is related to the subdocument subsystem.
         */
        SUBDOC("subdoc"),
        /**
         * The error is related to the DCP subsystem.
         */
        DCP("dcp");

        private final String raw;

        ErrorAttribute(String raw) {
            this.raw = raw;
        }

        @JsonValue
        public String raw() {
            return raw;
        }
    }

    @Override
    public String toString() {
        return "ErrorMap{" +
                "version=" + version +
                ", revision=" + revision +
                ", errors=" + errors +
                '}';
    }
}
