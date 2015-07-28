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
package com.couchbase.client.core.utils;

import com.couchbase.client.core.CouchbaseException;
import com.couchbase.client.core.event.CouchbaseEvent;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.HashMap;
import java.util.Map;

/**
 * Utility methods for event handling.
 *
 * @author Michael Nitschinger
 * @since 1.2.0
 */
public class Events {

    private static final ObjectMapper JACKSON = new ObjectMapper();

    /**
     * Takes a {@link CouchbaseEvent} and returns a map with event information.
     *
     * @param source the source event.
     * @return a new map which contains name and type info in an event sub-map.
     */
    public static Map<String, Object> identityMap(CouchbaseEvent source) {
        Map<String, Object> root = new HashMap<String, Object>();
        Map<String, String> event = new HashMap<String, String>();

        event.put("name", source.getClass().getSimpleName().replaceAll("Event$", ""));
        event.put("type", source.type().toString());
        root.put("event", event);

        return root;
    }

    /**
     * Takes a {@link CouchbaseEvent} and generates a JSON string.
     *
     * @param source the source event.
     * @param pretty if pretty print should be used.
     * @return the generated json string.
     */
    public static String toJson(CouchbaseEvent source, boolean pretty) {
        try {
            if (pretty) {
                return JACKSON.writerWithDefaultPrettyPrinter().writeValueAsString(source.toMap());
            } else {
                return JACKSON.writeValueAsString(source.toMap());
            }
        } catch (JsonProcessingException e) {
            throw new CouchbaseException("Could not convert CouchbaseEvent " + source.toString() + " to JSON: ", e);
        }
    }
}
