/**
 * Copyright (C) 2014 Couchbase, Inc.
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
package com.couchbase.client.core.config.parser;

import com.couchbase.client.core.CouchbaseException;
import com.couchbase.client.core.config.BucketConfig;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;

/**
 * .
 *
 * @author Michael Nitschinger
 */
public final class BucketConfigParser {
    /**
     * Jackson object mapper for JSON parsing.
     */
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    /**
     * Parse a raw configuration into a {@link BucketConfig}.
     *
     * @param input the raw string input.
     * @return the parsed bucket configuration.
     */
    public static BucketConfig parse(final String input) {
        try {
            return OBJECT_MAPPER.readValue(input, BucketConfig.class);
        } catch (IOException e) {
            throw new CouchbaseException("Could not parse configuration", e);
        }
    }
}
