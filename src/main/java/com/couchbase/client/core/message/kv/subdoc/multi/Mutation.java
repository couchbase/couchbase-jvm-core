/*
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

package com.couchbase.client.core.message.kv.subdoc.multi;

import com.couchbase.client.core.annotations.InterfaceAudience;
import com.couchbase.client.core.annotations.InterfaceStability;
import com.couchbase.client.core.endpoint.kv.KeyValueHandler;

/**
 * Enumeration of possible mutations inside a sub-document {@link MutationCommand}.
 *
 * @author Simon Baslé
 * @since 1.2
 */
@InterfaceStability.Experimental
@InterfaceAudience.Public
public enum Mutation {

    COUNTER(KeyValueHandler.OP_SUB_COUNTER),
    REPLACE(KeyValueHandler.OP_SUB_REPLACE),
    DICT_ADD(KeyValueHandler.OP_SUB_DICT_ADD),
    DICT_UPSERT(KeyValueHandler.OP_SUB_DICT_UPSERT),
    ARRAY_PUSH_FIRST(KeyValueHandler.OP_SUB_ARRAY_PUSH_FIRST),
    ARRAY_PUSH_LAST(KeyValueHandler.OP_SUB_ARRAY_PUSH_LAST),
    ARRAY_ADD_UNIQUE(KeyValueHandler.OP_SUB_ARRAY_ADD_UNIQUE),
    ARRAY_INSERT(KeyValueHandler.OP_SUB_ARRAY_INSERT);

    private final byte opCode;

    Mutation(byte opCode) {
        this.opCode = opCode;
    }

    public byte opCode() {
        return opCode;
    }
}
