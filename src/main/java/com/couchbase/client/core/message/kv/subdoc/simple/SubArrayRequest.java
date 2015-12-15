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

package com.couchbase.client.core.message.kv.subdoc.simple;

import com.couchbase.client.core.annotations.InterfaceAudience;
import com.couchbase.client.core.annotations.InterfaceStability;
import com.couchbase.client.core.endpoint.kv.KeyValueHandler;
import io.netty.buffer.ByteBuf;

/**
 * A request covering all sub-document array operations (see {@link ArrayOperation}).
 *
 * @author Simon Baslé
 * @since 1.2
 */
@InterfaceStability.Experimental
@InterfaceAudience.Public
public class SubArrayRequest extends AbstractSubdocMutationRequest {

    private final ArrayOperation arrayOp;

    /**
     * Creates a new {@link SubArrayRequest} of type <i>arrayOp</i>.
     *
     * @param key        the key of the document.
     * @param path       the subdocument path to consider inside the document.
     * @param arrayOp    the specific {@link ArrayOperation} to perform.
     * @param fragment   the fragment of valid JSON to mutate into at the site denoted by the path.
     * @param bucket     the bucket of the document.
     * @param expiration the TTL of the whole enclosing document.
     * @param cas        the cas value for the operation
     * @throws NullPointerException if the path is null (see {@link #EXCEPTION_NULL_PATH})
     * @throws IllegalArgumentException if the operation is {@link ArrayOperation#INSERT} and path is empty (see {@link #EXCEPTION_EMPTY_PATH})
     */
    public SubArrayRequest(String key, String path, ArrayOperation arrayOp, ByteBuf fragment, String bucket, int expiration, long cas) {
        super(key, path, fragment, bucket, expiration, cas);
        this.arrayOp = arrayOp;
        if (path.isEmpty() && arrayOp == ArrayOperation.INSERT) {
            cleanUpAndThrow(EXCEPTION_EMPTY_PATH);
        }
    }

    /**
     * Creates a new {@link SubArrayRequest} of type <i>arrayOp</i>.
     *
     * @param key        the key of the document.
     * @param path       the subdocument path to consider inside the document.
     * @param arrayOp    the specific {@link ArrayOperation} to perform.
     * @param fragment   the fragment of valid JSON to mutate into at the site denoted by the path.
     * @param bucket     the bucket of the document.
     * @throws NullPointerException if the path is null (see {@link #EXCEPTION_NULL_PATH})
     * @throws IllegalArgumentException if the operation is {@link ArrayOperation#INSERT} and path is empty (see {@link #EXCEPTION_EMPTY_PATH})
     */
    public SubArrayRequest(String key, String path, ArrayOperation arrayOp, ByteBuf fragment, String bucket) {
        this(key, path, arrayOp, fragment, bucket, 0, 0L);
    }

    @Override
    public byte opcode() {
        return arrayOp.opCode();
    }

    /**
     * @return the more specific {@link ArrayOperation} this request describes.
     */
    public ArrayOperation arrayOperation() {
        return arrayOp;
    }

    public enum ArrayOperation {
        /**
         * Prepend an existing array with a value.
         */
        PUSH_FIRST(KeyValueHandler.OP_SUB_ARRAY_PUSH_FIRST),
        /**
         * Append a value to an existing array.
         */
        PUSH_LAST(KeyValueHandler.OP_SUB_ARRAY_PUSH_LAST),
        /**
         * Insert a value at a specific index into an existing array, shifting values at and after the given index.
         */
        INSERT(KeyValueHandler.OP_SUB_ARRAY_INSERT),
        /**
         * Add a value in an existing array unless the value already is present in the array.
         *
         * Existence of the value is tested via String comparison, and the array can only be containing primitive values.
         */
        ADD_UNIQUE(KeyValueHandler.OP_SUB_ARRAY_ADD_UNIQUE);

        private byte opCode;

        ArrayOperation(byte opCode) {
            this.opCode = opCode;
        }

        public byte opCode() {
            return this.opCode;
        }
    }
}
