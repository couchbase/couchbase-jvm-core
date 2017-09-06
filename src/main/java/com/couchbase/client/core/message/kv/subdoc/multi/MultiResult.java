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

package com.couchbase.client.core.message.kv.subdoc.multi;

import com.couchbase.client.core.annotations.InterfaceAudience;
import com.couchbase.client.core.annotations.InterfaceStability;
import com.couchbase.client.core.message.ResponseStatus;
import io.netty.buffer.ByteBuf;
import io.netty.util.CharsetUtil;

/**
 * The result corresponding to an individual {@link LookupCommand} or {@link MutationCommand}.
 * It contains the command's path and operation for reference.
 *
 * The value only makes sense for some commands (like {@link Lookup#GET} or {@link Mutation#COUNTER}).
 * If it does make sense, it is represented as an UTF-8 encoded {@link ByteBuf}.
 * It is the responsibility of the caller to consume and {@link ByteBuf#release()} this ByteBuf.
 *
 * @author Simon Baslé
 * @since 1.2
 */
@InterfaceStability.Committed
@InterfaceAudience.Public
public class MultiResult<OPERATION> {

    private final short statusCode;
    private final ResponseStatus status;
    private final String path;
    private final OPERATION operation;
    private final ByteBuf value;

    private MultiResult(short statusCode, ResponseStatus status, String path, OPERATION operation, ByteBuf value) {
        this.statusCode = statusCode;
        this.status = status;
        this.path = path;
        this.operation = operation;
        this.value = value;
    }

    public static MultiResult<Lookup> create(short statusCode, ResponseStatus status, String path, Lookup operation, ByteBuf value) {
        return new MultiResult<Lookup>(statusCode, status, path, operation, value);
    }

    public static MultiResult<Mutation> create(short statusCode, ResponseStatus status, String path, Mutation operation, ByteBuf value) {
        return new MultiResult<Mutation>(statusCode, status, path, operation, value);
    }

    /**
     * @return the byte status of the individual operation.
     */
    public short statusCode() {
        return statusCode;
    }

    /**
     * Returns the individual operation's status.
     *
     * Note that the containing {@link MultiLookupResponse} status can only be {@link ResponseStatus#SUCCESS} if all
     * individual LookupResults are a SUCCESS too.
     *
     * A {@link Lookup#EXIST} can either be a SUCCESS if the value exist or a {@link ResponseStatus#SUBDOC_PATH_NOT_FOUND}
     * if not.
     */
    public ResponseStatus status() {
        return status;
    }

    /**
     * @return the path asked for in the original {@link LookupCommand} or {@link MutationCommand}, for reference.
     */
    public String path() {
        return path;
    }

    /**
     * @return the {@link Lookup}/{@link Mutation} operation of the original {@link LookupCommand}/{@link MutationCommand}, for reference.
     */
    public OPERATION operation() {
        return operation;
    }

    /**
     * @return the value as a {@link ByteBuf} (that you must consume and {@link ByteBuf#release()}). Can be empty
     * in case of error or if the operation doesn't return a value.
     */
    public ByteBuf value() {
        return value;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        MultiResult that = (MultiResult) o;

        if (statusCode != that.statusCode) return false;
        if (status != that.status) return false;
        if (path != null ? !path.equals(that.path) : that.path != null) return false;
        if (operation != that.operation) return false;
        if (value == null) return that.value == null;

        return value.toString(CharsetUtil.UTF_8).equals(that.value.toString(CharsetUtil.UTF_8));

    }

    @Override
    public int hashCode() {
        int result = (int) statusCode;
        result = 31 * result + (status != null ? status.hashCode() : 0);
        result = 31 * result + (path != null ? path.hashCode() : 0);
        result = 31 * result + (operation != null ? operation.hashCode() : 0);
        if (value != null) {
            result = 31 * result + value.toString(CharsetUtil.UTF_8).hashCode();
        }
        return result;
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append(operation())
                .append('(').append(path()).append("): ")
                .append(status());
        if (value.readableBytes() > 0) {
            builder.append(" = ").append(value().toString(CharsetUtil.UTF_8));
        }
        return builder.toString();
    }
}
