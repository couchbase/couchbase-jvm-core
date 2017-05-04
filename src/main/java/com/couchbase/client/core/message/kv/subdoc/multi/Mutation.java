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

    UPSERTDOC(KeyValueHandler.OP_UPSERT),
    COUNTER(KeyValueHandler.OP_SUB_COUNTER),
    REPLACE(KeyValueHandler.OP_SUB_REPLACE),
    DICT_ADD(KeyValueHandler.OP_SUB_DICT_ADD),
    DICT_UPSERT(KeyValueHandler.OP_SUB_DICT_UPSERT),
    ARRAY_PUSH_FIRST(KeyValueHandler.OP_SUB_ARRAY_PUSH_FIRST),
    ARRAY_PUSH_LAST(KeyValueHandler.OP_SUB_ARRAY_PUSH_LAST),
    ARRAY_ADD_UNIQUE(KeyValueHandler.OP_SUB_ARRAY_ADD_UNIQUE),
    ARRAY_INSERT(KeyValueHandler.OP_SUB_ARRAY_INSERT),
    DELETE(KeyValueHandler.OP_SUB_DELETE);

    private final byte opCode;

    Mutation(byte opCode) {
        this.opCode = opCode;
    }

    public byte opCode() {
        return opCode;
    }
}
