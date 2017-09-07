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

package com.couchbase.client.core.message.kv.subdoc.multi;

import com.couchbase.client.core.annotations.InterfaceAudience;
import com.couchbase.client.core.annotations.InterfaceStability;

/**
 * Document options for {@link SubMultiLookupRequest}
 *
 * @author Subhashni Balakrishnan
 * @since 1.5.0
 */
@InterfaceStability.Committed
@InterfaceAudience.Public
public class SubMultiLookupDocOptionsBuilder {
    private boolean accessDeleted;

    public static SubMultiLookupDocOptionsBuilder builder() {
        return new SubMultiLookupDocOptionsBuilder();
    }

    public SubMultiLookupDocOptionsBuilder accessDeleted(boolean accessDeleted) {
        this.accessDeleted = accessDeleted;
        return this;
    }

    public boolean accessDeleted() {
        return this.accessDeleted;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("{");
        sb.append(" \"accessDeleted\": " + accessDeleted);
        sb.append("}");
        return sb.toString();
    }
}