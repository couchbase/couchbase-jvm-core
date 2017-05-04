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
 * Document options for {@link SubMultiMutationRequest}
 *
 * @author Subhashni Balakrishnan
 * @since 1.4.6
 */
@InterfaceStability.Experimental
@InterfaceAudience.Public
public class SubMultiMutationDocOptionsBuilder {
    private boolean createDocument;

    public static SubMultiMutationDocOptionsBuilder builder() {
        return new SubMultiMutationDocOptionsBuilder();
    }

    public SubMultiMutationDocOptionsBuilder createDocument(boolean createDocument) {
        this.createDocument = createDocument;
        return this;
    }

    public boolean createDocument() {
        return this.createDocument;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("{");
        sb.append(" \"createDocument\":" + createDocument);
        sb.append("}");
        return sb.toString();
    }
}