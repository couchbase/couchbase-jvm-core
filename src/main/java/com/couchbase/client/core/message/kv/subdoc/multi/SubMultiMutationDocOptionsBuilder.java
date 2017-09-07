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
@InterfaceStability.Committed
@InterfaceAudience.Public
public class SubMultiMutationDocOptionsBuilder {
    private boolean upsertDocument;
    private boolean insertDocument;

    public static SubMultiMutationDocOptionsBuilder builder() {
        return new SubMultiMutationDocOptionsBuilder();
    }

    @Deprecated
    public SubMultiMutationDocOptionsBuilder createDocument(boolean createDocument) {
        if (this.insertDocument && createDocument) {
            throw new IllegalArgumentException("Invalid to set createDocument to true along with insertDocument");
        }
        this.upsertDocument = createDocument;
        return this;
    }

    @Deprecated
    public boolean createDocument() {
        return this.upsertDocument;
    }

    public SubMultiMutationDocOptionsBuilder upsertDocument(boolean upsertDocument) {
        if (this.insertDocument && upsertDocument) {
            throw new IllegalArgumentException("Invalid to set upsertDocument to true along with insertDocument");
        }
        this.upsertDocument = upsertDocument;
        return this;
    }

    public boolean upsertDocument() {
        return this.upsertDocument;
    }

    public SubMultiMutationDocOptionsBuilder insertDocument(boolean insertDocument) {
        if (this.upsertDocument && insertDocument) {
            throw new IllegalArgumentException("Invalid to set insertDocument to true along with upsertDocument(or createDocumnet)");
        }
        this.insertDocument = insertDocument;
        return this;
    }

    public boolean insertDocument() {
        return this.insertDocument;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("{");
        sb.append(" \"upsertDocument\":" + upsertDocument);
        sb.append(", \"insertDocument\": " + insertDocument);
        sb.append("}");
        return sb.toString();
    }
}