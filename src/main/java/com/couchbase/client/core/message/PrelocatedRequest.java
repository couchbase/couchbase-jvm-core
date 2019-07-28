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

package com.couchbase.client.core.message;

/**
 * A {@link CouchbaseRequest} that can be targeted at a specific node through the corresponding
 * hostname, shortcutting the dispatch usually performed by a
 * {@link com.couchbase.client.core.node.locate.Locator}..
 *
 * Note that only some types of services perform this user-specified dispatch.
 *
 * @author Simon Baslé
 * @since 1.2.7
 */
public interface PrelocatedRequest extends CouchbaseRequest {

    /**
     * The hostname to send this request to, or null to use
     * default {@link com.couchbase.client.core.node.locate.Locator node location process}.
     *
     * @return the address of the target node or null to revert to default dispatching.
     */
    String sendTo();

}
