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
package com.couchbase.client.core;

/**
 * This exception is thrown when there is a problem in the underlying network I/O. Non-idempotent requests
 * should be retried carefully while retrying idempotent ones is more straight forward.
 * Events that trigger this exception are
 *
 *  - The underlying connection to a particular endpoint on the server is closed after the request is dispatched.
 *  So the outstanding requests in flight are cancelled out. It is not definitive if the operation indeed
 *  succeeded on the server and the socket error happened while a response was on its way out.
 *
 *  - The request cannot be dispatched to the server's endpoint due to various reasons like downed node,
 *  request queue limit, particular service failure on the node,... If the failure was due to request queue
 *  limit, try increasing the value on the system property "com.couchbase.sentRequestQueueLimit". The SDK will
 *  automatically retry the request based on the retry strategy - Best Effort will retry until maxRequestLifetime
 *  and Fail Fast will cancel the request immediately.
 *
 * @author Michael Nitschinger
 * @author Subhashni Balakrishnan
 */
public class RequestCancelledException extends CouchbaseException {

    public RequestCancelledException() {
        super();
    }

    public RequestCancelledException(String message) {
        super(message);
    }

    public RequestCancelledException(String message, Throwable cause) {
        super(message, cause);
    }

    public RequestCancelledException(Throwable cause) {
        super(cause);
    }
}
