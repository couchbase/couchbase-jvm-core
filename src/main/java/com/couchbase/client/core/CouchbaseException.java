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

import com.couchbase.client.core.annotations.InterfaceAudience;
import com.couchbase.client.core.annotations.InterfaceStability;
import com.couchbase.client.core.message.ResponseStatusDetails;

/**
 * Common parent exception to build a proper exception hierachy inside the driver.
 *
 * @author Michael Nitschinger
 * @since 1.0
 */
public class CouchbaseException extends RuntimeException {

    private volatile ResponseStatusDetails responseStatusDetails;

    public CouchbaseException() {
	super();
    }

    public CouchbaseException(String message) {
        super(message);
    }

    public CouchbaseException(String message, Throwable cause) {
        super(message, cause);
    }

    public CouchbaseException(Throwable cause) {
        super(cause);
    }

    @InterfaceAudience.Public
    @InterfaceStability.Experimental
    public ResponseStatusDetails details() {
        return responseStatusDetails;
    }

    @InterfaceAudience.Private
    @InterfaceStability.Experimental
    public void details(final ResponseStatusDetails responseStatusDetails) {
        this.responseStatusDetails = responseStatusDetails;
    }

    @Override
    public String getMessage() {
        String message = super.getMessage();

        if (responseStatusDetails != null) {
            message +=  " (Context: " + responseStatusDetails.context() + ", Reference: "
                    + responseStatusDetails.reference() + ")";
        }
        return message;
    }
}
