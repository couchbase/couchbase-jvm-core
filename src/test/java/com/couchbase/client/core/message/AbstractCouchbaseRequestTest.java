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

package com.couchbase.client.core.message;

import org.junit.Test;
import rx.observers.TestSubscriber;

import java.util.concurrent.TimeoutException;

import static org.mockito.Mockito.mock;

/**
 * Verifies the functionality of the {@link AbstractCouchbaseRequest}.
 */
public class AbstractCouchbaseRequestTest {

    @Test
    public void shouldCompleteResponseWithSuccess() {
        SampleRequest sr = new SampleRequest();
        TestSubscriber<CouchbaseResponse> subs = new TestSubscriber<CouchbaseResponse>();
        sr.observable().subscribe(subs);
        subs.assertNotCompleted();

        CouchbaseResponse mockResponse = mock(CouchbaseResponse.class);
        sr.succeed(mockResponse);

        subs.awaitTerminalEvent();
        subs.assertCompleted();
        subs.assertValue(mockResponse);
    }

    @Test
    public void shouldCompleteResponseWithFailure() {
        SampleRequest sr = new SampleRequest();
        TestSubscriber<CouchbaseResponse> subs = new TestSubscriber<CouchbaseResponse>();
        sr.observable().subscribe(subs);
        subs.assertNotCompleted();

        Exception exception = new TimeoutException();
        sr.fail(exception);

        subs.awaitTerminalEvent();
        subs.assertError(exception);
    }

    class SampleRequest extends AbstractCouchbaseRequest {
        public SampleRequest() {
            super("bucket", "password");
        }
    }

}