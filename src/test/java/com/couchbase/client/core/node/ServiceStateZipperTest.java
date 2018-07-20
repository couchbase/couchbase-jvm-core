/*
 * Copyright (c) 2018 Couchbase, Inc.
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

package com.couchbase.client.core.node;

import com.couchbase.client.core.service.Service;
import com.couchbase.client.core.state.LifecycleState;
import org.junit.Test;
import rx.Observable;
import rx.subjects.BehaviorSubject;
import rx.subjects.Subject;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Verifies the state transitions of the {@link ServiceStateZipper}.
 */
public class ServiceStateZipperTest {

    @Test
    public void shouldHandleDegradedService() {
        ServiceStateZipper zipper = new ServiceStateZipper(LifecycleState.DISCONNECTED);

        assertEquals(LifecycleState.DISCONNECTED, zipper.state());

        Service configService = mock(Service.class);
        when(configService.states()).thenReturn(Observable.just(LifecycleState.IDLE));
        zipper.register(configService, configService);

        assertEquals(LifecycleState.IDLE, zipper.state());

        Service queryService = mock(Service.class);
        when(queryService.states()).thenReturn(Observable.just(LifecycleState.DEGRADED));
        zipper.register(queryService, queryService);

        assertEquals(LifecycleState.DEGRADED, zipper.state());
    }
}