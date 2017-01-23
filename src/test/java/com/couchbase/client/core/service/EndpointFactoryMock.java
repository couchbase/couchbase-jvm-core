/**
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
package com.couchbase.client.core.service;

import com.couchbase.client.core.ResponseEvent;
import com.couchbase.client.core.endpoint.Endpoint;
import com.couchbase.client.core.env.CoreEnvironment;
import com.couchbase.client.core.state.LifecycleState;
import com.lmax.disruptor.RingBuffer;
import org.mockito.Mockito;
import org.mockito.invocation.Invocation;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import rx.Observable;
import rx.functions.Action2;
import rx.functions.Func1;
import rx.subjects.BehaviorSubject;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * A factory mock to easily handle and assert state transitions in created endpoints
 * without having to repeat this in every test.
 *
 * @author Michael Nitschinger
 * @since 1.4.2
 */
public class EndpointFactoryMock implements Service.EndpointFactory {

    private static final String DEFAULT_HOST = "localhost";
    private static final String DEFAULT_BUCKET = "bucket";
    private static final String DEFAULT_PASSWORD = "password";
    private static final int DEFAULT_PORT = 1234;

    private final String hostname;
    private final String bucket;
    private final String password;
    private final int port;
    private final CoreEnvironment env;
    private final RingBuffer<ResponseEvent> responseBuffer;
    private final List<Endpoint> endpoints;
    private final List<BehaviorSubject<LifecycleState>> endpointStates;
    private final List<Action2<Endpoint, BehaviorSubject<LifecycleState>>> createActions;

    EndpointFactoryMock(String hostname, String bucket, String password, int port, CoreEnvironment env, RingBuffer<ResponseEvent> responseBuffer) {
        this.hostname = hostname;
        this.bucket = bucket;
        this.password = password;
        this.port = port;
        this.env = env;
        this.responseBuffer = responseBuffer;
        this.endpoints = Collections.synchronizedList(new ArrayList<Endpoint>());
        this.endpointStates = Collections.synchronizedList(new ArrayList<BehaviorSubject<LifecycleState>>());
        this.createActions = Collections.synchronizedList(
            new ArrayList<Action2<Endpoint, BehaviorSubject<LifecycleState>>>()
        );
    }

    public static EndpointFactoryMock simple(CoreEnvironment env, RingBuffer<ResponseEvent> rb) {
        return new EndpointFactoryMock(DEFAULT_HOST, DEFAULT_BUCKET, DEFAULT_PASSWORD, DEFAULT_PORT, env, rb);
    }

    public void onCreate(final Action2<Endpoint, BehaviorSubject<LifecycleState>> createAction) {
        this.createActions.add(createAction);
    }

    public void onConnectTransition(final Func1<Endpoint, LifecycleState> action) {
        onCreate(new Action2<Endpoint, BehaviorSubject<LifecycleState>>() {
            @Override
            public void call(final Endpoint endpoint, final BehaviorSubject<LifecycleState> states) {
                when(endpoint.connect()).then(new Answer<Observable<LifecycleState>>() {
                    @Override
                    public Observable<LifecycleState> answer(InvocationOnMock invocation) throws Throwable {
                        LifecycleState state = action.call(endpoint);
                        states.onNext(state);
                        return Observable.just(state);
                    }
                });
            }
        });
    }

    public void onDisconnectTransition(final Func1<Endpoint, LifecycleState> action) {
        onCreate(new Action2<Endpoint, BehaviorSubject<LifecycleState>>() {
            @Override
            public void call(final Endpoint endpoint, final BehaviorSubject<LifecycleState> states) {
                when(endpoint.disconnect()).then(new Answer<Observable<LifecycleState>>() {
                    @Override
                    public Observable<LifecycleState> answer(InvocationOnMock invocation) throws Throwable {
                        LifecycleState state = action.call(endpoint);
                        states.onNext(state);
                        return Observable.just(state);
                    }
                });
            }
        });
    }

    @Override
    public Endpoint create(String hostname, String bucket, String password, int port, CoreEnvironment env, RingBuffer<ResponseEvent> responseBuffer) {
        final BehaviorSubject<LifecycleState> state = BehaviorSubject.create(LifecycleState.DISCONNECTED);
        final Endpoint endpoint = mock(Endpoint.class);
        when(endpoint.states()).thenReturn(state);
        for (Action2<Endpoint, BehaviorSubject<LifecycleState>> action : createActions) {
            action.call(endpoint, state);
        }

        endpoints.add(endpoint);
        endpointStates.add(state);
        return endpoint;
    }

    public void advance(int idx, LifecycleState state) {
        endpointStates.get(idx).onNext(state);
    }

    public void advanceAll(LifecycleState state) {
        for (int i = 0; i < endpointStates.size(); i++) {
            advance(i, state);
        }
    }

    public int endpointConnectCalled() {
        return invocationsFor("connect(");

    }

    public int endpointDisconnectCalled() {
        return invocationsFor("disconnect(");

    }

    public int endpointSendCalled() {
        return invocationsFor("send(");
    }

    private int invocationsFor(String contains) {
        int counts = 0;
        for (Endpoint ep : endpoints) {
            for (Invocation inv : Mockito.mockingDetails(ep).getInvocations()) {
                if (inv.toString().contains(contains)) {
                    counts++;
                }
            }
        }
        return counts;
    }

    public int endpointCount() {
        return endpoints.size();
    }

    public List<Endpoint> endpoints() {
        return endpoints;
    }

    public String getHostname() {
        return hostname;
    }

    public String getBucket() {
        return bucket;
    }

    public String getPassword() {
        return password;
    }

    public int getPort() {
        return port;
    }

    public CoreEnvironment getEnv() {
        return env;
    }

    public RingBuffer<ResponseEvent> getResponseBuffer() {
        return responseBuffer;
    }
}
