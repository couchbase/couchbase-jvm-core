package com.couchbase.client.core.endpoint;

import com.couchbase.client.core.message.CouchbaseRequest;
import com.couchbase.client.core.message.CouchbaseResponse;
import com.couchbase.client.core.state.LifecycleState;
import com.couchbase.client.core.state.Stateful;
import reactor.core.composable.Promise;

public interface Endpoint extends Stateful<LifecycleState> {

    <R extends CouchbaseResponse> Promise<R> send(CouchbaseRequest request);

    Promise<LifecycleState> connect();

}
