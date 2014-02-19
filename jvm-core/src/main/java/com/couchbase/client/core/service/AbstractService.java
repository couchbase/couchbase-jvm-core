package com.couchbase.client.core.service;

import com.couchbase.client.core.environment.Environment;
import com.couchbase.client.core.state.AbstractStateMachine;
import com.couchbase.client.core.state.LifecycleState;


public abstract class AbstractService extends AbstractStateMachine<LifecycleState> implements Service  {

    protected AbstractService(Environment env) {
        super(LifecycleState.DISCONNECTED, env);
    }
}
