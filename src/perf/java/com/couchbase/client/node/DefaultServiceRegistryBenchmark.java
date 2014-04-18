package com.couchbase.client.node;

import com.couchbase.client.core.message.CouchbaseRequest;
import com.couchbase.client.core.node.DefaultServiceRegistry;
import com.couchbase.client.core.node.ServiceRegistry;
import com.couchbase.client.core.service.BucketServiceMapping;
import com.couchbase.client.core.service.Service;
import com.couchbase.client.core.service.ServiceType;
import com.couchbase.client.core.state.LifecycleState;
import org.openjdk.jmh.annotations.GenerateMicroBenchmark;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import rx.Observable;

public class DefaultServiceRegistryBenchmark {

    @State(Scope.Thread)
    public static class Input {
        public ServiceRegistry registry = new DefaultServiceRegistry();
    }

    @GenerateMicroBenchmark
    public void measureGlobalAddService(Input input) {
        input.registry.addService(new GlobalService(), null);
    }

    @GenerateMicroBenchmark
    public void measureLocalService(Input input) {
        input.registry.addService(new LocalService(), "bucket");
    }

    static abstract class DummyService implements Service {

        @Override
        public void send(CouchbaseRequest request) {

        }

        @Override
        public BucketServiceMapping mapping() {
            return type().mapping();
        }

        @Override
        public Observable<LifecycleState> connect() {
            return null;
        }

        @Override
        public Observable<LifecycleState> disconnect() {
            return null;
        }
    }

    static class GlobalService extends DummyService {
        @Override
        public ServiceType type() {
            return ServiceType.CONFIG;
        }
    }

    static class LocalService extends DummyService {
        @Override
        public ServiceType type() {
            return ServiceType.BINARY;
        }
    }
}
