package com.couchbase.client.node;

public class DefaultServiceRegistryBenchmark {

    /*
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

    static abstract class DummyService extends AbstractService {

        protected DummyService(String hostname, Environment env, int numEndpoints, SelectionStrategy strategy) {
            super(hostname, env, numEndpoints, strategy);
        }

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
    */
}
