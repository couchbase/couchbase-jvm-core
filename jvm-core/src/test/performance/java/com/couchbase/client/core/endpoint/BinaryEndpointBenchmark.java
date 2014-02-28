package com.couchbase.client.core.endpoint;

import com.couchbase.client.core.endpoint.binary.BinaryEndpoint;
import com.couchbase.client.core.environment.CouchbaseEnvironment;
import com.couchbase.client.core.environment.Environment;
import com.couchbase.client.core.message.CouchbaseResponse;
import com.couchbase.client.core.message.binary.GetRequest;
import com.couchbase.client.core.message.binary.GetResponse;
import io.netty.util.ResourceLeakDetector;
import reactor.function.Consumer;

import java.net.InetSocketAddress;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class BinaryEndpointBenchmark {

    static {
    //    ResourceLeakDetector.setLevel(ResourceLeakDetector.Level.ADVANCED);
    }

    public static void main(String... args) throws Exception {

        Environment env = new CouchbaseEnvironment();
        Endpoint endpoint1 = new BinaryEndpoint(new InetSocketAddress(11210), env);
        endpoint1.connect().await();

       Endpoint endpoint2 = new BinaryEndpoint(new InetSocketAddress(11210), env);
        endpoint2.connect().await();

        Endpoint endpoint3 = new BinaryEndpoint(new InetSocketAddress(11210), env);
        endpoint3.connect().await();

        ExecutorService s = Executors.newCachedThreadPool();

       //Thread.sleep(TimeUnit.DAYS.toMillis(1));
       s.submit(new Task(endpoint1));
       s.submit(new Task(endpoint1));
       s.submit(new Task(endpoint1));

        s.submit(new Task(endpoint2));
        s.submit(new Task(endpoint2));
        s.submit(new Task(endpoint2));

        s.submit(new Task(endpoint3));
        s.submit(new Task(endpoint3));
        s.submit(new Task(endpoint3));
    }

    static class Task implements Runnable {
        Endpoint endpoint;
        public Task(Endpoint endpoint) {
            this.endpoint = endpoint;
        }
        @Override
        public void run() {
            while (true) {
                int count = new Random().nextInt(100);
                final CountDownLatch latch = new CountDownLatch(count);
                for (int i = 0; i < count; i++) {
                   endpoint.<GetResponse>send(new GetRequest("key" + i, "default", "")).consume(new Consumer<GetResponse>() {
                       @Override
                       public void accept(GetResponse r) {
                           latch.countDown();
                       }
                   });
                }
                try {
                    latch.await();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }
}
