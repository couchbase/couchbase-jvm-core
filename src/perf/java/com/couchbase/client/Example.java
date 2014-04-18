package com.couchbase.client;

import org.openjdk.jmh.annotations.GenerateMicroBenchmark;
import org.openjdk.jmh.logic.BlackHole;

public class Example {

    @GenerateMicroBenchmark
    public void sin(BlackHole hole) {
        hole.consume(Math.sin(300));
    }
}
