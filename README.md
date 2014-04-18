# Couchbase JVM Core

The Couchbase JVM Core is the foundation module for the next generation of Couchbase JVM SDKs. It implements all the
groundwork without imposing a specific language API. Language bindings like Java, Scala and so forth are implemented
separately.

Note that while properly documented and supported, the core package is not intended to be used directly by general
Couchbase adopters. Instead, use the higher level language bindings and come back here if you need to implement custom
piece of software which needs are not satisfied by the provided bindings.

## Introduction

## Getting Started

## Usage

## Contributing

### Running the Tests
The test suite is separated into unit, integration and performance tests. Each of those sets can and should be run
individually, depending on the type of testing needed. While unit and integration tests can be run from both the
command line and the IDE, it is recommend to run the performance tests from the command line only.

### Unit Tests
Unit tests do not need a Couchbase Server reachable, and they should complete within a very short time. They are
located under the `src/test` namespace and can be run directly from the IDE or through the `gradle test` command line:

```
~/couchbase-jvm-core $ ./gradlew test
...
:test

BUILD SUCCESSFUL
```

## Integration Tests
Those tests interact with Couchbase Server instances and therefore need to be configured as such. If you do not want
to change anything special, make sure you at least have one running on `localhost`. Then use the `gradle integrationTest`
command:

```
~/couchbase-jvm-core $ ./gradlew integrationTest
...
:integrationTest

BUILD SUCCESSFUL
```

## Performance Tests
The project uses JMH to build self-contained performance jars which can be run and distributed individually. To create
such a jar, use the `gradle benchmarks` command:

```
~/couchbase-jvm-core $ ./gradlew benchmarks
...
:benchmarks

BUILD SUCCESSFUL
```

This creates a jar in the `build/distributions` directory which can be executed with JMH params. If you do not supply
params, all test will be run with the default settings. See the [JMH](http://openjdk.java.net/projects/code-tools/jmh/)
documentation for more details.

```
~/couchbase-jvm-core $ java -jar build/distributions/couchbase-jvm-core-0.1-SNAPSHOT-benchmarks.jar -i 2 -wi 2 -f 1
# Run progress: 0.00% complete, ETA 00:00:04
# VM invoker: /Library/Java/JavaVirtualMachines/jdk1.7.0_45.jdk/Contents/Home/jre/bin/java
# VM options: <none>
# Fork: 1 of 1
# Warmup: 2 iterations, 1 s each
# Measurement: 2 iterations, 1 s each
# Threads: 1 thread, will synchronize iterations
# Benchmark mode: Throughput, ops/time
# Benchmark: com.couchbase.client.Example.sin
# Warmup Iteration   1: 21730.326 ops/ms
# Warmup Iteration   2: 21556.037 ops/ms
Iteration   1: 21670.531 ops/ms
Iteration   2: 20004.101 ops/ms

Run result: 20837.32 (<= 2 iterations)


Benchmark             Mode   Samples         Mean   Mean error    Units
c.c.c.Example.sin    thrpt         2    20837.316          NaN   ops/ms
```