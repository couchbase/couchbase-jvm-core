# Couchbase Core IO
The Couchbase JVM IO Core is the foundational module for the JVM SDKs. It implements all the common tasks without
imposing a specific language API. Language bindings like Java, Scala and so forth are implemented separately on top
of it.

Note that while properly documented and supported, the core package is not intended to be used directly by average
Couchbase user. Instead, please use the higher level language bindings and come back here if you need to implement
custom piece of software whose needs are not satisfied by the provided bindings.

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