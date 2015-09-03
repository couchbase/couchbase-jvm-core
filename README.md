# Couchbase JVM Core (core-io)
The Couchbase JVM Core module is the common library responsible for all the Couchbase Server
interaction. It is designed to be quite low level and only exposes a message-oriented API.

If you are not sure that this is the right project, you are probably looking for the
[java-client](https://github.com/couchbase/couchbase-java-client) instead. The `java-client` uses
the `core-io` package underneath, but exposes a nice Java API.

You can file bugs [here](https://issues.couchbase.com/browse/JVMCBC).

## Building
If you want to build the project, check out the sources and then run `mvn clean install`. This will
install the artifact into your local maven repository and can then be picked up by various
dependencies (like the `java-client`).

```
~/couchbase/couchbase-jvm-core $ mvn clean install
[INFO] Scanning for projects...
[INFO] Inspecting build with total of 1 modules...
[INFO] Installing Nexus Staging features:
[INFO]   ... total of 1 executions of maven-deploy-plugin replaced with nexus-staging-maven-plugin
[INFO]
[INFO] ------------------------------------------------------------------------
[INFO] Building Couchbase JVM Core IO 1.2.1-SNAPSHOT
[INFO] ------------------------------------------------------------------------
...
```

If you know the tests are building fine and you just want to deploy it, you can skip them with
`-Dmaven.test.skip=true`:

```
~/couchbase/couchbase-jvm-core $ mvn clean install -Dmaven.test.skip
[INFO] Scanning for projects...
[INFO] Inspecting build with total of 1 modules...
[INFO] Installing Nexus Staging features:
[INFO]   ... total of 1 executions of maven-deploy-plugin replaced with nexus-staging-maven-plugin
[INFO]
[INFO] ------------------------------------------------------------------------
[INFO] Building Couchbase JVM Core IO 1.2.1-SNAPSHOT
[INFO] ------------------------------------------------------------------------
[INFO]
[INFO] --- maven-clean-plugin:2.5:clean (default-clean) @ core-io ---
[INFO] Deleting /Users/michael/couchbase/couchbase-jvm-core/target
[INFO]
[INFO] --- maven-resources-plugin:2.6:resources (default-resources) @ core-io ---
[INFO] Using 'UTF-8' encoding to copy filtered resources.
[INFO] Copying 1 resource
[INFO]
[INFO] --- maven-compiler-plugin:3.3:compile (default-compile) @ core-io ---
[INFO] Changes detected - recompiling the module!
[INFO] Compiling 325 source files to /Users/michael/couchbase/couchbase-jvm-core/target/classes
[INFO] /Users/michael/couchbase/couchbase-jvm-core/src/main/java/com/couchbase/client/core/RequestHandler.java: Some input files use or override a deprecated API.
[INFO] /Users/michael/couchbase/couchbase-jvm-core/src/main/java/com/couchbase/client/core/RequestHandler.java: Recompile with -Xlint:deprecation for details.
[INFO] /Users/michael/couchbase/couchbase-jvm-core/src/main/java/com/couchbase/client/core/CouchbaseCore.java: Some input files use unchecked or unsafe operations.
[INFO] /Users/michael/couchbase/couchbase-jvm-core/src/main/java/com/couchbase/client/core/CouchbaseCore.java: Recompile with -Xlint:unchecked for details.
[INFO]
[INFO] --- build-helper-maven-plugin:1.9.1:add-test-source (add-integration-test-source-as-test-sources) @ core-io ---
[INFO] Test Source directory: /Users/michael/couchbase/couchbase-jvm-core/src/integration/java added.
[INFO]
[INFO] --- maven-resources-plugin:2.6:testResources (default-testResources) @ core-io ---
[INFO] Not copying test resources
[INFO]
[INFO] --- maven-compiler-plugin:3.3:testCompile (default-testCompile) @ core-io ---
[INFO] Not compiling test sources
[INFO]
[INFO] --- maven-surefire-plugin:2.12.4:test (default-test) @ core-io ---
[INFO] Tests are skipped.
[INFO]
[INFO] --- maven-jar-plugin:2.4:jar (default-jar) @ core-io ---
[INFO] Building jar: /Users/michael/couchbase/couchbase-jvm-core/target/core-io-1.2.1-SNAPSHOT.jar
[INFO]
[INFO] >>> maven-source-plugin:2.4:jar (attach-sources) > generate-sources @ core-io >>>
[INFO]
[INFO] <<< maven-source-plugin:2.4:jar (attach-sources) < generate-sources @ core-io <<<
[INFO]
[INFO] --- maven-source-plugin:2.4:jar (attach-sources) @ core-io ---
[INFO] Building jar: /Users/michael/couchbase/couchbase-jvm-core/target/core-io-1.2.1-SNAPSHOT-sources.jar
[INFO]
[INFO] --- maven-javadoc-plugin:2.10.3:jar (attach-javadocs) @ core-io ---
[INFO]
Loading source files for package com.couchbase.client.core.annotations...
Loading source files for package com.couchbase.client.core...
Loading source files for package com.couchbase.client.core.config...
Loading source files for package com.couchbase.client.core.config.loader...
Loading source files for package com.couchbase.client.core.config.parser..
...
Constructing Javadoc information...
Generating UML diagram /Users/michael/couchbase/couchbase-jvm-core/target/apidocs/com/couchbase/client/core/config/architecture.png
Generating UML diagram /Users/michael/couchbase/couchbase-jvm-core/target/apidocs/com/couchbase/client/core/state/transitions.png
Generating UML diagram /Users/michael/couchbase/couchbase-jvm-core/target/apidocs/com/couchbase/client/core/simple.png
Standard Doclet version 1.8.0_51
Building tree for all the packages and classes...
...
[INFO] Building jar: /Users/michael/couchbase/couchbase-jvm-core/target/core-io-1.2.1-SNAPSHOT-javadoc.jar
[INFO]
[INFO] --- maven-shade-plugin:2.4.1:shade (default) @ core-io ---
[INFO] Excluding io.reactivex:rxjava:jar:1.0.14 from the shaded jar.
[INFO] Including io.netty:netty-all:jar:4.0.30.Final in the shaded jar.
[INFO] Including com.lmax:disruptor:jar:3.3.2 in the shaded jar.
[INFO] Including com.fasterxml.jackson.core:jackson-databind:jar:2.6.1 in the shaded jar.
[INFO] Including com.fasterxml.jackson.core:jackson-annotations:jar:2.6.0 in the shaded jar.
[INFO] Including com.fasterxml.jackson.core:jackson-core:jar:2.6.1 in the shaded jar.
[INFO] Including org.latencyutils:LatencyUtils:jar:2.0.2 in the shaded jar.
[INFO] Including org.hdrhistogram:HdrHistogram:jar:2.1.0 in the shaded jar.
[INFO] Excluding org.slf4j:slf4j-api:jar:1.7.7 from the shaded jar.
[INFO] Excluding commons-logging:commons-logging:jar:1.1.3 from the shaded jar.
[INFO] Excluding log4j:log4j:jar:1.2.17 from the shaded jar.
[INFO] Excluding org.mockito:mockito-all:jar:1.10.19 from the shaded jar.
[INFO] Replacing original artifact with shaded artifact.
[INFO] Replacing /Users/michael/couchbase/couchbase-jvm-core/target/core-io-1.2.1-SNAPSHOT.jar with /Users/michael/couchbase/couchbase-jvm-core/target/core-io-1.2.1-SNAPSHOT-shaded.jar
[INFO] Replacing original source artifact with shaded source artifact.
[INFO] Replacing /Users/michael/couchbase/couchbase-jvm-core/target/core-io-1.2.1-SNAPSHOT-sources.jar with /Users/michael/couchbase/couchbase-jvm-core/target/core-io-1.2.1-SNAPSHOT-shaded-sources.jar
[INFO] Dependency-reduced POM written at: /Users/michael/couchbase/couchbase-jvm-core/dependency-reduced-pom.xml
[INFO] Dependency-reduced POM written at: /Users/michael/couchbase/couchbase-jvm-core/dependency-reduced-pom.xml
[INFO]
[INFO] --- maven-install-plugin:2.4:install (default-install) @ core-io ---
[INFO] Installing /Users/michael/couchbase/couchbase-jvm-core/target/core-io-1.2.1-SNAPSHOT.jar to /Users/michael/.m2/repository/com/couchbase/client/core-io/1.2.1-SNAPSHOT/core-io-1.2.1-SNAPSHOT.jar
[INFO] Installing /Users/michael/couchbase/couchbase-jvm-core/dependency-reduced-pom.xml to /Users/michael/.m2/repository/com/couchbase/client/core-io/1.2.1-SNAPSHOT/core-io-1.2.1-SNAPSHOT.pom
[INFO] Installing /Users/michael/couchbase/couchbase-jvm-core/target/core-io-1.2.1-SNAPSHOT-sources.jar to /Users/michael/.m2/repository/com/couchbase/client/core-io/1.2.1-SNAPSHOT/core-io-1.2.1-SNAPSHOT-sources.jar
[INFO] Installing /Users/michael/couchbase/couchbase-jvm-core/target/core-io-1.2.1-SNAPSHOT-javadoc.jar to /Users/michael/.m2/repository/com/couchbase/client/core-io/1.2.1-SNAPSHOT/core-io-1.2.1-SNAPSHOT-javadoc.jar
[INFO] Installing /Users/michael/couchbase/couchbase-jvm-core/target/core-io-1.2.1-SNAPSHOT-sources.jar to /Users/michael/.m2/repository/com/couchbase/client/core-io/1.2.1-SNAPSHOT/core-io-1.2.1-SNAPSHOT-sources.jar
[INFO] ------------------------------------------------------------------------
[INFO] BUILD SUCCESS
[INFO] ------------------------------------------------------------------------
[INFO] Total time: 23.220 s
[INFO] Finished at: 2015-09-03T14:12:38+02:00
[INFO] Final Memory: 44M/590M
[INFO] ------------------------------------------------------------------------

```

## Testing
You can run the tests through `mvn test`, which will run both integration and unit tests. If you
only want to run the unit tests from the command line, switch to the unit profile through 
`mvn test -Dunit`.

## Deploying
If you have the appropriate credentials to push to the `com.couchbase.client` namespace on maven
central, make sure that the version is properly set (no -SNAPSHOT), the commits are properly 
tagged (important!) and then run `mvn clean deploy`.

This will stage and close it, but not release it. Then head over to [sonatype](http://oss.sonatype.org)
do a final check and then release it.