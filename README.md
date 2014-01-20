# Official Couchbase JVM Client
The code in this repository will eventually replace the current Couchbase Java SDK with the next generation of Couchbase
SDKs. Instead of sticking with Java as the main API language, the project is now split into multiple libraries:

 - A language-agnostic message-oriented core named "jvm-core". It contains everything that is needed to talk to a
   Couchbase Server cluster, but does not impose a specific API. Instead it is completely based on top of immutable
   messages and a very simple request/response flow.
 - On top of "jvm-core", there are a multitude of language-specific API implementations, including Java, Scala, Groovy
   and Clojure. That way, language specific APIs can be implemented that provide the best possible experience, while
   still being consistent in terms of functionality.

## Feature List

 - It is multi-bucket aware (per cluster).
 - Built on top of a completely reactive core (utilizing Spring's Reactor library)
 - The IO layer is consolidated and built on top of Netty, the best IO library on the JVM.
 - Super-Easy configuration management thanks to Typesafe's Config library.
 - Tuned for performance and developer productivity.

## Usage
This part is still in flux and may be heavily outdated. You've been warned!

### Connecting

```java
CouchbaseCluster cluster = new CouchbaseCluster();
Promise<ConnectResponse> response = (Promise<ConnectResponse>) cluster.send(new ConnectRequest());
response.onSuccess(

```

## Further Info
This README will be expanded as soon as there is more to show and play around with. Volunteers to try it out are
encouraged, please ping me (daschl) on github or twitter for this.