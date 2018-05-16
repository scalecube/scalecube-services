# scalecube-services
[![Build Status](https://travis-ci.org/scalecube/scalecube-services.svg?branch=develop)](https://travis-ci.org/scalecube/scalecube-services)
[![Codacy Badge](https://api.codacy.com/project/badge/Grade/5e2ee9be41f7425590313ee1b8f737d7)](https://app.codacy.com/app/ScaleCube/scalecube-services?utm_source=github.com&utm_medium=referral&utm_content=scalecube/scalecube-services&utm_campaign=badger)
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/io.scalecube/scalecube-services-api/badge.svg)](https://maven-badges.herokuapp.com/maven-central/io.scalecube/scalecube-services-api)

## MICROSERVICES 2.0

ScaleCube Services provides a low latency Reactive Microservices library for peer-to-peer service registry and discovery 
based on gossip protocol ad without single point-of-failure or bottlenecks.

ScaleCube Services Features:

* Provision and interconnect microservices as a service-mesh (cluster)</li>
* Reactive Streams support.
  * Fire And Forget - Send and not wait for a reply
  * Request Response - Send single request and expect single reply
  * Request Stream - Send single request and expect stream of responses. 
  * Request bidirectional - send stream of requests and expect stream of responses.
* No single-point-of-failure or single-point-of-bottleneck
* Cluster aware and distributed
* Modular, flexible deployment models and topology
* Zero configuration, automatic peer-to-peer service discovery using gossip
* Simple non-blocking, asynchronous programming model
* Resilient due to failure detection, fault tolerance, and elasticity
* Routing and balancing strategies for both stateless and stateful services
* Low latency and high throughput
* Takes advantage of the JVM and scales over available cores
* Embeddable to existing Java applications
* Natural Circuit-Breaker due to tight integration with scalecube-cluster failure detector.
* Support Service instance tagging. 
* pluggable service transport
* pluggable encoders 

User Guide:

* [Services Overview](http://scalecube.io/services.html)
* [Defining Services](http://scalecube.io/user-reference/services/DefineService.html)
* [Implementing services](http://scalecube.io/user-reference/services/ServiceImplementation.html)
* [Provisioning Clustered Services](http://scalecube.io/user-reference/services/ProvisionClusterServices.html)
* [Consuming services](http://scalecube.io/user-reference/services/ConsumingServices.html)


Basic Usage:

```java
Microservices ms = Microservices.builder()
        .services(new GreetingServiceImpl())
        .build();
        
GreetingService service = ms.call().api(GreetingService.class);

service.sayHello("joe").subscribe(response->{
  System.out.println(response);
});
```

### Maven

Binaries and dependency information for Maven can be found at http://search.maven.org.

To add a dependency on ScaleCube Services using Maven, use the following:

```xml
<dependency>
  <groupId>io.scalecube</groupId>
  <artifactId>scalecube-services</artifactId>
  <version>2.x.x</version> 
</dependency>
```
