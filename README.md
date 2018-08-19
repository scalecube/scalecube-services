# scalecube-services
[![Build Status](https://travis-ci.org/scalecube/scalecube-services.svg?branch=develop)](https://travis-ci.org/scalecube/scalecube-services)
[![Codacy Badge](https://api.codacy.com/project/badge/Grade/5e2ee9be41f7425590313ee1b8f737d7)](https://app.codacy.com/app/ScaleCube/scalecube-services?utm_source=github.com&utm_medium=referral&utm_content=scalecube/scalecube-services&utm_campaign=badger)
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/io.scalecube/scalecube-services-api/badge.svg)](https://maven-badges.herokuapp.com/maven-central/io.scalecube/scalecube-services-api)

## MICROSERVICES 2.0

<table text-align="top">
 <tr>
   <td>
    An open-source project that is focused on streamlining reactive-programming of Microservices Reactive-systems that scale, built by developers for developers.<br><br>
ScaleCube Services provides a low latency Reactive Microservices library for peer-to-peer service registry and discovery 
based on gossip protocol, without single point-of-failure or bottlenecks.<br><br>
    Scalecube more gracefully address the cross cutting concernes of distributed microservices architecture.
    <br><br>
  </td>
  <td>
  <img src="https://user-images.githubusercontent.com/1706296/43058327-b4a0147e-8e4f-11e8-9999-68c4ec99632e.gif">
  </td>
</tr>
</table>
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

The example provisions 2 cluster nodes and making a remote interaction. 
1. seed is a member node and provision no services of its own. 
2. then microservices variable is a member that joins seed member and provision GreetingService instance.
3. finally from seed node - create a proxy by the GreetingService api and send a greeting request. 

```java
    //1. ScaleCube Node node with no members
    Microservices seed = Microservices.builder().startAwait();

    //2. Construct a ScaleCube node which joins the cluster hosting the Greeting Service
    Microservices microservices = Microservices.builder()
        .seeds(seed.cluster().address()) // some address so its possible to join the cluster.
        .services(new GreetingServiceImpl())
        .startAwait();


    //3. Create service proxy
    GreetingsService service = seed.call().create().api(GreetingsService.class);

    // Execute the services and subscribe to service events
    service.sayHello("joe").subscribe(consumer -> {
      System.out.println(consumer.message());
    });
```

Basic Service Example:


* RequestOne: Send single request and expect single reply
* RequestStream: Send single request and expect stream of responses. 
* RequestBidirectional: send stream of requests and expect stream of responses.

A service is nothing but an interface declaring what methods we wish to provision at our cluster.

```java

@Service
public interface ExampleService {

  @ServiceMethod
  Mono<String> sayHello(String request);

  @ServiceMethod
  Flux<MyResponse> helloStream();
  
  @ServiceMethod
  Flux<MyResponse> helloBidirectional(Flux<MyRequest> requests);
}

```




### Maven

With scalecube-services you may plug-and-play alternative providers for Transport,Codecs and discovery. 
Scalecube is using ServiceLoader to load providers from class path, 
  
You can think about scalecube as slf4j for microservices - Currently supported SPIs: 

**Transport providers:**

* rsocket-services-transport: using rsocket to communicate with remote services.

**Message codec providers:**

* scalecube-services-jackson: using Jackson to encode / decode service messages. https://github.com/FasterXML
* scalecube-services-protostuff: using protostuff to encode / decode service messages. https://github.com/protostuff
 
**Service discovery providers:**

* scalecube-services-discovery: using scalecue-cluster do locate service Endpoint within the cluster
   https://github.com/scalecube/scalecube-cluster
    

Binaries and dependency information for Maven can be found at http://search.maven.org.

https://mvnrepository.com/artifact/io.scalecube

[![Maven Central](https://maven-badges.herokuapp.com/maven-central/io.scalecube/scalecube-services-api/badge.svg)](https://maven-badges.herokuapp.com/maven-central/io.scalecube/scalecube-services-api)

To add a dependency on ScaleCube Services using Maven, use the following:

```xml

 <!-- -------------------------------------------
   scalecube core and api:   
 ------------------------------------------- -->

 <!-- scalecube apis   -->
 <dependency>
  <groupId>io.scalecube</groupId>
  <artifactId>scalecube-services-api</artifactId>
  <version>2.x.x</version>
 </dependency>
 
 <!-- scalecube services module   -->
 <dependency>
  <groupId>io.scalecube</groupId>
  <artifactId>scalecube-services</artifactId>
  <version>2.x.x</version>
 </dependency>
 

 <!--

     Plugins / SPIs: bellow a list of providers you may choose from. to constract your own configuration:
     you are welcome to build/contribute your own plugins please consider the existing ones as example.

  -->

 <!-- -------------------------------------------
   scalecube transport providers:   
 ------------------------------------------- -->
 <dependency>
  <groupId>io.scalecube</groupId>
  <artifactId>rsocket-services-transport</artifactId>
  <version>2.x.x</version>
 </dependency>
 
 <!-- -------------------------------------------
   scalecube message serialization providers:
   ------------------------------------------- -->

 <!-- jackson scalecube messages codec -->
 <dependency>
  <groupId>io.scalecube</groupId>
  <artifactId>scalecube-services-jackson</artifactId>
  <version>2.x.x</version>
 </dependency>

<!-- protostuff scalecube messages codec -->
 <dependency>
  <groupId>io.scalecube</groupId>
  <artifactId>scalecube-services-protostuff</artifactId>
  <version>2.x.x</version>
 </dependency>

 <!-- -------------------------------------------
    scalecube service discovery provider   
   ------------------------------------------- -->
 <dependency>
  <groupId>io.scalecube</groupId>
  <artifactId>scalecube-services-discovery</artifactId>
  <version>2.x.x</version>
 </dependency>


```
