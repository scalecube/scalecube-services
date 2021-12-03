# We Hire at exberry.io
https://exberry.io/career/

# website
https://scalecube.github.io/

# scalecube-services
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/io.scalecube/scalecube-services-api/badge.svg)](https://maven-badges.herokuapp.com/maven-central/io.scalecube/scalecube-services-api)
[![SourceSpy Dashboard](https://sourcespy.com/shield.svg)](https://sourcespy.com/github/scalecubescalecubeservices/)

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

* Provision and interconnect microservices peers in a cluster
* Fully Distributed with No single-point-of-failure or single-point-of-bottleneck
* Fast - Low latency and high throughput
* Scaleable over- cores, jvms, clusters, regions.
* Built-in Service Discovery and service routing
* Zero configuration, automatic peer-to-peer service discovery using SWIM cluster membership protocol
* Simple non-blocking, asynchronous programming model
* Reactive Streams support.
  * Fire And Forget - Send and not wait for a reply
  * Request Response - Send single request and expect single reply
  * Request Stream - Send single request and expect stream of responses.
  * Request bidirectional - send stream of requests and expect stream of responses.
* Built-in failure detection, fault tolerance, and elasticity
* Routing and balancing strategies for both stateless and stateful services
* Embeddable into existing applications
* Natural Circuit-Breaker via scalecube-cluster discovery and failure detector.
* Support Service instance tagging.
* Support Service discovery partitioning using hierarchy of namespaces in a multi-cluster deployments.
* Modular, flexible deployment models and topology
* pluggable api-gateway providers (http / websocket / rsocket)
* pluggable service transports (tcp / aeron / rsocket)
* pluggable encoders (json, SBE, Google protocol buffers)
* pluggable service security authentication and authorization providers. 

User Guide:

* [Services Overview](http://scalecube.github.io/services.html)
* [Defining Services](http://scalecube.github.io/user-reference/services/DefineService.html)
* [Implementing services](http://scalecube.github.io/user-reference/services/ServiceImplementation.html)
* [Provisioning Clustered Services](http://scalecube.github.io/user-reference/services/ProvisionClusterServices.html)
* [Consuming services](http://scalecube.github.io/user-reference/services/ConsumingServices.html)


Basic Usage:

The example provisions 2 cluster nodes and making a remote interaction.
1. seed is a member node and provision no services of its own.
2. then microservices variable is a member that joins seed member and provision GreetingService instance.
3. finally from seed node - create a proxy by the GreetingService api and send a greeting request.

```java
// service definition
@Service("io.scalecube.Greetings")
public interface GreetingsService {
  @ServiceMethod("sayHello")
	  Mono<Greeting> sayHello(String name);
	}
}
// service implementation
public class GreetingServiceImpl implements GreetingsService {
 @Override
 public Mono<Greeting> sayHello(String name) {
   return Mono.just(new Greeting("Nice to meet you " + name + " and welcome to ScaleCube"));
	}
}

//1. ScaleCube Node node with no members (container 1)
Microservices seed = Microservices.builder()
  .discovery("seed", ScalecubeServiceDiscovery::new)
	.transport(RSocketServiceTransport::new)
	.startAwait();

// get the address of the seed member - will be used to join any other members to the cluster.
final Address seedAddress = seed.discovery("seed").address();

//2. Construct a ScaleCube node which joins the cluster hosting the Greeting Service (container 2)
Microservices serviceNode = Microservices.builder()
  .discovery("seed", ep -> new ScalecubeServiceDiscovery(ep)
		.membership(cfg -> cfg.seedMembers(seedAddress)))
	.transport(RSocketServiceTransport::new)
	.services(new GreetingServiceImpl())
	.startAwait();

//3. Create service proxy (can be created from any node or container in the cluster)
//   and Execute the service and subscribe to incoming service events
seed.call().api(GreetingsService.class)
  .sayHello("joe").subscribe(consumer -> {
    System.out.println(consumer.message());
  });

// await all instances to shutdown.
Mono.whenDelayError(seed.shutdown(), serviceNode.shutdown()).block(); 
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

## API-Gateway:

Available api-gateways are [rsocket](/services-gateway-rsocket), [http](/services-gateway-http) and [websocket](/services-gateway-websocket)

Basic API-Gateway example:

```java

    Microservices.builder()
        .discovery(options -> options.seeds(seed.discovery().address()))
        .services(...) // OPTIONAL: services (if any) as part of this node.

        // configure list of gateways plugins exposing the apis
        .gateway(options -> new WebsocketGateway(options.id("ws").port(8080)))
        .gateway(options -> new HttpGateway(options.id("http").port(7070)))
        .gateway(options -> new RSocketGateway(options.id("rsws").port(9090)))

        .startAwait();

        // HINT: you can try connect using the api sandbox to these ports to try the api.
        // https://scalecube.github.io/api-sandbox/app/index.html
```

### Maven

With scalecube-services you may plug-and-play alternative providers for Transport,Codecs and discovery.
Scalecube is using ServiceLoader to load providers from class path,

You can think about scalecube as slf4j for microservices - Currently supported SPIs:

**Transport providers:**

* scalecube-services-transport-rsocket: using rsocket to communicate with remote services.

**Message codec providers:**

* scalecube-services-transport-jackson: using Jackson to encode / decode service messages. https://github.com/FasterXML
* scalecube-services-transport-protostuff: using protostuff to encode / decode service messages. https://github.com/protostuff

**Service discovery providers:**

* scalecube-services-discovery: using scalecue-cluster do locate service Endpoint within the cluster
   https://github.com/scalecube/scalecube-cluster


Binaries and dependency information for Maven can be found at http://search.maven.org.

https://mvnrepository.com/artifact/io.scalecube

To add a dependency on ScaleCube Services using Maven, use the following:

[![Maven Central](https://maven-badges.herokuapp.com/maven-central/io.scalecube/scalecube-services-api/badge.svg)](https://maven-badges.herokuapp.com/maven-central/io.scalecube/scalecube-services-api)

```xml

 <properties>
   <scalecube.version>2.x.x</scalecube.version>
 </properties>

 <!-- -------------------------------------------
   scalecube core and api:
 ------------------------------------------- -->

 <!-- scalecube apis   -->
 <dependency>
  <groupId>io.scalecube</groupId>
  <artifactId>scalecube-services-api</artifactId>
  <version>${scalecube.version}</version>
 </dependency>

 <!-- scalecube services module   -->
 <dependency>
  <groupId>io.scalecube</groupId>
  <artifactId>scalecube-services</artifactId>
  <version>${scalecube.version}</version>
 </dependency>


 <!--

     Plugins / SPIs: bellow a list of providers you may choose from. to constract your own configuration:
     you are welcome to build/contribute your own plugins please consider the existing ones as example.

  -->

 <!-- scalecube transport providers:  -->
 <dependency>
  <groupId>io.scalecube</groupId>
  <artifactId>scalecube-services-transport-rsocket</artifactId>
  <version>${scalecube.version}</version>
 </dependency>
```

----

## Sponsored by: 
* [OM2](https://www.om2.com/)
* [exberry.io](https://exberry.io/)
