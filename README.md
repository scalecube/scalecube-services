# ScaleCube

[![Build Status](https://travis-ci.org/scalecube/scalecube.svg?branch=master)](https://travis-ci.org/scalecube/scalecube)
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/io.scalecube/scalecube-cluster/badge.svg)](https://maven-badges.herokuapp.com/maven-central/io.scalecube/scalecube-cluster)
[![Codacy Badge](https://api.codacy.com/project/badge/Coverage/7a02aba38e5d4744ae3e3100a6b542a5)](https://www.codacy.com/app/ronenn/scalecube?utm_source=github.com&utm_medium=referral&utm_content=scalecube/scalecube&utm_campaign=Badge_Coverage)
[![Codacy Badge](https://api.codacy.com/project/badge/Grade/a162edec5ca347ef87db19320e41138a)](https://www.codacy.com/app/ScaleCube/scalecube?utm_source=github.com&amp;utm_medium=referral&amp;utm_content=scalecube/scalecube&amp;utm_campaign=Badge_Grade)
[![Join the chat at https://gitter.im/scalecube/Lobby](https://badges.gitter.im/Join%20Chat.svg)](https://gitter.im/scalecube/Lobby?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)
[![Twitter URL](https://img.shields.io/twitter/url/https/twitter.com/fold_left.svg?style=social&label=Follow%20%40ScaleCube)](https://twitter.com/scalecube)

## Welcome to scalecube! if you are new to scalecube:
[Please read ScaleCube-services Motivations and vision](https://github.com/scalecube/scalecube/wiki)

## Overview 
ScaleCube project provides the tools to develop, test and scale distributed components in a cluster with ease.
it provides a general purpose cluster-membership library based on [SWIM Membership protocol](http://www.cs.cornell.edu/~asdas/research/dsn02-swim.pdf) with gossip protocol as improvement. scalecube-cluster is Embeddable and can be used by any cluster-aware application that requires cluster membership, node discovery, failure-detection and gossip style of communication across member nodes. based on that unique properties scalecube implements aditional seperated module for microservices coordination library that features service discovery, fault-tolerance at scale.

The project focuses on ensuring that your application realizes the full potential of the [Reactive Manifesto](http://www.reactivemanifesto.org/), 
while delivering a high productivity development environment, and seamless production deployment experience.

Web Site: [http://scalecube.io](http://scalecube.io/)

## Features

ScaleCube is designed as an embeddable library for the Java VM. It is built in a modular way where each module independently 
implements a useful set of functionality and provides foundation for higher level features by abstracting away lower level concerns.

Next is described modules in the top to bottom order from the higher level features to the low level components.

### MICROSERVICES

ScaleCube Services provides a low latency Reactive Microservices library for peer-to-peer service registry and discovery 
based on gossip protocol ad without single point-of-failure or bottlenecks.

ScaleCube Services Features:

* Provision and interconnect microservices as a unified system (cluster)</li>
* Async RPC with java-8 CompleteableFutures support
* Reactive Streams support with RxJava.
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
* Message Driven based on google-protocol-buffers
* Natural Circuit-Breaker due to tight integration with scalecube-cluster failure detector.
* Support Service instance tagging. 

User Guide:

* [Services Overview](http://scalecube.io/services.html)
* [Defining Services](http://scalecube.io/user-reference/services/DefineService.html)
* [Implementing services](http://scalecube.io/user-reference/services/ServiceImplementation.html)
* [Provisioning Clustered Services](http://scalecube.io/user-reference/services/ProvisionClusterServices.html)
* [Consuming services](http://scalecube.io/user-reference/services/ConsumingServices.html)


### CLUSTER

ScaleCube Cluster is a lightweight decentralized cluster membership, failure detection, and gossip protocol library. 
It provides an implementation of [SWIM](http://www.cs.cornell.edu/~asdas/research/dsn02-swim.pdf) cluster membership protocol for Java VM.
It is an efficient and scalable weakly-consistent distributed group membership protocol based on gossip-style communication between the 
nodes in the cluster. Read my [blog post](http://www.antonkharenko.com/2015/09/swim-distributed-group-membership.html) with distilled 
notes on the SWIM paper for more details.

It is using a [random-probing failure detection algorithm](http://www.antonkharenko.com/2015/08/scalable-and-efficient-distributed.html) which provides 
a uniform expected network load at all members. 
The worst-case network load is linear O(n) for overall network produced by running algorithm on all nodes and constant network 
load at one particular member independent from the size of the cluster.

ScaleCube Cluster implements all improvements described at original SWIM algorithm paper, such as gossip-style dissemination, suspicion mechanism 
and time-bounded strong completeness of failure detector algorithm. In addition to that we have introduced support of additional SYNC mechanism 
in order to improve recovery of the cluster from network partitioning events.
  
Using ScaleCube Cluster as simple as few lines of code:
 
``` java
// Start cluster node Alice as a seed node of the cluster, listen and print all incoming messages
Cluster alice = Cluster.joinAwait();
alice.listen().subscribe(msg -> System.out.println("Alice received: " + msg.data()));

// Join cluster node Bob to cluster with Alice, listen and print all incoming messages
Cluster bob = Cluster.joinAwait(alice.address());
bob.listen().subscribe(msg -> System.out.println("Bob received: " + msg.data()));

// Join cluster node Carol to cluster with Alice (and Bob which is resolved via Alice)
Cluster carol = Cluster.joinAwait(alice.address());

// Send from Carol greeting message to all other cluster members (which is Alice and Bob)
carol.otherMembers().forEach(member -> carol.send(member, Message.fromData("Greetings from Carol")));
```

You are welcome to explore javadoc documentation on cluster API and examples module for more advanced use cases.

### TRANSPORT

ScaleCube Transport is a network communication layer which provides high throughput and low latency peer-to-peer messaging. 
It is based on [Netty](http://netty.io/) asynchronous networking framework and is using [RxJava](https://github.com/ReactiveX/RxJava) 
in order to provide convenient reactive API on top of network handlers pipelines.

Using ScaleCube Transport as simple as few lines of code:

``` java
// Bind first transport to port 5000
TransportConfig config1 = TransportConfig.builder().port(5000).build();
Transport transport1 = Transport.bindAwait(config1);

// Make first transport to listen and print all incoming messages
transport1.listen().subscribe(System.out::println);

// Get 'host:port' address of the first transport
Address address1 = transport1.address(); 

// Bind second transport on available port and send message to the first transport
Transport transport2 = Transport.bindAwait();
transport2.send(address1, Message.fromData("Hello World"));
```

You are welcome to explore javadoc documentation on transport API for more advanced use cases.

## Support

For improvement requests, bugs and discussions please use the [GitHub Issues](https://github.com/scalecube/scalecube/issues) 
or chat with us to get support on [Gitter](https://gitter.im/scalecube/Lobby).

You are more then welcome to join us or just show your support by granting us a small star :)

## Maven

Binaries and dependency information for Maven can be found at 
[http://search.maven.org](http://search.maven.org/#search%7Cga%7C1%7Cio.scalecube.scalecube).

To add a dependency on ScaleCube Services using Maven, use the following:

``` xml
<dependency>
  <groupId>io.scalecube</groupId>
  <artifactId>scalecube-services</artifactId>
  <version>x.y.z</version> 
</dependency>
```

To add a dependency on ScaleCube Cluster using Maven, use the following:

``` xml
<dependency>
  <groupId>io.scalecube</groupId>
  <artifactId>scalecube-cluster</artifactId>
  <version>x.y.z</version>
</dependency>
```

To add a dependency on ScaleCube Transport using Maven, use the following:

``` xml
<dependency>
  <groupId>io.scalecube</groupId>
  <artifactId>scalecube-transport</artifactId>
  <version>x.y.z</version>
</dependency>
```

## Contributing
* Follow/Star us on github.
* Fork (and then git clone https://github.com/--your-username-here--/scalecube.git).
* Create a branch (git checkout -b branch_name).
* Commit your changes (git commit -am "Description of contribution").
* Push the branch (git push origin branch_name).
* Open a Pull Request.
* Thank you for your contribution! Wait for a response...

## References
* Anton Kharenko

  [blog](http://www.antonkharenko.com/)
  
* Ronen Nachmias 

  [posts](https://www.linkedin.com/today/author/ronenhm?trk=pprof-feed)

## License

[Apache License, Version 2.0](https://github.com/scalecube/scalecube/blob/master/LICENSE.txt)
