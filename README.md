# ScaleCube
ScalueCube, the art of scaling, in microservice architecture scalecube is a strategy in which components can scale on X,Y,Z axis. ScaleCube project provides the tools to develop, test and scale microservice components in distributed manner with ease.

The project focuses on ensuring that your application realises the full potential of the [Reactive Manifesto](http://www.reactivemanifesto.org/), while delivering a high productivity development environment, and seamless production deployment experience.

[![Build Status](https://travis-ci.org/scalecube/scalecube.svg?branch=master)](https://travis-ci.org/scalecube/scalecube)
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/io.scalecube/scalecube-cluster/badge.svg)](https://maven-badges.herokuapp.com/maven-central/io.scalecube/scalecube-cluster)

## Features

### MICROSERVICES
ScaleCube provides a low latency Reactive Microservices library peer-to-peer service registry and discovery based on gossip protocol no single point-of-failure or bottlenecks.

User Guide:
* [Services Overview] (http://scalecube.io/services.html)
* [Defining Services] (http://scalecube.io/user-reference/services/DefineService.html)
* [Implementing services] (http://scalecube.io/user-reference/services/ServiceImplementation.html)
* [Provisioning Clustered Services] (http://scalecube.io/user-reference/services/ProvisionClusterServices.html)
* [Consuming services] (http://scalecube.io/user-reference/services/ConsumingServices.html)

### CLUSTER
Cluster provides a fault-tolerant decentralized peer-to-peer based cluster membership service with no single point of failure or single point of bottleneck. It does this using gossip protocol and an scalable and efficient failure detection algorithm.

### TRANSPORT
The Transport module is communication layer of nodes and service. its main goal is to deal with managing message exchange

Web Site: [http://scalecube.io](http://scalecube.io/)

## Project Status

You are more then welcome to join us. Your [feedback](https://github.com/scalecube/scalecube/issues) is welcome.
or just show your support by granting us a small star :)

## Support
Chat with us or get support: https://gitter.im/scalecube/Lobby


## Maven

Binaries and dependency information for Maven can be found at 
[http://search.maven.org](http://search.maven.org/#search%7Cga%7C1%7Cio.scalecube.scalecube).

To add a dependency on ScaleCube cluster using Maven, use the following:

``` xml
<dependency>
  <groupId>io.scalecube</groupId>
  <artifactId>scalecube-cluster</artifactId>
  <version>x.y.z</version>
</dependency>
```

To add a dependency on ScaleCube services using Maven, use the following:

``` xml
<dependency>
  <groupId>io.scalecube</groupId>
  <artifactId>scalecube-services</artifactId>
  <version>x.y.z</version> 
</dependency>
```

## Bugs and Feedback

For bugs, questions and discussions please use the [GitHub Issues](https://github.com/scalecube/scalecube/issues).

## License

[Apache License, Version 2.0](https://github.com/scalecube/scalecube/blob/master/LICENSE.txt)
