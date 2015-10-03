# Project Status

This is an alpha version of the project and it is **not** stable or production ready as for now. 
Your [feedback](https://github.com/servicefabric/servicefabric/issues) is welcome.    

# Service Fabric I/O

[![Build Status](https://travis-ci.org/servicefabric/servicefabric.svg?branch=master)](https://travis-ci.org/servicefabric/servicefabric)

Service Fabric I/O is a microservices framework for the rapid development of distributed, resilient, reactive
applications that scales. It allows a set of distributed microservices to be connected in a way that resemble a fabric 
when viewed collectively. It greatly simplifies and streamlines asynchronous programming and provides a tool-set for 
managing [microservices architecture](http://microservices.io/patterns/index.html). Service Fabric has been designed 
carefully with the experiences earned over the years from the implementation of many online services and platforms. 
As a result, Service Fabric I/O has succeeded to find a way to achieve ease of development, performance, stability, 
and flexibility without a compromise.

The latest preview release of Transport and Cluster modules are available on Maven Central as

``` xml
<dependency>
	<groupId>io.servicefabric</groupId>
	<artifactId>servicefabric-transport</artifactId>
	<version>0.0.4</version>
</dependency>
<dependency>
	<groupId>io.servicefabric</groupId>
	<artifactId>servicefabric-cluster</artifactId>
	<version>0.0.4</version>
</dependency>
```

## Modules

TBD: Description

### Transport ([Code](https://github.com/servicefabric/servicefabric/blob/v0.0.4/transport/src/main/java/io/servicefabric/transport/ITransport.java))

``` java
public interface ITransport {
  TransportEndpoint localEndpoint();
  void start();
  void stop();
  void stop(SettableFuture<Void> promise);
  ListenableFuture<TransportEndpoint> connect(TransportAddress address);
  void disconnect(TransportEndpoint endpoint, SettableFuture<Void> promise);
  void send(TransportEndpoint endpoint, Message message);
  void send(TransportEndpoint endpoint, Message message, SettableFuture<Void> promise);
  Observable<Message> listen();
}
```

TBD: Description

### Cluster ([Code](https://github.com/servicefabric/servicefabric/blob/v0.0.4/cluster/src/main/java/io/servicefabric/cluster/ICluster.java))

``` java
public interface ICluster {
  void send(ClusterMember member, Message message);
  void send(ClusterMember member, Message message, SettableFuture<Void> promise);
  Observable<Message> listen();
  IGossipProtocol gossip();
  IClusterMembership membership();
  ICluster join();
  ListenableFuture<Void> leave();
}
```

TBD: Description

#### Membership ([Code](https://github.com/servicefabric/servicefabric/blob/v0.0.4/cluster/src/main/java/io/servicefabric/cluster/IClusterMembership.java))

``` java
public interface IClusterMembership {
  List<ClusterMember> members();
  ClusterMember member(String id);
  ClusterMember localMember();
  boolean isLocalMember(ClusterMember member);
  Observable<ClusterMember> listenUpdates();
}
```

TBD: Description

#### Failure Detector

TBD: Description

#### Gossip ([Code](https://github.com/servicefabric/servicefabric/blob/v0.0.4/cluster/src/main/java/io/servicefabric/cluster/gossip/IGossipProtocol.java))

``` java
public interface IGossipProtocol {
  void spread(Message message);
  Observable<Message> listen();
}
```

TBD: Description

### Services (Coming soon...) 

TBD: Description

### Gateway (Planned)

TBD: Description

## Links

* [Web Site](http://servicefabric.io/)

## Bugs and Feedback

For bugs, questions and discussions please use the [GitHub Issues](https://github.com/servicefabric/servicefabric/issues).
