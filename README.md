# Project Status

This is an alpha version of the project and it is **not** stable or production ready as for now. The project is based on 
the experience gained from the development of similar systems over the years. The decision was made to make a graceful 
roll-out on a per module basis at early stage of completion in order to get 
[feedback](https://github.com/servicefabric/servicefabric/issues) as soon as possible.    

# Service Fabric I/O

[![Build Status](https://travis-ci.org/servicefabric/servicefabric.svg?branch=master)](https://travis-ci.org/servicefabric/servicefabric)

Service Fabric I/O is a microservices framework for a rapid development of a distributed, resilient, reactive 
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
	<version>0.0.3</version>
</dependency>
<dependency>
	<groupId>io.servicefabric</groupId>
	<artifactId>servicefabric-cluster</artifactId>
	<version>0.0.3</version>
</dependency>
```

## Links

* [Web Site](http://servicefabric.io/)

## Bugs and Feedback

For bugs, questions and discussions please use the [GitHub Issues](https://github.com/servicefabric/servicefabric/issues).
