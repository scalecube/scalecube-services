### MICROSERVICES v2.0

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

## Contributing
* Follow/Star us on github.
* Fork (and then git clone https://github.com/--your-username-here--/scalecube.git).
* Create a branch (git checkout -b branch_name).
* Commit your changes (git commit -am "Description of contribution").
* Push the branch (git push origin branch_name).
* Open a Pull Request.
* Thank you for your contribution! Wait for a response...

## References  
* Ronen Nachmias 

  [posts](https://www.linkedin.com/today/author/ronenhm?trk=pprof-feed)

## License

[Apache License, Version 2.0](https://github.com/scalecube/scalecube/blob/master/LICENSE.txt)