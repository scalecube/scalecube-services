package io.scalecube.services.examples;

import org.consul.registry.integration.ConsulServiceDiscovery;

import io.scalecube.cluster.Cluster;
import io.scalecube.cluster.ClusterConfig;
import io.scalecube.examples.util.Util;
import io.scalecube.services.Microservices;
import io.scalecube.transport.TransportConfig;

public class ServiceProvisioningExample {

  public static void main(String[] args) throws InterruptedException {
    // Create microservices cluster instance on port 4040.
    Microservices microservices = Microservices.builder()
        .discovery(new ConsulServiceDiscovery())
        .cluster(Cluster.joinAwait(ClusterConfig.builder()
            .transportConfig(TransportConfig.builder()
                .port(4040)
                .build()).build())).build();

    // introduce a service instance by registering it.
    microservices.registry()
        .service(new HelloWorldComponent())
        .register();
    
    Util.sleepForever();
  }
}
