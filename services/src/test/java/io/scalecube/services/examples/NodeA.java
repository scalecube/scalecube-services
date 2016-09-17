package io.scalecube.services.examples;

import java.util.Random;

import io.scalecube.cluster.Cluster;
import io.scalecube.cluster.ClusterConfig;
import io.scalecube.cluster.ICluster;
import io.scalecube.services.ServiceFabric;
import io.scalecube.transport.TransportConfig;

/**
 * @author Anton Kharenko
 */
public class NodeA {

  public static void main(String[] args) throws InterruptedException {
    // Join cluster
    ClusterConfig config = ClusterConfig.builder().transportConfig(TransportConfig.builder().port(4000).build()).build();

    ICluster clusterA = Cluster.joinAwait(config);

    // Create service fabric instance
    ServiceFabric serviceFabricA = ServiceFabric.newInstance(clusterA);

    // Create service instance
    ExampleService exampleService = new ExampleServiceImpl();

    // Register service
    serviceFabricA.registerService(exampleService);

    // Wait for service requests to process
    Thread.sleep(30000);

    // Unregister service
    serviceFabricA.unregisterService(exampleService);
  }

}
