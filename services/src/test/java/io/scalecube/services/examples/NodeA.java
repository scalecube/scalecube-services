package io.scalecube.services.examples;

import java.util.Random;

import io.scalecube.cluster.Cluster;
import io.scalecube.cluster.ICluster;
import io.scalecube.services.ServiceFabric;

/**
 * @author Anton Kharenko
 */
public class NodeA {

  public static void main(String[] args) throws InterruptedException {
    // Join cluster
    int incarnationId = new Random().nextInt(Integer.MAX_VALUE);
    ICluster clusterA = Cluster.newInstance("A-" + incarnationId, 4001, "localhost:4002");
    clusterA.joinAwait();

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
