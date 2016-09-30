package io.scalecube.services.examples;

import io.scalecube.cluster.Cluster;
import io.scalecube.cluster.ICluster;
import io.scalecube.services.Microservices;
import io.scalecube.transport.Address;


public class ServiceProvider {

  public static void main(String[] args) throws InterruptedException {
    // Join cluster

    ICluster clusterA = Cluster.joinAwait(Address.from("localhost:8000"));

    // Create service fabric instance
    Microservices microservices = Microservices.newInstance(clusterA);

    // Create service instance
    AdSelectorService service = new AdSelector();

    // Register service
    microservices.registerService(service);

    sleepForever();
  }

  private static void sleepForever() {
    while (true) {
      try {
        Thread.sleep(10000);
      } catch (InterruptedException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
    }
  }

}
