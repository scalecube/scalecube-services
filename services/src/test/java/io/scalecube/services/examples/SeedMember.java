package io.scalecube.services.examples;

import io.scalecube.cluster.Cluster;
import io.scalecube.cluster.ClusterConfig;
import io.scalecube.cluster.ICluster;
import io.scalecube.transport.TransportConfig;

public class SeedMember {

  public static void main(String[] args) {
  
    ClusterConfig cfg = ClusterConfig.builder().transportConfig(TransportConfig.builder().port(8000).build()).build();
    ICluster clusterA = Cluster.joinAwait(cfg);

 // Wait for service requests to process
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
