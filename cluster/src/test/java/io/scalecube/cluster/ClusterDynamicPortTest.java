package io.scalecube.cluster;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

public class ClusterDynamicPortTest {

  @Test
  public void testJoin() throws Exception {
    ICluster seedNode = Cluster.joinAwait();
    String seedNodeAddress = "localhost:" + ClusterConfig.DEFAULT_PORT;

    long start = System.currentTimeMillis();
    for (int i = 0; i < 10; i++) {
      Cluster.joinAwait(seedNodeAddress);
    }
    long end = System.currentTimeMillis();
    System.out.println("Time: " + (end - start) + " millis");
    assertEquals(11, seedNode.membership().members().size());
    System.out.println("Cluster nodes: " + seedNode.membership().members());
  }
}
