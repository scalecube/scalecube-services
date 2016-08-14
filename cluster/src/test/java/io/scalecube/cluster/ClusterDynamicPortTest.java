package io.scalecube.cluster;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import io.scalecube.transport.TransportConfig;

public class ClusterDynamicPortTest {

  @Test
  public void testJoin() throws Exception {
    ICluster seedNode = Cluster.joinAwait();
    String seedNodeAddress = seedNode.localAddress().toString();

    long start = System.currentTimeMillis();
    List<ICluster> otherNodes = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      otherNodes.add(Cluster.joinAwait(seedNodeAddress));
    }
    long end = System.currentTimeMillis();
    System.out.println("Time: " + (end - start) + " millis");
    assertEquals(11, seedNode.membership().members().size());
    System.out.println("Cluster nodes: " + seedNode.membership().members());

    // Destroy all nodes
    seedNode.leave().get();
    for (ICluster node : otherNodes) {
      node.leave().get();
    }
  }
}
