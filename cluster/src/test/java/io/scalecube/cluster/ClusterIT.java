package io.scalecube.cluster;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class ClusterIT {

  @Test
  public void testJoinDynamicPort() throws Exception {
    // Start seed node
    ICluster seedNode = Cluster.joinAwait();
    String seedNodeAddress = seedNode.localAddress().toString();

    // Start other nodes
    int membersNum = 10;
    long start = System.currentTimeMillis();
    List<ICluster> otherNodes = new ArrayList<>();
    for (int i = 0; i < membersNum; i++) {
      otherNodes.add(Cluster.joinAwait(seedNodeAddress));
    }
    long end = System.currentTimeMillis();
    System.out.println("Time: " + (end - start) + " millis");
    assertEquals(membersNum + 1, seedNode.membership().members().size());
    System.out.println("Cluster nodes: " + seedNode.membership().members());

    // Shutdown all nodes
    seedNode.shutdown().get();
    for (ICluster node : otherNodes) {
      node.shutdown().get();
    }
  }
}
