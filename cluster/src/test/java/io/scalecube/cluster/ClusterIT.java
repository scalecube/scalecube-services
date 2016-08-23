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

    // Start other nodes
    int membersNum = 10;
    long start = System.currentTimeMillis();
    List<ICluster> otherNodes = new ArrayList<>();
    for (int i = 0; i < membersNum; i++) {
      otherNodes.add(Cluster.joinAwait(seedNode.address()));
    }
    long end = System.currentTimeMillis();
    System.out.println("Time: " + (end - start) + " millis");
    assertEquals(membersNum + 1, seedNode.members().size());
    System.out.println("Cluster nodes: " + seedNode.members());

    // Shutdown all nodes
    seedNode.shutdown().get();
    for (ICluster node : otherNodes) {
      node.shutdown().get();
    }
  }
}
