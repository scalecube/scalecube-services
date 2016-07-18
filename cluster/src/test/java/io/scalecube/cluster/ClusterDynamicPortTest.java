package io.scalecube.cluster;

import static org.junit.Assert.fail;
import static org.junit.Assert.assertEquals;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;


public class ClusterDynamicPortTest {

  @Test
  public void testJoin() throws Exception {
    ICluster seedNode = Cluster.joinAwait();
    String seedNodeAddress = "localhost:" + ClusterConfig.DEFAULT_PORT;

    List<ListenableFuture<ICluster>> nodes = new ArrayList<>();
    long start = System.currentTimeMillis();
    for (int i = 0; i < 10; i++) {
      nodes.add(Cluster.join(seedNodeAddress));
    }
    ListenableFuture<List<ICluster>> result = Futures.allAsList(nodes);
    try {
      result.get();
    } catch (InterruptedException | ExecutionException e) {
      fail();
    }
    long end = System.currentTimeMillis();
    System.out.println("Time: " + (end - start) + " millis");
    assertEquals(11, seedNode.membership().members().size());
    System.out.println("Cluster nodes: " + seedNode.membership().members());
  }
}
