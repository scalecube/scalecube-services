package io.scalecube.examples;

import io.scalecube.cluster.Cluster;
import io.scalecube.cluster.ICluster;
import io.scalecube.transport.Message;

import rx.functions.Action1;

/**
 * Basic example for member gossiping between cluster members. to run the example Start ClusterNodeA and cluster
 * ClusterNodeB A listen on gossip B spread gossip
 * 
 * @author ronen hamias
 *
 */
public class GossipExample {

  /**
   * Main method.
   */
  public static void main(String[] args) {
    // Start cluster node A
    ICluster clusterA = Cluster.joinAwait();
    clusterA.listenGossips().subscribe(new Action1<Message>() {
      @Override
      public void call(Message gossip) {
        System.out.println("A: Received gossip message: " + gossip);
      }
    });

    // Start cluster node B that joins node A as seed node
    String seedMember = clusterA.address().toString();
    ICluster clusterB = Cluster.joinAwait(seedMember);
    clusterB.spreadGossip(Message.fromData(new Greetings("Greetings from ClusterMember B")));
  }
}
