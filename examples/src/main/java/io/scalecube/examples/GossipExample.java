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
    // start cluster node that listen on port 3000
    ICluster clusterA = Cluster.joinAwait(3000);
    clusterA.listenGossips().subscribe(new Action1<Message>() {
      @Override
      public void call(Message gossip) {
        System.out.println("A: Received gossip message: " + gossip);
      }
    });

    // start cluster node that listen on port 3001 and point to node A as seed node
    ICluster clusterB = Cluster.joinAwait(3001, "localhost:3000");
    clusterB.spreadGossip(Message.fromData(new Greetings("Greetings from ClusterMember B")));
  }
}
