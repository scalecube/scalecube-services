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
    // Start cluster node Alice and subscribe on listening gossips
    ICluster alice = Cluster.joinAwait();
    alice.listenGossips().subscribe(gossip -> System.out.println("Alice heard gossip: " + gossip.data()));

    // Start cluster node B that joins node A as seed node
    ICluster bob = Cluster.joinAwait(alice.address());
    bob.spreadGossip(Message.fromData("Greetings from Bob"));
  }
}
