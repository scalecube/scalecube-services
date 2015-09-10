package io.servicefabric.examples.gossip;

import io.servicefabric.cluster.Cluster;
import io.servicefabric.cluster.ICluster;
import io.servicefabric.transport.protocol.Message;

import rx.functions.Action1;

/**
 * Basic example for member gossiping between cluster members. to run the example Start ClusterNodeA and cluster
 * ClusterNodeB A listen on gossip B spread gossip
 * 
 * @author ronen hamias
 *
 */
public class ClusterNodeA {

  public static void main(String[] args) {
    // start cluster node that listen on port 3000
    ICluster clusterA = Cluster.newInstance(3000).join();

    // subscribe to all gossip messages:
    clusterA.gossip().listen().subscribe(new Action1<Message>() {
      @Override
      public void call(Message gossip) {
        // print out the gossip message
        System.out.println("Gossip message:" + gossip);
      }
    });
  }
}
