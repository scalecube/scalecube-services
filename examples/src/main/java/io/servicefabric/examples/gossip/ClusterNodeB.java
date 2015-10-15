package io.servicefabric.examples.gossip;

import io.servicefabric.cluster.Cluster;
import io.servicefabric.cluster.ICluster;
import io.servicefabric.examples.Greetings;
import io.servicefabric.transport.Message;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;

/**
 * Basic example for member gossiping between cluster members to run the example Start ClusterNodeA and cluster
 * ClusterNodeB A listen on gossip B spread gossip.
 * 
 * @author ronen hamias
 *
 */
public class ClusterNodeB {

  /**
   * Main method.
   */
  public static void main(String[] args) {
    // start cluster node that listen on port 3001 and point to node A as seed node
    Futures.addCallback(Cluster.newInstance(3001, "localhost:3000").join(), new FutureCallback<ICluster>() {
      @Override
      public void onSuccess(ICluster cluster) {
        cluster.gossip().spread(new Message(new Greetings("Greetings from ClusterMember B")));
      }

      @Override
      public void onFailure(Throwable throwable) {
        // do nothing
      }
    });
  }

}
