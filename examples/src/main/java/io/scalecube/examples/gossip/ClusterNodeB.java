package io.scalecube.examples.gossip;

import io.scalecube.cluster.Cluster;
import io.scalecube.cluster.ICluster;
import io.scalecube.examples.Greetings;
import io.scalecube.transport.Message;

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
