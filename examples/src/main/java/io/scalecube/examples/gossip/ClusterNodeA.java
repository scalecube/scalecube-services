package io.scalecube.examples.gossip;

import io.scalecube.cluster.Cluster;
import io.scalecube.cluster.ICluster;
import io.scalecube.transport.Message;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;

import rx.functions.Action1;

/**
 * Basic example for member gossiping between cluster members. to run the example Start ClusterNodeA and cluster
 * ClusterNodeB A listen on gossip B spread gossip
 * 
 * @author ronen hamias
 *
 */
public class ClusterNodeA {

  /**
   * Main method.
   */
  public static void main(String[] args) {
    // start cluster node that listen on port 3000
    Futures.addCallback(Cluster.newInstance(3000).join(), new FutureCallback<ICluster>() {
      @Override
      public void onSuccess(ICluster cluster) {
        cluster.gossip().listen().subscribe(new Action1<Message>() {
          @Override
          public void call(Message gossip) {
            // print out the gossip message
            System.out.println("Gossip message:" + gossip);
          }
        });
      }

      @Override
      public void onFailure(Throwable throwable) {
        // do nothing
      }
    });

  }
}
