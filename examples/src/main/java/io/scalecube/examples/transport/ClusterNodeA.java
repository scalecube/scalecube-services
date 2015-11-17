package io.scalecube.examples.transport;

import io.scalecube.cluster.Cluster;
import io.scalecube.cluster.ClusterMember;
import io.scalecube.cluster.ICluster;
import io.scalecube.examples.Greetings;
import io.scalecube.transport.Message;

import rx.functions.Action1;

/**
 * Basic example for member transport between cluster members to run the example Start ClusterNodeA and cluster
 * ClusterNodeB A listen on transport messages B send message to member A.
 * 
 * @author ronen hamias
 *
 */
public class ClusterNodeA {

  /**
   * Main method.
   */
  public static void main(String[] args) throws Exception {
    // Start cluster node that listen on port 3000
    final ICluster clusterA = Cluster.newInstance(3000).joinAwait();

    // Listen to greetings messages and respond to them
    clusterA.listen().filter(Greetings.MSG_FILTER).subscribe(new Action1<Message>() {
      @Override
      public void call(Message message) {
        // Print greeting to console
        Greetings greetings = message.data();
        System.out.println(greetings);

        // Respond with greetings
        ClusterMember senderMember = clusterA.membership().member(message.sender().id());
        clusterA.send(senderMember, new Message(new Greetings("Greetings from ClusterMember A")));
      }
    });
  }

}
