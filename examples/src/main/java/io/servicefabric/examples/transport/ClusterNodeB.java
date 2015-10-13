package io.servicefabric.examples.transport;

import io.servicefabric.cluster.Cluster;
import io.servicefabric.cluster.ClusterMember;
import io.servicefabric.cluster.ICluster;
import io.servicefabric.examples.Greetings;
import io.servicefabric.transport.Message;

import rx.functions.Action1;

import java.util.List;

/**
 * Basic example for member transport between cluster members to run the example Start ClusterNodeA and cluster
 * ClusterNodeB A listen on transport messages B send message to member A.
 * 
 * @author ronen hamias
 *
 */
public class ClusterNodeB {

  /**
   * Main method.
   */
  public static void main(String[] args) throws Exception {
    // Start cluster node that listen on port 3001 and point to node A as seed node
    ICluster clusterB = Cluster.newInstance(3001, "localhost:3000").joinAwait();

    // Listen for incoming greeting messages
    clusterB.listen().filter(Greetings.MSG_FILTER).subscribe(new Action1<Message>() {
      @Override
      public void call(Message message) {
        // Print greeting to console
        Greetings greetings = message.data();
        System.out.println(greetings);
      }
    });

    // Send greeting message to other cluster members
    List<ClusterMember> members = clusterB.membership().members();
    Greetings greetings = new Greetings("Greetings from ClusterMember B");
    Message greetingsMessage = new Message(greetings);
    for (ClusterMember member : members) {
      if (!clusterB.membership().isLocalMember(member)) {
        clusterB.send(member, greetingsMessage);
      }
    }
  }

}
