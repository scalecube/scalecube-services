package io.scalecube.examples.transport;

import io.scalecube.cluster.Cluster;
import io.scalecube.cluster.ClusterMember;
import io.scalecube.cluster.ICluster;
import io.scalecube.examples.Greetings;
import io.scalecube.transport.Message;

import java.util.List;

import rx.functions.Action1;

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
