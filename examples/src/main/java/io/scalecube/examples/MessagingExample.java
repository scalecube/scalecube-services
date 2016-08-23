package io.scalecube.examples;

import io.scalecube.cluster.Cluster;
import io.scalecube.cluster.membership.MembershipRecord;
import io.scalecube.cluster.ICluster;
import io.scalecube.transport.Message;

import rx.functions.Action1;

/**
 * Basic example for member transport between cluster members to run the example Start ClusterNodeA and cluster
 * ClusterNodeB A listen on transport messages B send message to member A.
 * 
 * @author ronen hamias
 *
 */
public class MessagingExample {

  /**
   * Main method.
   */
  public static void main(String[] args) throws Exception {
    // Start cluster node A
    final ICluster clusterA = Cluster.joinAwait();

    // Listen to greetings messages and respond to them
    clusterA.listen().filter(Greetings.MSG_FILTER).subscribe(new Action1<Message>() {
      @Override
      public void call(Message request) {
        // Print greeting to console
        Greetings greetings = request.data();
        System.out.println(greetings);

        // Respond with greetings
        Message response = Message.fromData(new Greetings("Greetings from ClusterMember A"));
        clusterA.send(request.sender(), response);
      }
    });


    // Start cluster node B that joins node A as a seed node
    ICluster clusterB = Cluster.joinAwait(clusterA.address());

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
    Greetings greetings = new Greetings("Greetings from ClusterMember B");
    Message greetingsMessage = Message.fromData(greetings);
    for (MembershipRecord member : clusterB.otherMembers()) {
      clusterB.send(member, greetingsMessage);
    }
  }

}
