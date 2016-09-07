package io.scalecube.examples;

import io.scalecube.cluster.Cluster;
import io.scalecube.cluster.Member;
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

  public static void main(String[] args) throws Exception {
    // Start cluster node Alice to listen and respond for incoming greeting messages
    ICluster alice = Cluster.joinAwait();
    alice.listen()
        .filter(msg -> "greeting".equals(msg.qualifier()))
        .subscribe(msg -> {
          System.out.println("Alice received greeting: " + msg.data());
          Message response = Message.withData("Greetings from Alice").qualifier("greeting").build();
          alice.send(msg.sender(), response);
        });

    // Join cluster node Bob to cluster with Alice
    ICluster bob = Cluster.joinAwait(alice.address());

    // Subscribe Bob to listen for incoming greeting messages and print them to system out
    bob.listen()
        .filter(msg -> "greeting".equals(msg.qualifier()))
        .map(msg -> "Bob received greeting: " + msg.data())
        .subscribe(System.out::println);

    // Send from Bob greeting message to all other cluster members (which is Alice)
    Message greetingMsg = Message.withData("Greetings from Bob").qualifier("greeting").build();
    for (Member member : bob.otherMembers()) {
      bob.send(member, greetingMsg);
    }

    // Avoid exit main
    Thread.yield();
  }

}
