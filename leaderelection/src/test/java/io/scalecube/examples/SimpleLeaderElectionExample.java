package io.scalecube.examples;

import io.scalecube.cluster.Cluster;
import io.scalecube.leaderelection.LeadershipEvent;
import io.scalecube.leaderelection.RaftLeaderElection;

public class SimpleLeaderElectionExample {

  public static void main(String[] args) {

    RaftLeaderElection alice = RaftLeaderElection.builder(Cluster.joinAwait()).build();
    alice.listen().filter(le -> {
        return LeadershipEvent.Type.BECAME_LEADER.equals(le.type() );
      }).subscribe(le -> {
        System.out.println("Alice became leader.");
      });

    RaftLeaderElection bob = RaftLeaderElection.builder(
        Cluster.joinAwait(alice.cluster().address())).build();

    bob.listen().filter(le -> {
      return LeadershipEvent.Type.BECAME_LEADER.equals( le.type() );
    }).subscribe(le -> {
      System.out.println("Bob became leader.");
    });

    bob.listen().filter(le -> {
      return LeadershipEvent.Type.NEW_LEADER.equals( le.type() );
    }).subscribe(le -> {
      System.out.println("Bob follow leader:" + le.address());
    });
    
    avoidExit();
  }

  private static void avoidExit() {
    try {
      Thread.sleep(60000);
    } catch (InterruptedException e) {
    }
  }

}
