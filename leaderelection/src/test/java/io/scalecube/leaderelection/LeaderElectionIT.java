package io.scalecube.leaderelection;

import static org.junit.Assert.assertTrue;

import io.scalecube.cluster.Cluster;
import io.scalecube.cluster.ICluster;
import io.scalecube.cluster.Member;
import io.scalecube.transport.Address;

import com.google.common.base.Throwables;

import org.junit.Test;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class LeaderElectionIT {

  /**
   * planned tests to develop. ------------------------- Test case when you select leader then disable network from/to
   * this leader, check that in the rest of the cluster new leader elected, also what will happen with this current
   * leader? then enable network againfor this leader and check that this previous leader will agree on new one.
   * 
   * Split cluster on two partitions one is quorum and another isn't. Check that quorum part will select leader, what
   * will be with non quorum? then join cluster again and check that leader is stable.
   * 
   * Try to fail some mebers in the process of leader election and check that leader is still elected correctly Come up
   * with some more creative test cases
   * 
   */

  private static final int NUM_OF_NODES = 10;
  Executor exec = Executors.newCachedThreadPool();

  @Test
  public void testAllNodesFollowOneLeader() {
    ICluster seed = Cluster.joinAwait();
    LeaderElection seedLeader = createLeaderElection(seed);
    Queue<LeaderElection> leaders = createCluster(seed, NUM_OF_NODES);

    // waiting for a leader to be selected
    awaitSeconds(4);

    Member seedMember = leaders.peek().leader();

    for (LeaderElection m : leaders) {
      Member member = m.leader();
      assertTrue("Expected leader address " + seedMember.address() + ", but actual: " + member.address(),
          member.equals(seedMember));
    }

    teardownCluster(seed, leaders);
  }


  @Test
  public void testLeaderDiesAndNewLeaderIsElected() {
    ICluster seed = Cluster.joinAwait();
    LeaderElection seedLeader = createLeaderElection(seed);
    Queue<LeaderElection> leaders = createCluster(seed, NUM_OF_NODES);

    // waiting for a leader to be selected
    awaitSeconds(3);

    killTheLeader(leaders, seedLeader.leader());

    // waiting for a leader to be selected
    awaitSeconds(3);

    Member newLeader = leaders.peek().leader();

    for (LeaderElection m : leaders) {
      Address memberLeaderAddress = m.leader().address();
      assertTrue("Expected leader address " + newLeader.address() + ", but actual: " + memberLeaderAddress,
          memberLeaderAddress.equals(newLeader.address()));
    }

    teardownCluster(seed, leaders);
  }


  private void teardownCluster(ICluster seed, Queue<LeaderElection> leaders) {
    // shutdown everything
    for (LeaderElection m : leaders) {
      m.cluster().shutdown();
    }
    seed.shutdown();
  }


  private void killTheLeader(Queue<LeaderElection> leaders, Member leader) {
    LeaderElection rm = null;
    for (LeaderElection m : leaders) {
      if (leader.equals(m.cluster().address())) {
        m.cluster().shutdown();
        rm = m;
        break;
      }
    }
    leaders.remove(rm);
  }


  private Queue<LeaderElection> createCluster(ICluster seed, int numOfNodes) {
    Queue<LeaderElection> leaders = new ConcurrentLinkedQueue<LeaderElection>();
    ICluster member = Cluster.joinAwait(seed.address());
    leaders.add(createLeaderElection(member));

    return leaders;
  }


  private LeaderElection createLeaderElection(ICluster cluster) {
    LeaderElection el = RaftLeaderElection.builder(cluster)
        .build();
    return el;
  }

  private void awaitSeconds(int seconds) {
    try {
      TimeUnit.SECONDS.sleep(seconds);
    } catch (InterruptedException e) {
      Throwables.propagate(e);
    }
  }
}
