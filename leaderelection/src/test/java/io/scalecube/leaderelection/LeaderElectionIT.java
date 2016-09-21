package io.scalecube.leaderelection;

import static org.junit.Assert.assertTrue;

import io.scalecube.cluster.Cluster;
import io.scalecube.cluster.ICluster;
import io.scalecube.transport.Address;

import com.google.common.base.Throwables;

import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class LeaderElectionIT {

  private static final int NUM_OF_NODES = 10;
  Executor exec = Executors.newCachedThreadPool();

  @Test
  public void testAllNodesFollowOneLeader() {
    ICluster seed = Cluster.joinAwait();
    LeaderElection seedLeader = createLeaderElection(seed);
    Queue<LeaderElection> leaders = createCluster(seed, NUM_OF_NODES);

    // waiting for a leader to be selected
    awaitSeconds(1);

    Address seedLeaderAddress = leaders.peek().leader();

    for (LeaderElection m : leaders) {
      Address memberLeaderAddress = m.leader();
      assertTrue("Expected leader address " + seedLeaderAddress + ", but actual: " + memberLeaderAddress,
          memberLeaderAddress.equals(seedLeaderAddress));
    }

    teardownCluster(seed, leaders);
  }


  @Test
  public void testLeaderDiesAndNewLeaderIsElected() {
    ICluster seed = Cluster.joinAwait();
    LeaderElection seedLeader = createLeaderElection(seed);
    Queue<LeaderElection> leaders = createCluster(seed, NUM_OF_NODES);

    // waiting for a leader to be selected
    awaitSeconds(1);

    killTheLeader(leaders, seedLeader.leader());

    // waiting for a leader to be selected
    awaitSeconds(1);

    Address newLeader = leaders.peek().leader();

    for (LeaderElection m : leaders) {
      Address memberLeaderAddress = m.leader();
      assertTrue("Expected leader address " + newLeader + ", but actual: " + memberLeaderAddress,
          memberLeaderAddress.equals(newLeader));
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


  private void killTheLeader(Queue<LeaderElection> leaders, Address leader) {
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
    for (int i = 0; i < numOfNodes; i++) {
      exec.execute(new Runnable() {
        @Override
        public void run() {
          ICluster member = Cluster.joinAwait(seed.address());
          leaders.add(createLeaderElection(member));
        }
      });
      awaitSeconds(2);
    }
    return leaders;
  }


  private LeaderElection createLeaderElection(ICluster cluster) {
    LeaderElection el = RaftLeaderElection.builder(cluster)
        .leadershipTimeout(7)
        .heartbeatInterval(1)
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
