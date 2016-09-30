package io.scalecube.leaderelection;

import static org.junit.Assert.assertTrue;

import io.scalecube.cluster.Cluster;
import io.scalecube.cluster.ICluster;
import io.scalecube.transport.Address;

import com.google.common.base.Throwables;

import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class LeaderElectionIT {

  Executor exec = Executors.newCachedThreadPool();
  @Test
  public void testAllNodesFollowOneLeader() {
    ICluster seed = Cluster.joinAwait();
    LeaderElection seedLeader = createLeaderElection(seed);
    List<LeaderElection> leaders = createCluster(seed);

    // waiting for a leader to be selected
    awaitSeconds(30);

    Address seedLeaderAddress = seedLeader.leader();
    
    for (LeaderElection m : leaders) {
      Address memberLeaderAddress = m.leader();
      assertTrue("Expected leader address " + seedLeaderAddress + ", but actual: " + memberLeaderAddress,
          memberLeaderAddress.equals(seedLeaderAddress));
    }
    
    // shutdown everything
    for (LeaderElection m : leaders) {
      m.cluster().shutdown();
    }
    seed.shutdown();
  }

  
  @Test
  public void testLeaderDiesAndNewLeaderIsElected() {
    ICluster seed = Cluster.joinAwait();
    LeaderElection seedLeader = createLeaderElection(seed);
    List<LeaderElection> leaders = createCluster(seed);

    // waiting for a leader to be selected
    awaitSeconds(30);

    killTheLeader(leaders,seedLeader.leader());
    
    // waiting for a leader to be selected
    awaitSeconds(30);

    Address newLeader = seedLeader.leader();
    
    for (LeaderElection m : leaders) {
       Address memberLeaderAddress = m.leader();
       assertTrue("Expected leader address " + newLeader + ", but actual: " + memberLeaderAddress,
       memberLeaderAddress.equals(newLeader));
    }
    
    // shutdown everything
    for (LeaderElection m : leaders) {
      m.cluster().shutdown();
    }
    seed.shutdown();
  }

  
  private void killTheLeader(List<LeaderElection> leaders, Address leader) {
    LeaderElection rm = null;
    for (LeaderElection m : leaders) {
      if(leader.equals(m.cluster().address())) {
        m.cluster().shutdown();
        rm = m;
        break;
      }
    }
    leaders.remove(rm);
  }


  private List<LeaderElection> createCluster(ICluster seed) {
    List<LeaderElection> leaders = new ArrayList<>();
    for (int i = 0 ; i < 50; i++) {
      exec.execute(new Runnable() {
        @Override
        public void run() {
          ICluster member = Cluster.joinAwait(seed.address());
          leaders.add(createLeaderElection(member));  
        }
      });
    }
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
