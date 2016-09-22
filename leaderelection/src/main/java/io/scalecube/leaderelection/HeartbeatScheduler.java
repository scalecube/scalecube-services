package io.scalecube.leaderelection;

import io.scalecube.cluster.ICluster;
import io.scalecube.transport.Message;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * heartbeat scheduler broadcast heartbeats on the cluster to maintain leadership, members in the cluster expects this
 * heartbeat. in case heartbeat does not reach members within X time leader election process is initiated.
 * 
 * @author Ronen Nachmias
 */

public class HeartbeatScheduler {

  private static final Logger LOGGER = LoggerFactory.getLogger(HeartbeatScheduler.class);
  private ScheduledExecutorService scheduler;
  private final ICluster cluster;
  private final long heartbeatInterval;

  public HeartbeatScheduler(ICluster cluster, long heartbeatInterval) {
    this.cluster = cluster;
    this.heartbeatInterval = heartbeatInterval;
  }

  /**
   * when becoming a leader the leader schedule heatbeats and maintain leadership followers are expecting this heartbeat
   * in case the heartbeat will not arrive in X time they will assume no leader.
   */
  public void schedule() {
    String nameFormat = "sc-leaderelection-heartbeat" + cluster.address().toString();
    scheduler = Executors.newSingleThreadScheduledExecutor(
        new ThreadFactoryBuilder().setNameFormat(nameFormat).setDaemon(true).build());

    scheduler.scheduleAtFixedRate(this::spreadHeartbeat, heartbeatInterval, heartbeatInterval, TimeUnit.MILLISECONDS);
  }

  private void spreadHeartbeat() {
    LOGGER.debug("Leader Node: {} maintain leadership spread {} gossip regards selected leader {}",
        cluster.address(), RaftLeaderElection.HEARTBEAT, cluster.address());

    cluster.spreadGossip(Message.builder()
        .qualifier(RaftLeaderElection.HEARTBEAT)
        .data(cluster.address())
        .build());
  }

  /**
   * stop sending heartbeats - it actually means this member is no longer a leader.
   */
  public void stop() {
    if (scheduler != null) {
      scheduler.shutdown();
    }
    scheduler = null;
  }
}
