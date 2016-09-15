package io.scalecube.cluster.leaderelection;

import io.scalecube.cluster.ICluster;
import io.scalecube.transport.Message;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Created by ronenn on 9/13/2016.
 */
public class HeartbeatScheduler {

  private static final Logger LOGGER = LoggerFactory.getLogger(HeartbeatScheduler.class);
  private final ScheduledThreadPoolExecutor scheduler = new ScheduledThreadPoolExecutor(1);
  public ICluster cluster;
  long heartbeatInterval;

  public HeartbeatScheduler(ICluster cluster, long heartbeatInterval) {
    this.cluster = cluster;
    this.heartbeatInterval = heartbeatInterval;
  }

  public void schedule() {
    scheduler.scheduleAtFixedRate(new Runnable() {
      @Override
      public void run() {
        LOGGER.debug("Leader Node: {} maintain leadership spread {} gossip regards selected leader {}",
            cluster.address(), RaftProtocol.RAFT_PROTOCOL_HEARTBEAT, cluster.address());
        cluster.spreadGossip(
            Message.builder().qualifier(RaftProtocol.RAFT_PROTOCOL_HEARTBEAT).data(cluster.address()).build());
      }
    }, heartbeatInterval, heartbeatInterval, TimeUnit.SECONDS);
  }

  public void stop() {
    scheduler.getQueue().clear();
  }
}
