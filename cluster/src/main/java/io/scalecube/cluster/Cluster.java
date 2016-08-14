package io.scalecube.cluster;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.util.concurrent.Futures.transform;

import io.scalecube.cluster.fdetector.FailureDetector;
import io.scalecube.cluster.gossip.GossipProtocol;
import io.scalecube.cluster.gossip.IGossipProtocol;
import io.scalecube.transport.Address;
import io.scalecube.transport.Message;
import io.scalecube.transport.Transport;

import com.google.common.base.Function;
import com.google.common.base.Throwables;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import rx.Observable;

import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nullable;

/**
 * Main ICluster implementation.
 * 
 * @author Anton Kharenko
 */
public final class Cluster implements ICluster {

  private static final Logger LOGGER = LoggerFactory.getLogger(Cluster.class);

  private final ClusterConfig config;
  private final String memberId;

  // Cluster components
  private Transport transport;
  private FailureDetector failureDetector;
  private GossipProtocol gossipProtocol;
  private ClusterMembership clusterMembership;

  private Cluster(ClusterConfig config) {
    checkNotNull(config);
    checkNotNull(config.transportConfig);
    checkNotNull(config.gossipProtocolConfig);
    checkNotNull(config.failureDetectorConfig);
    checkNotNull(config.membershipConfig);
    this.config = config;
    this.memberId = UUID.randomUUID().toString();
    LOGGER.info("Cluster instance '{}' created with configuration: {}", memberId, config);
  }

  public static ICluster joinAwait() {
    return joinAwait(ClusterConfig.newInstance());
  }

  public static ICluster joinAwait(String seedMembers) {
    return joinAwait(ClusterConfig.newInstance().seedMembers(seedMembers));
  }

  /**
   * Init cluster instance with the given configuration and join cluster synchronously.
   */
  public static ICluster joinAwait(ClusterConfig config) {
    try {
      return join(config).get();
    } catch (Exception e) {
      throw Throwables.propagate(Throwables.getRootCause(e));
    }
  }

  public static ListenableFuture<ICluster> join() {
    return join(ClusterConfig.newInstance());
  }

  public static ListenableFuture<ICluster> join(String seedMembers) {
    return join(ClusterConfig.newInstance().seedMembers(seedMembers));
  }

  public static ListenableFuture<ICluster> join(final ClusterConfig config) {
    return new Cluster(config).join0();
  }

  private ListenableFuture<ICluster> join0() {
    LOGGER.info("Cluster instance '{}' joining seed members: {}", memberId, config.seedMembers);
    ListenableFuture<Transport> transportFuture = Transport.bind(config.transportConfig);
    ListenableFuture<Void> clusterFuture = transform(transportFuture, new AsyncFunction<Transport, Void>() {
      @Override
      public ListenableFuture<Void> apply(@Nullable Transport input) throws Exception {
        // Init transport component
        transport = input;

        // Init gossip protocol component
        gossipProtocol = new GossipProtocol(memberId, transport, config.gossipProtocolConfig);

        // Init failure detector component
        failureDetector = new FailureDetector(transport, config.failureDetectorConfig);

        // Init cluster membership component
        clusterMembership = new ClusterMembership(memberId, transport);
        clusterMembership.setFailureDetector(failureDetector);
        clusterMembership.setGossipProtocol(gossipProtocol);
        clusterMembership.setLocalMetadata(config.metadata);
        clusterMembership.setSeedMembers(config.seedMembers);
        clusterMembership.setSyncTime(config.membershipConfig.getSyncTime());
        clusterMembership.setSyncTimeout(config.membershipConfig.getSyncTimeout());
        clusterMembership.setSyncGroup(config.membershipConfig.getSyncGroup());
        clusterMembership.setMaxSuspectTime(config.membershipConfig.getMaxSuspectTime());
        clusterMembership.setMaxShutdownTime(config.membershipConfig.getMaxShutdownTime());

        // Start components
        failureDetector.start();
        gossipProtocol.start();
        return clusterMembership.start();
      }
    });
    return transform(clusterFuture, new Function<Void, ICluster>() {
      @Override
      public ICluster apply(@Nullable Void param) {
        LOGGER.info("Cluster instance '{}' joined cluster of members: {}", memberId, clusterMembership.members());
        return Cluster.this;
      }
    });
  }

  @Override
  public Address localAddress() {
    return transport.address();
  }

  @Override
  public void send(ClusterMember member, Message message) {
    transport.send(member.address(), message);
  }

  @Override
  public void send(ClusterMember member, Message message, SettableFuture<Void> promise) {
    transport.send(member.address(), message, promise);
  }

  @Override
  public Observable<Message> listen() {
    return transport.listen();
  }

  @Override
  public void spreadGossip(Message message) {
    gossipProtocol.spread(message);
  }

  @Override
  public Observable<Message> listenGossips() {
    return gossipProtocol.listen();
  }

  @Override
  public IClusterMembership membership() {
    return clusterMembership;
  }

  @Override
  public ListenableFuture<Void> leave() {
    LOGGER.info("Cluster instance '{}' leaving cluster", memberId);

    // Notify cluster members about graceful shutdown of current member
    clusterMembership.leave();

    // Wait for some time until 'leave' gossip start to spread through the cluster before stopping cluster components
    final SettableFuture<Void> transportStoppedFuture = SettableFuture.create();
    final ScheduledExecutorService stopExecutor = Executors.newSingleThreadScheduledExecutor();
    long delay = 3 * config.gossipProtocolConfig.getGossipTime(); // wait for 3 gossip periods before stopping
    stopExecutor.schedule(new Runnable() {
      @Override
      public void run() {
        clusterMembership.stop();
        gossipProtocol.stop();
        failureDetector.stop();
        transport.stop(transportStoppedFuture);
      }
    }, delay, TimeUnit.MILLISECONDS);

    // Update cluster state to terminal state
    return transform(transportStoppedFuture, new Function<Void, Void>() {
      @Nullable
      @Override
      public Void apply(Void input) {
        stopExecutor.shutdown();
        LOGGER.info("Cluster instance '{}' stopped", memberId);
        return input;
      }
    });
  }

}
