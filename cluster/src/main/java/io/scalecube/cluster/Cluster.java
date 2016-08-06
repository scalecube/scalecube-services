package io.scalecube.cluster;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.util.concurrent.Futures.transform;

import io.scalecube.cluster.fdetector.FailureDetector;
import io.scalecube.cluster.gossip.GossipProtocol;
import io.scalecube.cluster.gossip.IGossipProtocol;
import io.scalecube.transport.Message;
import io.scalecube.transport.Transport;
import io.scalecube.transport.Address;
import io.scalecube.transport.AvailablePortFinder;

import com.google.common.base.Function;
import com.google.common.base.Throwables;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import rx.Observable;
import rx.schedulers.Schedulers;

import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import javax.annotation.Nullable;

/**
 * Main ICluster implementation.
 * 
 * @author Anton Kharenko
 */
public final class Cluster implements ICluster {

  private static final Logger LOGGER = LoggerFactory.getLogger(Cluster.class);

  private enum State {
    INSTANTIATED, JOINING, JOINED, LEAVING, STOPPED
  }

  // Cluster config
  private final ClusterConfig config;

  // Member
  private final String memberId;

  // Cluster components
  private final Transport transport;
  private final FailureDetector failureDetector;
  private final GossipProtocol gossipProtocol;
  private final ClusterMembership clusterMembership;

  // Cluster state
  private final AtomicReference<State> state;

  private Cluster(ClusterConfig config) {
    checkNotNull(config);
    checkNotNull(config.transportSettings);
    checkNotNull(config.gossipProtocolSettings);
    checkNotNull(config.failureDetectorSettings);
    checkNotNull(config.membershipSettings);
    this.config = config;

    // Build local member
    memberId = UUID.randomUUID().toString();
    Address localAddress = Address.createLocal(config.port);

    // Build transport
    transport = Transport.newInstance(localAddress, config.transportSettings);

    // Build gossip protocol component
    gossipProtocol = new GossipProtocol(memberId, transport, config.gossipProtocolSettings);

    // Build failure detector component
    failureDetector = new FailureDetector(transport, config.failureDetectorSettings);

    // Build cluster membership component
    clusterMembership = new ClusterMembership(memberId, transport);
    clusterMembership.setFailureDetector(failureDetector);
    clusterMembership.setGossipProtocol(gossipProtocol);
    clusterMembership.setLocalMetadata(config.metadata);
    clusterMembership.setSeedMembers(config.seedMembers);
    clusterMembership.setSyncTime(config.membershipSettings.getSyncTime());
    clusterMembership.setSyncTimeout(config.membershipSettings.getSyncTimeout());
    clusterMembership.setSyncGroup(config.membershipSettings.getSyncGroup());
    clusterMembership.setMaxSuspectTime(config.membershipSettings.getMaxSuspectTime());
    clusterMembership.setMaxShutdownTime(config.membershipSettings.getMaxShutdownTime());

    // Initial state
    this.state = new AtomicReference<>(State.INSTANTIATED);
    LOGGER.info("Cluster instance '{}' created with configuration: {}", memberId, config);
  }

  public static ICluster joinAwait() {
    return joinAwait(ClusterConfig.newInstance());
  }

  public static ICluster joinAwait(int port) {
    return joinAwait(ClusterConfig.newInstance().port(port));
  }

  public static ICluster joinAwait(int port, String seedMembers) {
    return joinAwait(ClusterConfig.newInstance().port(port).seedMembers(seedMembers));
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

  public static ListenableFuture<ICluster> join(int port) {
    return join(ClusterConfig.newInstance().port(port));
  }

  public static ListenableFuture<ICluster> join(int port, String seedMembers) {
    return join(ClusterConfig.newInstance().port(port).seedMembers(seedMembers));
  }

  public static ListenableFuture<ICluster> join(String seedMembers) {
    return join(ClusterConfig.newInstance().seedMembers(seedMembers));
  }

  public static ListenableFuture<ICluster> join(final ClusterConfig config) {
    if (config.portAutoIncrement) { // Find available port
      config.port = AvailablePortFinder.getNextAvailable(config.port, config.portCount);
    }
    return new Cluster(config).join0();
  }

  private ListenableFuture<ICluster> join0() {
    updateClusterState(State.INSTANTIATED, State.JOINING);
    LOGGER.info("Cluster instance '{}' joining seed members: {}", memberId, config.seedMembers);
    ListenableFuture<Void> transportFuture = transport.start();
    ListenableFuture<Void> clusterFuture = transform(transportFuture, new AsyncFunction<Void, Void>() {
      @Override
      public ListenableFuture<Void> apply(@Nullable Void param) throws Exception {
        failureDetector.start();
        gossipProtocol.start();
        return clusterMembership.start();
      }
    });
    return transform(clusterFuture, new Function<Void, ICluster>() {
      @Override
      public ICluster apply(@Nullable Void param) {
        updateClusterState(State.JOINING, State.JOINED);
        LOGGER.info("Cluster instance '{}' joined cluster of members: {}", memberId, membership().members());
        return Cluster.this;
      }
    });
  }

  @Override
  public void send(ClusterMember member, Message message) {
    checkJoinedState();
    transport.send(member.address(), message);
  }

  @Override
  public void send(ClusterMember member, Message message, SettableFuture<Void> promise) {
    checkJoinedState();
    transport.send(member.address(), message, promise);
  }

  @Override
  public Observable<Message> listen() {
    checkJoinedState();
    return transport.listen();
  }

  @Override
  public IGossipProtocol gossip() {
    checkJoinedState();
    return gossipProtocol;
  }

  @Override
  public IClusterMembership membership() {
    checkJoinedState();
    return clusterMembership;
  }

  @Override
  public ListenableFuture<Void> leave() {
    updateClusterState(State.JOINED, State.LEAVING);
    LOGGER.info("Cluster instance '{}' leaving cluster", memberId);

    // Notify cluster members about graceful shutdown of current member
    clusterMembership.leave();

    // Wait for some time until 'leave' gossip start to spread through the cluster before stopping cluster components
    final SettableFuture<Void> transportStoppedFuture = SettableFuture.create();
    final ScheduledExecutorService stopExecutor = Executors.newSingleThreadScheduledExecutor();
    long delay = 3 * config.gossipProtocolSettings.getGossipTime(); // wait for 3 gossip periods before stopping
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
        updateClusterState(State.LEAVING, State.STOPPED);
        LOGGER.info("Cluster instance '{}' stopped", memberId);
        return input;
      }
    });
  }

  private void checkJoinedState() {
    State currentState = state.get();
    checkState(currentState == State.JOINED, "Illegal operation at state %s. Member should be joined to cluster.",
        state.get());
  }

  private void updateClusterState(State expected, State update) {
    boolean stateUpdated = state.compareAndSet(expected, update);
    checkState(stateUpdated, "Illegal state transition from %s to %s cluster state. Expected state %s.", state.get(),
        update, expected);
  }

}
