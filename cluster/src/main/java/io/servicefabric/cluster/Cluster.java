package io.servicefabric.cluster;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

import io.servicefabric.cluster.fdetector.FailureDetector;
import io.servicefabric.cluster.gossip.GossipProtocol;
import io.servicefabric.cluster.gossip.IGossipProtocol;
import io.servicefabric.transport.Transport;
import io.servicefabric.transport.TransportAddress;
import io.servicefabric.transport.TransportEndpoint;
import io.servicefabric.transport.TransportMessage;
import io.servicefabric.transport.protocol.Message;

import com.google.common.base.Function;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;

import rx.Observable;
import rx.functions.Func1;
import rx.schedulers.Schedulers;

import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import javax.annotation.Nullable;

/**
 * @author Anton Kharenko
 */
public final class Cluster implements ICluster {

  private enum State {
    INSTANTIATED, JOINING, JOINED, LEAVING, STOPPED
  }

  // Cluster components
  private final Transport transport;
  private final FailureDetector failureDetector;
  private final GossipProtocol gossipProtocol;
  private final ClusterMembership clusterMembership;

  // Cluster state
  private final AtomicReference<State> state;

  private Cluster(Transport transport, FailureDetector failureDetector, GossipProtocol gossipProtocol,
      ClusterMembership clusterMembership) {
    this.transport = transport;
    this.failureDetector = failureDetector;
    this.gossipProtocol = gossipProtocol;
    this.clusterMembership = clusterMembership;

    // Initial state
    this.state = new AtomicReference<>(State.INSTANTIATED);
  }

  public static Cluster newInstance() {
    return newInstance(ClusterConfiguration.newInstance());
  }

  public static Cluster newInstance(int port) {
    return newInstance(ClusterConfiguration.newInstance().port(port));
  }

  public static Cluster newInstance(int port, String seedMembers) {
    return newInstance(ClusterConfiguration.newInstance().port(port).seedMembers(seedMembers));
  }

  public static Cluster newInstance(String memberId, int port, String seedMembers) {
    return newInstance(ClusterConfiguration.newInstance().memberId(memberId).port(port).seedMembers(seedMembers));
  }

  public static Cluster newInstance(ClusterConfiguration config) {
    checkNotNull(config);
    checkNotNull(config.transportSettings);
    checkNotNull(config.gossipProtocolSettings);
    checkNotNull(config.failureDetectorSettings);
    checkNotNull(config.clusterMembershipSettings);

    // Build local endpoint
    String memberId = config.memberId != null ? config.memberId : UUID.randomUUID().toString();
    TransportEndpoint localTransportEndpoint = TransportEndpoint.from(memberId, TransportAddress.localTcp(config.port));

    // Build transport
    Transport transport = Transport.newInstance(localTransportEndpoint, config.transportSettings);

    // Build gossip protocol component
    GossipProtocol gossipProtocol = new GossipProtocol(localTransportEndpoint);
    gossipProtocol.setTransport(transport);
    gossipProtocol.setMaxGossipSent(config.gossipProtocolSettings.getMaxGossipSent());
    gossipProtocol.setGossipTime(config.gossipProtocolSettings.getGossipTime());
    gossipProtocol.setMaxEndpointsToSelect(config.gossipProtocolSettings.getMaxEndpointsToSelect());

    // Build failure detector component
    FailureDetector failureDetector =
        new FailureDetector(localTransportEndpoint, Schedulers.from(transport.getEventExecutor()));
    failureDetector.setTransport(transport);
    failureDetector.setPingTime(config.failureDetectorSettings.getPingTime());
    failureDetector.setPingTimeout(config.failureDetectorSettings.getPingTimeout());
    failureDetector.setMaxEndpointsToSelect(config.failureDetectorSettings.getMaxEndpointsToSelect());

    // Build cluster membership component
    ClusterMembership clusterMembership =
        new ClusterMembership(localTransportEndpoint, Schedulers.from(transport.getEventExecutor()));
    clusterMembership.setFailureDetector(failureDetector);
    clusterMembership.setGossipProtocol(gossipProtocol);
    clusterMembership.setTransport(transport);
    clusterMembership.setLocalMetadata(config.metadata);
    clusterMembership.setSeedMembers(config.seedMembers);
    clusterMembership.setSyncTime(config.clusterMembershipSettings.getSyncTime());
    clusterMembership.setSyncTimeout(config.clusterMembershipSettings.getSyncTimeout());
    clusterMembership.setMaxSuspectTime(config.clusterMembershipSettings.getMaxSuspectTime());
    clusterMembership.setMaxShutdownTime(config.clusterMembershipSettings.getMaxShutdownTime());
    clusterMembership.setSyncGroup(config.clusterMembershipSettings.getSyncGroup());

    // Build cluster component
    Cluster cluster = new Cluster(transport, failureDetector, gossipProtocol, clusterMembership);

    return cluster;
  }

  @Override
  public void send(ClusterMember member, Message message) {
    checkJoinedState();
    transport.send(member.endpoint(), message);
  }

  @Override
  public void send(ClusterMember member, Message message, SettableFuture<Void> promise) {
    checkJoinedState();
    transport.send(member.endpoint(), message, promise);
  }

  @Override
  public Observable<ClusterMessage> listen() {
    checkJoinedState();
    return transport.listen().map(new Func1<TransportMessage, ClusterMessage>() {
      @Override
      public ClusterMessage call(TransportMessage transportMessage) {
        // FIXME [AK]: Member can be null and ClusterMessage constructor will throw IllegalArgumentException (issue #41)
        return new ClusterMessage(transportMessage.message(), membership().member(transportMessage.endpoint().id()));
      }
    });
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
  public ICluster join() {
    updateClusterState(State.INSTANTIATED, State.JOINING);
    transport.start();
    failureDetector.start();
    gossipProtocol.start();
    clusterMembership.start();
    updateClusterState(State.JOINING, State.JOINED);
    return this;
  }

  @Override
  public ListenableFuture<Void> leave() {
    updateClusterState(State.JOINED, State.LEAVING);

    // Notify cluster members about graceful shutdown of current member
    clusterMembership.leave();

    // Wait for some time until 'leave' gossip start to spread through the cluster before stopping cluster components
    final SettableFuture<Void> transportStoppedFuture = SettableFuture.create();
    final ScheduledExecutorService stopExecutor = Executors.newSingleThreadScheduledExecutor();
    long delay = 3 * gossipProtocol.getGossipTime(); // wait for 3 gossip periods before stopping
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
    return Futures.transform(transportStoppedFuture, new Function<Void, Void>() {
      @Nullable
      @Override
      public Void apply(Void input) {
        stopExecutor.shutdown();
        updateClusterState(State.LEAVING, State.STOPPED);
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
