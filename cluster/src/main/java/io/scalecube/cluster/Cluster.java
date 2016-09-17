package io.scalecube.cluster;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.util.concurrent.Futures.transform;
import static com.google.common.util.concurrent.Futures.transformAsync;
import static io.scalecube.cluster.fdetector.FailureDetector.PING_ACK;
import static io.scalecube.cluster.fdetector.FailureDetector.PING;
import static io.scalecube.cluster.fdetector.FailureDetector.PING_REQ;
import static io.scalecube.cluster.gossip.GossipProtocol.GOSSIP_REQ;
import static io.scalecube.cluster.membership.MembershipProtocol.MEMBERSHIP_GOSSIP;
import static io.scalecube.cluster.membership.MembershipProtocol.SYNC;
import static io.scalecube.cluster.membership.MembershipProtocol.SYNC_ACK;

import io.scalecube.cluster.fdetector.FailureDetector;
import io.scalecube.cluster.gossip.GossipProtocol;
import io.scalecube.cluster.membership.MembershipConfig;
import io.scalecube.cluster.membership.MembershipEvent;
import io.scalecube.cluster.membership.MembershipProtocol;
import io.scalecube.transport.Address;
import io.scalecube.transport.Message;
import io.scalecube.transport.Transport;

import com.google.common.base.Function;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import rx.Observable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import javax.annotation.Nullable;

/**
 * Main ICluster implementation.
 * 
 * @author Anton Kharenko
 */
public final class Cluster implements ICluster {

  private static final Logger LOGGER = LoggerFactory.getLogger(Cluster.class);

  private static final Set<String> SYSTEM_MESSAGES =
      ImmutableSet.of(PING, PING_REQ, PING_ACK, SYNC, SYNC_ACK, GOSSIP_REQ);

  private static final Set<String> SYSTEM_GOSSIPS = ImmutableSet.of(MEMBERSHIP_GOSSIP);

  private final ClusterConfig config;

  private final ConcurrentMap<String, Member> members = new ConcurrentHashMap<>();
  private final ConcurrentMap<Address, String> memberAddressIndex = new ConcurrentHashMap<>();


  // Cluster components
  private Transport transport;
  private FailureDetector failureDetector;
  private GossipProtocol gossip;
  private MembershipProtocol membership;

  private Cluster(ClusterConfig config) {
    checkNotNull(config);
    this.config = config;
  }

  /**
   * Init cluster instance and join cluster synchronously.
   */
  public static ICluster joinAwait() {
    try {
      return join().get();
    } catch (Exception e) {
      throw Throwables.propagate(Throwables.getRootCause(e));
    }
  }

  /**
   * Init cluster instance with the given seed members and join cluster synchronously.
   */
  public static ICluster joinAwait(Address... seedMembers) {
    try {
      return join(seedMembers).get();
    } catch (Exception e) {
      throw Throwables.propagate(Throwables.getRootCause(e));
    }
  }

  /**
   * Init cluster instance with the given metadata and seed members and join cluster synchronously.
   */
  public static ICluster joinAwait(Map<String, String> metadata, Address... seedMembers) {
    try {
      return join(metadata, seedMembers).get();
    } catch (Exception e) {
      throw Throwables.propagate(Throwables.getRootCause(e));
    }
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

  /**
   * Init cluster instance and join cluster asynchronously.
   */
  public static ListenableFuture<ICluster> join() {
    return join(ClusterConfig.defaultConfig());
  }

  /**
   * Init cluster instance with the given seed members and join cluster asynchronously.
   */
  public static ListenableFuture<ICluster> join(Address... seedMembers) {
    ClusterConfig config = ClusterConfig.builder()
        .membershipConfig(MembershipConfig.builder().seedMembers(Arrays.asList(seedMembers)).build())
        .build();
    return join(config);
  }

  /**
   * Init cluster instance with the given metadata and seed members and join cluster synchronously.
   */
  public static ListenableFuture<ICluster> join(Map<String, String> metadata, Address... seedMembers) {
    ClusterConfig config = ClusterConfig.builder()
        .membershipConfig(
            MembershipConfig.builder()
                .seedMembers(Arrays.asList(seedMembers))
                .metadata(metadata)
                .build())
        .build();
    return join(config);
  }

  /**
   * Init cluster instance with the given configuration and join cluster synchronously.
   */
  public static ListenableFuture<ICluster> join(final ClusterConfig config) {
    return new Cluster(config).join0();
  }

  private ListenableFuture<ICluster> join0() {
    ListenableFuture<Transport> transportFuture = Transport.bind(config.getTransportConfig());
    ListenableFuture<Void> clusterFuture = transformAsync(transportFuture, boundTransport -> {
      // Init components
      transport = boundTransport;
      membership = new MembershipProtocol(transport, config.getMembershipConfig());
      gossip = new GossipProtocol(transport, membership, config.getGossipConfig());
      failureDetector = new FailureDetector(transport, membership, config.getFailureDetectorConfig());
      membership.setFailureDetector(failureDetector);
      membership.setGossipProtocol(gossip);

      // Init membership
      Member localMember = membership.member();
      onMemberAdded(localMember);
      membership.listen()
          .filter(MembershipEvent::isAdded).map(MembershipEvent::member).subscribe(this::onMemberAdded);
      membership.listen()
          .filter(MembershipEvent::isRemoved).map(MembershipEvent::member).subscribe(this::onMemberRemoved);

      // Start components
      failureDetector.start();
      gossip.start();
      return membership.start();
    });
    return transform(clusterFuture, new Function<Void, ICluster>() {
      @Override
      public ICluster apply(@Nullable Void param) {
        return Cluster.this;
      }
    });
  }

  private void onMemberAdded(Member member) {
    memberAddressIndex.put(member.address(), member.id());
    members.put(member.id(), member);
  }

  private void onMemberRemoved(Member member) {
    members.remove(member.id());
    memberAddressIndex.remove(member.address());
  }

  @Override
  public Address address() {
    return transport.address();
  }

  @Override
  public void send(Member member, Message message) {
    transport.send(member.address(), message);
  }

  @Override
  public void send(Address address, Message message) {
    transport.send(address, message);
  }

  @Override
  public void send(Member member, Message message, SettableFuture<Void> promise) {
    transport.send(member.address(), message, promise);
  }

  @Override
  public void send(Address address, Message message, SettableFuture<Void> promise) {
    transport.send(address, message, promise);
  }

  @Override
  public Observable<Message> listen() {
    return transport.listen().filter(msg -> !SYSTEM_MESSAGES.contains(msg.qualifier())); // filter out system gossips
  }

  @Override
  public void spreadGossip(Message message) {
    gossip.spread(message);
  }

  @Override
  public Observable<Message> listenGossips() {
    return gossip.listen().filter(msg -> !SYSTEM_GOSSIPS.contains(msg.qualifier())); // filter out system gossips
  }

  @Override
  public Collection<Member> members() {
    return Collections.unmodifiableCollection(members.values());
  }

  @Override
  public Member member() {
    return membership.member();
  }

  @Override
  public Member member(String id) {
    return members.get(id);
  }

  @Override
  public Member member(Address address) {
    String memberId = memberAddressIndex.get(address);
    return memberId != null ? members.get(memberId) : null;
  }

  @Override
  public Collection<Member> otherMembers() {
    ArrayList<Member> otherMembers = new ArrayList<>(members.values());
    otherMembers.remove(membership.member());
    return Collections.unmodifiableCollection(otherMembers);
  }

  @Override
  public Observable<MembershipEvent> listenMembership() {
    return membership.listen();
  }

  @Override
  public ListenableFuture<Void> shutdown() {
    LOGGER.info("Cluster member {} is shutting down...", membership.member());

    // stop algorithms
    membership.stop();
    gossip.stop();
    failureDetector.stop();

    // stop transport
    SettableFuture<Void> transportStoppedFuture = SettableFuture.create();
    transport.stop(transportStoppedFuture);

    return transportStoppedFuture;
  }

}
