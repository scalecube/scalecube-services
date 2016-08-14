package io.scalecube.cluster.membership;

import static com.google.common.collect.Collections2.filter;
import static com.google.common.collect.Collections2.transform;
import static com.google.common.collect.Lists.newArrayList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import io.scalecube.cluster.ClusterMember;
import io.scalecube.cluster.ClusterMemberStatus;
import io.scalecube.cluster.fdetector.FailureDetector;
import io.scalecube.cluster.fdetector.FailureDetectorConfig;
import io.scalecube.cluster.gossip.GossipProtocol;
import io.scalecube.transport.ITransport;
import io.scalecube.transport.Transport;
import io.scalecube.transport.Address;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.base.Throwables;
import com.google.common.util.concurrent.SettableFuture;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

public class ClusterMembershipBuilder {
  final Transport transport;
  final ClusterMembership membership;
  final GossipProtocol gossipProtocol;
  final FailureDetector failureDetector;

  private ClusterMembershipBuilder(Transport transport, List<Address> members) {
    this.transport = transport;

    String memberId = UUID.randomUUID().toString();

    FailureDetectorConfig fdConfig = FailureDetectorConfig.builder() // faster config for local testing
        .pingTime(200)
        .pingTimeout(100)
        .pingReqMembers(2)
        .build();
    failureDetector = new FailureDetector(transport, fdConfig);

    gossipProtocol = new GossipProtocol(memberId, transport);

    MembershipConfig membershipConfig = MembershipConfig.builder()
        .syncTime(1000)
        .syncTimeout(200)
        .maxSuspectTime(5000)
        .build();
    membership = new ClusterMembership(memberId, transport, membershipConfig);
    membership.setFailureDetector(failureDetector);
    membership.setGossipProtocol(gossipProtocol);
    membership.setSeedMembers(members);
  }

  public static ClusterMembershipBuilder CMBuilder(Transport transport, List<Address> members) {
    return new ClusterMembershipBuilder(transport, members);
  }

  ClusterMembershipBuilder init() {
    try {
      failureDetector.start();
      gossipProtocol.start();
      membership.start().get();
    } catch (Exception ex) {
      Throwables.propagate(ex);
    }
    return this;
  }

  void destroy() {
    membership.stop();
    gossipProtocol.stop();
    failureDetector.stop();
    destroyTransport(transport);
  }

  private void destroyTransport(ITransport tf) {
    SettableFuture<Void> close = SettableFuture.create();
    tf.stop(close);
    try {
      close.get(1, TimeUnit.SECONDS);
    } catch (Exception ignore) {
      // ignore
    }
  }

  public ClusterMembershipBuilder assertTrusted(Address... members) {
    assertStatus(ClusterMemberStatus.TRUSTED, members);
    return this;
  }

  public ClusterMembershipBuilder assertSuspected(Address... members) {
    assertStatus(ClusterMemberStatus.SUSPECTED, members);
    return this;
  }

  public ClusterMembershipBuilder assertNoSuspected() {
    assertStatus(ClusterMemberStatus.SUSPECTED, new Address[0]);
    return this;
  }

  private void assertStatus(final ClusterMemberStatus s, Address[] members) {
    Predicate<ClusterMember> predicate = new Predicate<ClusterMember>() {
      @Override
      public boolean apply(ClusterMember input) {
        return input.status() == s;
      }
    };
    Function<ClusterMember, Address> function = new Function<ClusterMember, Address>() {
      @Override
      public Address apply(ClusterMember input) {
        return input.address();
      }
    };
    List<Address> list = newArrayList(transform(filter(membership.members(), predicate), function));
    assertEquals("expect " + s + ": " + list, members.length, list.size());
    for (Address member : members) {
      assertTrue("expect " + s + ": " + member, list.contains(member));
    }
  }
}
