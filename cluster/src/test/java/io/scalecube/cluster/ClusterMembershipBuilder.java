package io.scalecube.cluster;

import static com.google.common.collect.Collections2.filter;
import static com.google.common.collect.Collections2.transform;
import static com.google.common.collect.Lists.newArrayList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import io.scalecube.cluster.fdetector.FailureDetector;
import io.scalecube.cluster.fdetector.FailureDetectorSettings;
import io.scalecube.cluster.gossip.GossipProtocol;
import io.scalecube.transport.ITransport;
import io.scalecube.transport.Transport;
import io.scalecube.transport.Address;
import io.scalecube.transport.TransportSettings;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.base.Throwables;
import com.google.common.util.concurrent.SettableFuture;

import rx.schedulers.Schedulers;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

public class ClusterMembershipBuilder {
  final Transport transport;
  final ClusterMembership membership;
  final GossipProtocol gossipProtocol;
  final FailureDetector failureDetector;

  private ClusterMembershipBuilder(Address localAddress, List<Address> members) {
    TransportSettings transportSettings = TransportSettings.builder().useNetworkEmulator(true).build();
    transport = Transport.newInstance(localAddress, transportSettings);

    String memberId = UUID.randomUUID().toString();

    FailureDetectorSettings fdSettings = FailureDetectorSettings.builder().pingTime(100).pingTimeout(100).build();
    failureDetector = new FailureDetector(transport, fdSettings);

    gossipProtocol = new GossipProtocol(memberId, transport);


    membership = new ClusterMembership(memberId, localAddress, Schedulers.computation());
    membership.setTransport(transport);
    membership.setFailureDetector(failureDetector);
    membership.setGossipProtocol(gossipProtocol);
    membership.setLocalMetadata(new HashMap<String, String>() {
      {
        put("key", "val");
      }
    });
    membership.setSeedMembers(members);
    membership.setSyncTime(1000);
    membership.setSyncTimeout(100);
  }

  public static ClusterMembershipBuilder CMBuilder(Address localAddress, List<Address> members) {
    return new ClusterMembershipBuilder(localAddress, members);
  }

  public static ClusterMembershipBuilder CMBuilder(Address localAddress, Address... members) {
    return new ClusterMembershipBuilder(localAddress, Arrays.asList(members));
  }

  public ClusterMembershipBuilder maxSuspectTime(int maxSuspectTime) {
    membership.setMaxSuspectTime(maxSuspectTime);
    return this;
  }

  public ClusterMembershipBuilder block(Address dest) {
    transport.blockMessagesTo(dest);
    return this;
  }

  public ClusterMembershipBuilder unblockAll() {
    transport.unblockAll();
    return this;
  }

  ClusterMembershipBuilder init() {
    try {
      transport.start().get();
      failureDetector.start();
      gossipProtocol.start();
      membership.start().get();
    } catch (Exception ex) {
      Throwables.propagate(ex);
    }
    return this;
  }

  void destroy() {
    destroyTransport(transport);
    failureDetector.stop();
    gossipProtocol.stop();
    membership.stop();
  }

  private void destroyTransport(ITransport tf) {
    SettableFuture<Void> close = SettableFuture.create();
    tf.stop(close);
    try {
      close.get(1, TimeUnit.SECONDS);
    } catch (Exception ignore) {
    }
    try {
      Thread.sleep(10);
    } catch (InterruptedException ignore) {
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
