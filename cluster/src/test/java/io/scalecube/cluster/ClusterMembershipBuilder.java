package io.scalecube.cluster;

import static com.google.common.collect.Collections2.filter;
import static com.google.common.collect.Collections2.transform;
import static com.google.common.collect.Lists.newArrayList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import io.scalecube.cluster.fdetector.FailureDetectorBuilder;
import io.scalecube.cluster.gossip.GossipProtocol;
import io.scalecube.transport.ITransport;
import io.scalecube.transport.Transport;
import io.scalecube.transport.TransportEndpoint;
import io.scalecube.transport.TransportSettings;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.base.Throwables;
import com.google.common.util.concurrent.SettableFuture;

import rx.schedulers.Schedulers;

import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class ClusterMembershipBuilder {
  final ClusterMembership membership;
  final GossipProtocol gossipProtocol;
  final FailureDetectorBuilder fdBuilder;
  final Transport transport;

  private ClusterMembershipBuilder(TransportEndpoint transportEndpoint, List<InetSocketAddress> members) {
    transport = Transport.newInstance(transportEndpoint, TransportSettings.DEFAULT_WITH_NETWORK_EMULATOR);

    fdBuilder = FailureDetectorBuilder.FDBuilder(transportEndpoint, transport).pingTime(100).pingTimeout(100);

    gossipProtocol = new GossipProtocol(transportEndpoint, Executors.newSingleThreadScheduledExecutor());
    gossipProtocol.setTransport(transport);

    membership = new ClusterMembership(transportEndpoint, Schedulers.computation());
    membership.setTransport(transport);
    membership.setFailureDetector(fdBuilder.target());
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

  public static ClusterMembershipBuilder CMBuilder(TransportEndpoint transportEndpoint,
      List<InetSocketAddress> members) {
    return new ClusterMembershipBuilder(transportEndpoint, members);
  }

  public static ClusterMembershipBuilder CMBuilder(TransportEndpoint transportEndpoint, InetSocketAddress... members) {
    return new ClusterMembershipBuilder(transportEndpoint, Arrays.asList(members));
  }

  public ClusterMembershipBuilder maxSuspectTime(int maxSuspectTime) {
    membership.setMaxSuspectTime(maxSuspectTime);
    return this;
  }

  public ClusterMembershipBuilder block(TransportEndpoint dest) {
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
      fdBuilder.target().start();
      gossipProtocol.start();
      membership.start().get();
    } catch (Exception ex) {
      Throwables.propagate(ex);
    }
    return this;
  }

  void destroy() {
    destroyTransport(transport);
    fdBuilder.target().stop();
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

  public ClusterMembershipBuilder assertTrusted(TransportEndpoint... members) {
    assertStatus(ClusterMemberStatus.TRUSTED, members);
    return this;
  }

  public ClusterMembershipBuilder assertSuspected(TransportEndpoint... members) {
    assertStatus(ClusterMemberStatus.SUSPECTED, members);
    return this;
  }

  public ClusterMembershipBuilder assertNoSuspected() {
    assertStatus(ClusterMemberStatus.SUSPECTED, new TransportEndpoint[0]);
    return this;
  }

  private void assertStatus(final ClusterMemberStatus s, TransportEndpoint[] members) {
    Predicate<ClusterMember> predicate = new Predicate<ClusterMember>() {
      @Override
      public boolean apply(ClusterMember input) {
        return input.status() == s;
      }
    };
    Function<ClusterMember, TransportEndpoint> function = new Function<ClusterMember, TransportEndpoint>() {
      @Override
      public TransportEndpoint apply(ClusterMember input) {
        return input.endpoint();
      }
    };
    List<TransportEndpoint> list = newArrayList(transform(filter(membership.members(), predicate), function));
    assertEquals("expect " + s + ": " + list, members.length, list.size());
    for (TransportEndpoint member : members) {
      assertTrue("expect " + s + ": " + member, list.contains(member));
    }
  }
}
