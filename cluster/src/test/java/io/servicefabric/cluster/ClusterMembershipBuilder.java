package io.servicefabric.cluster;

import static com.google.common.collect.Collections2.filter;
import static com.google.common.collect.Collections2.transform;
import static com.google.common.collect.Lists.newArrayList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import io.servicefabric.cluster.fdetector.FailureDetectorBuilder;
import io.servicefabric.cluster.gossip.GossipProtocol;
import io.servicefabric.transport.ITransport;
import io.servicefabric.transport.TransportPipelineFactory;
import io.servicefabric.transport.Transport;
import io.servicefabric.transport.TransportAddress;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.util.concurrent.SettableFuture;

import io.servicefabric.transport.TransportEndpoint;
import io.servicefabric.transport.TransportSettings;
import rx.schedulers.Schedulers;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nullable;

public class ClusterMembershipBuilder {
  final ClusterMembership target;
  final GossipProtocol gossipProtocol;
  final FailureDetectorBuilder fdBuilder;
  final Transport transport;

  private ClusterMembershipBuilder(TransportEndpoint transportEndpoint, List<TransportAddress> members) {
    transport = Transport.newInstance(transportEndpoint, TransportSettings.DEFAULT_WITH_NETWORK_EMULATOR);

    fdBuilder = FailureDetectorBuilder.FDBuilder(transportEndpoint, transport).pingTime(100).pingTimeout(100);

    gossipProtocol = new GossipProtocol(transportEndpoint, Executors.newSingleThreadScheduledExecutor());
    gossipProtocol.setTransport(transport);

    target = new ClusterMembership(transportEndpoint, Schedulers.computation());
    target.setTransport(transport);
    target.setFailureDetector(fdBuilder.target());
    target.setGossipProtocol(gossipProtocol);
    target.setLocalMetadata(new HashMap<String, String>() {
      {
        put("key", "val");
      }
    });
    target.setSeedMembers(members);
    target.setSyncTime(1000);
    target.setSyncTimeout(100);
  }

  public static ClusterMembershipBuilder CMBuilder(TransportEndpoint transportEndpoint, List<TransportAddress> members) {
    return new ClusterMembershipBuilder(transportEndpoint, members);
  }

  public static ClusterMembershipBuilder CMBuilder(TransportEndpoint transportEndpoint, TransportAddress... members) {
    return new ClusterMembershipBuilder(transportEndpoint, Arrays.asList(members));
  }

  public ClusterMembershipBuilder syncTime(int syncTime) {
    target.setSyncTime(syncTime);
    return this;
  }

  public ClusterMembershipBuilder syncTimeout(int syncTimeout) {
    target.setSyncTimeout(syncTimeout);
    return this;
  }

  public ClusterMembershipBuilder maxSuspectTime(int maxSuspectTime) {
    target.setMaxSuspectTime(maxSuspectTime);
    return this;
  }

  public ClusterMembershipBuilder maxShutdownTime(int maxShutdownTime) {
    target.setMaxShutdownTime(maxShutdownTime);
    return this;
  }

  public ClusterMembershipBuilder pingTime(int pingTime) {
    fdBuilder.pingTime(pingTime);
    return this;
  }

  public ClusterMembershipBuilder pingTimeout(int pingTimeout) {
    fdBuilder.pingTimeout(pingTimeout);
    return this;
  }

  public ClusterMembershipBuilder ping(TransportEndpoint member) {
    fdBuilder.ping(member);
    return this;
  }

  public ClusterMembershipBuilder noRandomMembers() {
    fdBuilder.noRandomMembers();
    return this;
  }

  public ClusterMembershipBuilder randomMembers(List<TransportEndpoint> members) {
    fdBuilder.randomMembers(members);
    return this;
  }

  public ClusterMembershipBuilder block(TransportEndpoint dest) {
    transport.<TransportPipelineFactory>getPipelineFactory().blockMessagesTo(dest);
    return this;
  }

  public ClusterMembershipBuilder block(List<TransportEndpoint> members) {
    for (TransportEndpoint dest : members) {
      block(dest);
    }
    return this;
  }

  public ClusterMembershipBuilder network(TransportEndpoint member, int lostPercent, int mean) {
    transport.<TransportPipelineFactory>getPipelineFactory().setNetworkSettings(member, lostPercent, mean);
    return this;
  }

  public ClusterMembershipBuilder unblockAll() {
    transport.<TransportPipelineFactory>getPipelineFactory().unblockAll();
    return this;
  }

  ClusterMembershipBuilder init() {
    transport.start();
    fdBuilder.target().start();
    gossipProtocol.start();
    target.start();
    return this;
  }

  void destroy() {
    destroyTransport(transport);
    fdBuilder.target().stop();
    gossipProtocol.stop();
    target.stop();
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
    List<TransportEndpoint> list = newArrayList(transform(filter(target.members(), predicate), function));
    assertEquals("expect " + s + ": " + list, members.length, list.size());
    for (TransportEndpoint member : members) {
      assertTrue("expect " + s + ": " + member, list.contains(member));
    }
  }
}
