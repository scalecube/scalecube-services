package io.scalecube.cluster.membership;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import io.scalecube.cluster.fdetector.FailureDetector;
import io.scalecube.cluster.fdetector.FailureDetectorConfig;
import io.scalecube.cluster.gossip.GossipConfig;
import io.scalecube.cluster.gossip.GossipProtocol;
import io.scalecube.transport.Address;
import io.scalecube.transport.ITransport;
import io.scalecube.transport.Transport;

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.SettableFuture;

import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class MembershipProtocolIT {

  @Test
  public void testInitialPhaseOk() {
    Transport a = Transport.bindAwait(true);
    Transport b = Transport.bindAwait(true);
    Transport c = Transport.bindAwait(true);
    List<Address> members = ImmutableList.of(a.address(), b.address(), c.address());

    MembershipProtocol cm_a = createMembership(a, members);
    MembershipProtocol cm_b = createMembership(b, members);
    MembershipProtocol cm_c = createMembership(c, members);

    try {
      awaitSeconds(1);

      assertTrusted(cm_a, a.address(), b.address(), c.address());
      assertNoSuspected(cm_a);
      assertTrusted(cm_b, a.address(), b.address(), c.address());
      assertNoSuspected(cm_b);
      assertTrusted(cm_c, a.address(), b.address(), c.address());
      assertNoSuspected(cm_c);
    } finally {
      stopAll(cm_a, cm_b, cm_c);
    }
  }

  @Test
  public void testInitialPhaseWithNetworkPartitionThenRecovery() {
    Transport a = Transport.bindAwait(true);
    Transport b = Transport.bindAwait(true);
    Transport c = Transport.bindAwait(true);
    List<Address> members = ImmutableList.of(a.address(), b.address(), c.address());

    MembershipProtocol cm_a = createMembership(a, members);
    MembershipProtocol cm_b = createMembership(b, members);
    MembershipProtocol cm_c = createMembership(c, members);

    // Block traffic
    a.block(members);
    b.block(members);
    c.block(members);

    try {
      awaitSeconds(6);

      assertTrusted(cm_a, a.address());
      assertNoSuspected(cm_a);
      assertTrusted(cm_b, b.address());
      assertNoSuspected(cm_b);
      assertTrusted(cm_c, c.address());
      assertNoSuspected(cm_c);

      a.unblockAll();
      b.unblockAll();
      c.unblockAll();

      awaitSeconds(3);

      assertTrusted(cm_a, a.address(), b.address(), c.address());
      assertNoSuspected(cm_a);
      assertTrusted(cm_b, a.address(), b.address(), c.address());
      assertNoSuspected(cm_b);
      assertTrusted(cm_c, a.address(), b.address(), c.address());
      assertNoSuspected(cm_c);
    } finally {
      stopAll(cm_a, cm_b, cm_c);
    }
  }

  @Test
  public void testRunningPhaseOk() {
    Transport a = Transport.bindAwait(true);
    Transport b = Transport.bindAwait(true);
    Transport c = Transport.bindAwait(true);
    List<Address> members = ImmutableList.of(a.address(), b.address(), c.address());

    MembershipProtocol cm_a = createMembership(a, members);
    MembershipProtocol cm_b = createMembership(b, members);
    MembershipProtocol cm_c = createMembership(c, members);

    try {
      awaitSeconds(1);

      assertTrusted(cm_a, a.address(), b.address(), c.address());
      assertNoSuspected(cm_a);
      assertTrusted(cm_b, a.address(), b.address(), c.address());
      assertNoSuspected(cm_b);
      assertTrusted(cm_c, a.address(), b.address(), c.address());
      assertNoSuspected(cm_c);

      a.block(members);
      b.block(members);
      c.block(members);

      awaitSeconds(3);

      assertTrusted(cm_a, a.address());
      assertSuspected(cm_a, b.address(), c.address());

      assertTrusted(cm_b, b.address());
      assertSuspected(cm_b,a.address(), c.address());

      assertTrusted(cm_c, c.address());
      assertSuspected(cm_c, a.address(), b.address());

      a.unblockAll();
      b.unblockAll();
      c.unblockAll();

      awaitSeconds(3);

      assertTrusted(cm_a, a.address(), b.address(), c.address());
      assertNoSuspected(cm_a);

      assertTrusted(cm_b, a.address(), b.address(), c.address());
      assertNoSuspected(cm_b);

      assertTrusted(cm_c, a.address(), b.address(), c.address());
      assertNoSuspected(cm_c);
    } finally {
      stopAll(cm_a, cm_b, cm_c);
    }
  }

  @Test
  public void testLongNetworkPartitionNoRecovery() {
    Transport a = Transport.bindAwait(true);
    Transport b = Transport.bindAwait(true);
    Transport c = Transport.bindAwait(true);
    Transport d = Transport.bindAwait(true);
    List<Address> members = ImmutableList.of(a.address(), b.address(), c.address(), d.address());

    MembershipProtocol cm_a = createMembership(a, members);
    MembershipProtocol cm_b = createMembership(b, members);
    MembershipProtocol cm_c = createMembership(c, members);
    MembershipProtocol cm_d = createMembership(d, members);

    try {
      awaitSeconds(1);

      assertTrusted(cm_a, a.address(), b.address(), c.address(), d.address());
      assertTrusted(cm_b, a.address(), b.address(), c.address(), d.address());
      assertTrusted(cm_c, a.address(), b.address(), c.address(), d.address());
      assertTrusted(cm_d, a.address(), b.address(), c.address(), d.address());

      a.block(Arrays.asList(c.address(), d.address()));
      b.block(Arrays.asList(c.address(), d.address()));

      c.block(Arrays.asList(a.address(), b.address()));
      d.block(Arrays.asList(a.address(), b.address()));

      awaitSeconds(3);

      assertTrusted(cm_a, a.address(), b.address());
      assertSuspected(cm_a, c.address(), d.address());
      assertTrusted(cm_b, a.address(), b.address());
      assertSuspected(cm_b, c.address(), d.address());
      assertTrusted(cm_c, c.address(), d.address());
      assertSuspected(cm_c, a.address(), b.address());
      assertTrusted(cm_d, c.address(), d.address());
      assertSuspected(cm_d, a.address(), b.address());

      awaitSeconds(6); // > max suspect time (5)

      assertTrusted(cm_a, a.address(), b.address());
      assertNoSuspected(cm_a);
      assertTrusted(cm_b, a.address(), b.address());
      assertNoSuspected(cm_b);
      assertTrusted(cm_c, c.address(), d.address());
      assertNoSuspected(cm_c);
      assertTrusted(cm_d, c.address(), d.address());
      assertNoSuspected(cm_d);
    } finally {
      stopAll(cm_a, cm_b, cm_c, cm_d);
    }
  }

  @Test
  public void testRestartFailedMembers() {
    Transport a = Transport.bindAwait(true);
    Transport b = Transport.bindAwait(true);
    Transport c = Transport.bindAwait(true);
    Transport d = Transport.bindAwait(true);
    List<Address> members = ImmutableList.of(a.address(), b.address(), c.address(), d.address());

    MembershipProtocol cm_a = createMembership(a, members);
    MembershipProtocol cm_b = createMembership(b, members);
    MembershipProtocol cm_c = createMembership(c, members);
    MembershipProtocol cm_d = createMembership(d, members);

    MembershipProtocol cm_restartedC = null;
    MembershipProtocol cm_restartedD = null;

    try {
      awaitSeconds(1);

      assertTrusted(cm_a, a.address(), b.address(), c.address(), d.address());
      assertTrusted(cm_b, a.address(), b.address(), c.address(), d.address());
      assertTrusted(cm_c, a.address(), b.address(), c.address(), d.address());
      assertTrusted(cm_d, a.address(), b.address(), c.address(), d.address());

      stop(cm_c);
      stop(cm_d);

      awaitSeconds(3);

      assertTrusted(cm_a, a.address(), b.address());
      assertSuspected(cm_a, c.address(), d.address());
      assertTrusted(cm_b, a.address(), b.address());
      assertSuspected(cm_b, c.address(), d.address());

      awaitSeconds(6); // > max suspect time (5)

      assertTrusted(cm_a, a.address(), b.address());
      assertNoSuspected(cm_a);
      assertTrusted(cm_b, a.address(), b.address());
      assertNoSuspected(cm_b);

      c = Transport.bindAwait(true);
      d = Transport.bindAwait(true);
      cm_restartedC = createMembership(c, Arrays.asList(a.address(), b.address()));
      cm_restartedD = createMembership(d, Arrays.asList(a.address(), b.address()));

      awaitSeconds(1);

      assertTrusted(cm_restartedC, a.address(), b.address(), c.address(), d.address());
      assertNoSuspected(cm_restartedC);
      assertTrusted(cm_restartedD, a.address(), b.address(), c.address(), d.address());
      assertNoSuspected(cm_restartedD);
      assertTrusted(cm_a, a.address(), b.address(), c.address(), d.address());
      assertNoSuspected(cm_a);
      assertTrusted(cm_b, a.address(), b.address(), c.address(), d.address());
      assertNoSuspected(cm_b);
    } finally {
      stopAll(cm_a, cm_b, cm_restartedC, cm_restartedD);
    }
  }

  @Test
  public void testClusterMembersWellknownMembersLimited() {
    Transport a = Transport.bindAwait(true);
    Transport b = Transport.bindAwait(true);
    Transport c = Transport.bindAwait(true);
    Transport d = Transport.bindAwait(true);
    Transport e = Transport.bindAwait(true);

    MembershipProtocol cm_a = createMembership(a, Collections.emptyList());
    MembershipProtocol cm_b = createMembership(b, Collections.singletonList(a.address()));
    MembershipProtocol cm_c = createMembership(c, Collections.singletonList(a.address()));
    MembershipProtocol cm_d = createMembership(d, Collections.singletonList(b.address()));
    MembershipProtocol cm_e = createMembership(e, Collections.singletonList(b.address()));

    try {
      awaitSeconds(3);

      assertTrusted(cm_a, a.address(), b.address(), c.address(), d.address(), e.address());
      assertNoSuspected(cm_a);
      assertTrusted(cm_b, a.address(), b.address(), c.address(), d.address(), e.address());
      assertNoSuspected(cm_b);
      assertTrusted(cm_c, a.address(), b.address(), c.address(), d.address(), e.address());
      assertNoSuspected(cm_c);
      assertTrusted(cm_d, a.address(), b.address(), c.address(), d.address(), e.address());
      assertNoSuspected(cm_d);
      assertTrusted(cm_e, a.address(), b.address(), c.address(), d.address(), e.address());
      assertNoSuspected(cm_e);
    } finally {
      stopAll(cm_a, cm_b, cm_c, cm_d, cm_e);
    }
  }

  private void awaitSeconds(int seconds) {
    try {
      TimeUnit.SECONDS.sleep(seconds);
    } catch (InterruptedException e) {
      Throwables.propagate(e);
    }
  }

  public MembershipProtocol createMembership(Transport transport, List<Address> seedAddresses) {
    // Create membership protocol
    MembershipConfig membershipConfig = MembershipConfig.builder()
        .seedMembers(seedAddresses)
        .syncInterval(1000)
        .syncTimeout(200)
        .suspectTimeout(5000)
        .build();
    MembershipProtocol membership = new MembershipProtocol(transport, membershipConfig);

    // Create failure detector
    FailureDetectorConfig fdConfig = FailureDetectorConfig.builder() // faster config for local testing
        .pingInterval(200)
        .pingTimeout(100)
        .build();
    FailureDetector failureDetector = new FailureDetector(transport, membership, fdConfig);

    // Create gossip protocol
    GossipProtocol gossipProtocol = new GossipProtocol(transport, membership, GossipConfig.defaultConfig());

    // Set membership components
    membership.setGossipProtocol(gossipProtocol);
    membership.setFailureDetector(failureDetector);

    try {
      failureDetector.start();
      gossipProtocol.start();
      membership.start().get();
    } catch (Exception ex) {
      Throwables.propagate(ex);
    }

    return membership;
  }

  public void stopAll(MembershipProtocol... memberships) {
    for (MembershipProtocol membership : memberships) {
      if (membership != null) {
        stop(membership);
      }
    }
  }

  public void stop(MembershipProtocol membership) {
    membership.stop();
    membership.getGossipProtocol().stop();
    membership.getFailureDetector().stop();

    ITransport transport = membership.getTransport();
    SettableFuture<Void> close = SettableFuture.create();
    transport.stop(close);
    try {
      close.get(1, TimeUnit.SECONDS);
    } catch (Exception ignore) {
      // ignore
    }
  }

  public void assertTrusted(MembershipProtocol membership, Address... expected) {
    List<Address> actual = getAddressesWithStatus(membership, MemberStatus.ALIVE);
    assertEquals("Expected " + expected.length + " trusted members " + Arrays.toString(expected)
        + ", but actual: " + actual, expected.length, actual.size());
    for (Address member : expected) {
      assertTrue("Expected to trust " + member + ", but actual: " + actual, actual.contains(member));
    }
  }

  public void assertSuspected(MembershipProtocol membership, Address... expected) {
    List<Address> actual = getAddressesWithStatus(membership, MemberStatus.SUSPECT);
    assertEquals("Expected " + expected.length + " suspect members " + Arrays.toString(expected)
        + ", but actual: " + actual, expected.length, actual.size());
    for (Address member : expected) {
      assertTrue("Expected to suspect " + member + ", but actual: " + actual, actual.contains(member));
    }
  }

  public void assertNoSuspected(MembershipProtocol membership) {
    List<Address> actual = getAddressesWithStatus(membership, MemberStatus.SUSPECT);
    assertEquals("Expected no suspected, but actual: " + actual, 0, actual.size());
  }

  private List<Address> getAddressesWithStatus(MembershipProtocol membership, MemberStatus status) {
    List<Address> addresses = new ArrayList<>();
    for (MembershipRecord member : membership.getMembershipRecords()) {
      if (member.status() == status) {
        addresses.add(member.address());
      }
    }
    return addresses;
  }
}
