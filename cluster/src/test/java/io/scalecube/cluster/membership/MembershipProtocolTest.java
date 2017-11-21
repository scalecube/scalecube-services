package io.scalecube.cluster.membership;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import io.scalecube.cluster.ClusterConfig;
import io.scalecube.cluster.ClusterMath;
import io.scalecube.cluster.fdetector.FailureDetectorImpl;
import io.scalecube.cluster.gossip.GossipProtocolImpl;
import io.scalecube.testlib.BaseTest;
import io.scalecube.transport.Address;
import io.scalecube.transport.Transport;

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;

import org.junit.Test;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class MembershipProtocolTest extends BaseTest {

  private static final int TEST_PING_INTERVAL = 200;

  @Test
  public void testInitialPhaseOk() {
    Transport a = Transport.bindAwait(true);
    Transport b = Transport.bindAwait(true);
    Transport c = Transport.bindAwait(true);
    List<Address> members = ImmutableList.of(a.address(), b.address(), c.address());

    MembershipProtocolImpl cm_a = createMembership(a, members);
    MembershipProtocolImpl cm_b = createMembership(b, members);
    MembershipProtocolImpl cm_c = createMembership(c, members);

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
  public void testNetworkPartitionThenRecovery() {
    Transport a = Transport.bindAwait(true);
    Transport b = Transport.bindAwait(true);
    Transport c = Transport.bindAwait(true);
    List<Address> members = ImmutableList.of(a.address(), b.address(), c.address());

    MembershipProtocolImpl cm_a = createMembership(a, members);
    MembershipProtocolImpl cm_b = createMembership(b, members);
    MembershipProtocolImpl cm_c = createMembership(c, members);

    // Block traffic
    a.networkEmulator().block(members);
    b.networkEmulator().block(members);
    c.networkEmulator().block(members);

    try {
      awaitSeconds(6);

      assertTrusted(cm_a, a.address());
      assertNoSuspected(cm_a);
      assertTrusted(cm_b, b.address());
      assertNoSuspected(cm_b);
      assertTrusted(cm_c, c.address());
      assertNoSuspected(cm_c);

      a.networkEmulator().unblockAll();
      b.networkEmulator().unblockAll();
      c.networkEmulator().unblockAll();

      awaitSeconds(6);

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
  public void testMemberLostNetworkThenRecover() {
    Transport a = Transport.bindAwait(true);
    Transport b = Transport.bindAwait(true);
    Transport c = Transport.bindAwait(true);
    List<Address> members = ImmutableList.of(a.address(), b.address(), c.address());

    MembershipProtocolImpl cm_a = createMembership(a, members);
    MembershipProtocolImpl cm_b = createMembership(b, members);
    MembershipProtocolImpl cm_c = createMembership(c, members);

    try {
      awaitSeconds(1);

      // Check all trusted
      assertTrusted(cm_a, a.address(), b.address(), c.address());
      assertNoSuspected(cm_a);
      assertTrusted(cm_b, a.address(), b.address(), c.address());
      assertNoSuspected(cm_b);
      assertTrusted(cm_c, a.address(), b.address(), c.address());
      assertNoSuspected(cm_c);

      // Node b lost network
      b.networkEmulator().block(Arrays.asList(a.address(), c.address()));
      a.networkEmulator().block(b.address());
      c.networkEmulator().block(b.address());

      awaitSeconds(1);

      // Check partition: {b}, {a, c}
      assertTrusted(cm_a, a.address(), c.address());
      assertSuspected(cm_a, b.address());
      assertTrusted(cm_b, b.address());
      assertSuspected(cm_b, a.address(), c.address());
      assertTrusted(cm_c, a.address(), c.address());
      assertSuspected(cm_c, b.address());

      // Node b recover network
      a.networkEmulator().unblockAll();
      b.networkEmulator().unblockAll();
      c.networkEmulator().unblockAll();

      awaitSeconds(1);

      // Check all trusted again
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
  public void testDoublePartitionThenRecover() {
    Transport a = Transport.bindAwait(true);
    Transport b = Transport.bindAwait(true);
    Transport c = Transport.bindAwait(true);
    List<Address> members = ImmutableList.of(a.address(), b.address(), c.address());

    MembershipProtocolImpl cm_a = createMembership(a, members);
    MembershipProtocolImpl cm_b = createMembership(b, members);
    MembershipProtocolImpl cm_c = createMembership(c, members);

    try {
      awaitSeconds(1);

      // Check all trusted
      assertTrusted(cm_a, a.address(), b.address(), c.address());
      assertNoSuspected(cm_a);
      assertTrusted(cm_b, a.address(), b.address(), c.address());
      assertNoSuspected(cm_b);
      assertTrusted(cm_c, a.address(), b.address(), c.address());
      assertNoSuspected(cm_c);

      // Node b lost network
      b.networkEmulator().block(Arrays.asList(a.address(), c.address()));
      a.networkEmulator().block(b.address());
      c.networkEmulator().block(b.address());

      awaitSeconds(1);

      // Check partition: {b}, {a, c}
      assertTrusted(cm_a, a.address(), c.address());
      assertSuspected(cm_a, b.address());
      assertTrusted(cm_b, b.address());
      assertSuspected(cm_b, a.address(), c.address());
      assertTrusted(cm_c, a.address(), c.address());
      assertSuspected(cm_c, b.address());

      // Node a and c lost network
      a.networkEmulator().block(c.address());
      c.networkEmulator().block(a.address());

      awaitSeconds(1);

      // Check partition: {a}, {b}, {c}
      assertTrusted(cm_a, a.address());
      assertSuspected(cm_a, b.address(), c.address());
      assertTrusted(cm_b, b.address());
      assertSuspected(cm_b, a.address(), c.address());
      assertTrusted(cm_c, c.address());
      assertSuspected(cm_c, b.address(), a.address());

      // Recover network
      a.networkEmulator().unblockAll();
      b.networkEmulator().unblockAll();
      c.networkEmulator().unblockAll();

      awaitSeconds(1);

      // Check all trusted again
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
  public void testNetworkDisabledThenRecovered() {
    Transport a = Transport.bindAwait(true);
    Transport b = Transport.bindAwait(true);
    Transport c = Transport.bindAwait(true);
    List<Address> members = ImmutableList.of(a.address(), b.address(), c.address());

    MembershipProtocolImpl cm_a = createMembership(a, members);
    MembershipProtocolImpl cm_b = createMembership(b, members);
    MembershipProtocolImpl cm_c = createMembership(c, members);

    try {
      awaitSeconds(1);

      assertTrusted(cm_a, a.address(), b.address(), c.address());
      assertNoSuspected(cm_a);
      assertTrusted(cm_b, a.address(), b.address(), c.address());
      assertNoSuspected(cm_b);
      assertTrusted(cm_c, a.address(), b.address(), c.address());
      assertNoSuspected(cm_c);

      a.networkEmulator().block(members);
      b.networkEmulator().block(members);
      c.networkEmulator().block(members);

      awaitSeconds(1);

      assertTrusted(cm_a, a.address());
      assertSuspected(cm_a, b.address(), c.address());

      assertTrusted(cm_b, b.address());
      assertSuspected(cm_b,a.address(), c.address());

      assertTrusted(cm_c, c.address());
      assertSuspected(cm_c, a.address(), b.address());

      a.networkEmulator().unblockAll();
      b.networkEmulator().unblockAll();
      c.networkEmulator().unblockAll();

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
  public void testLongNetworkPartitionNoRecovery() {
    Transport a = Transport.bindAwait(true);
    Transport b = Transport.bindAwait(true);
    Transport c = Transport.bindAwait(true);
    Transport d = Transport.bindAwait(true);
    List<Address> members = ImmutableList.of(a.address(), b.address(), c.address(), d.address());

    MembershipProtocolImpl cm_a = createMembership(a, members);
    MembershipProtocolImpl cm_b = createMembership(b, members);
    MembershipProtocolImpl cm_c = createMembership(c, members);
    MembershipProtocolImpl cm_d = createMembership(d, members);

    try {
      awaitSeconds(1);

      assertTrusted(cm_a, a.address(), b.address(), c.address(), d.address());
      assertTrusted(cm_b, a.address(), b.address(), c.address(), d.address());
      assertTrusted(cm_c, a.address(), b.address(), c.address(), d.address());
      assertTrusted(cm_d, a.address(), b.address(), c.address(), d.address());

      a.networkEmulator().block(Arrays.asList(c.address(), d.address()));
      b.networkEmulator().block(Arrays.asList(c.address(), d.address()));

      c.networkEmulator().block(Arrays.asList(a.address(), b.address()));
      d.networkEmulator().block(Arrays.asList(a.address(), b.address()));

      awaitSeconds(2);

      assertTrusted(cm_a, a.address(), b.address());
      assertSuspected(cm_a, c.address(), d.address());
      assertTrusted(cm_b, a.address(), b.address());
      assertSuspected(cm_b, c.address(), d.address());
      assertTrusted(cm_c, c.address(), d.address());
      assertSuspected(cm_c, a.address(), b.address());
      assertTrusted(cm_d, c.address(), d.address());
      assertSuspected(cm_d, a.address(), b.address());

      long suspicionTimeoutSec =
          ClusterMath.suspicionTimeout(ClusterConfig.DEFAULT_SUSPICION_MULT, 4, TEST_PING_INTERVAL) / 1000;
      awaitSeconds(suspicionTimeoutSec + 1); // > max suspect time

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

    MembershipProtocolImpl cm_a = createMembership(a, members);
    MembershipProtocolImpl cm_b = createMembership(b, members);
    MembershipProtocolImpl cm_c = createMembership(c, members);
    MembershipProtocolImpl cm_d = createMembership(d, members);

    MembershipProtocolImpl cm_restartedC = null;
    MembershipProtocolImpl cm_restartedD = null;

    try {
      awaitSeconds(1);

      assertTrusted(cm_a, a.address(), b.address(), c.address(), d.address());
      assertTrusted(cm_b, a.address(), b.address(), c.address(), d.address());
      assertTrusted(cm_c, a.address(), b.address(), c.address(), d.address());
      assertTrusted(cm_d, a.address(), b.address(), c.address(), d.address());

      stop(cm_c);
      stop(cm_d);

      awaitSeconds(1);

      assertTrusted(cm_a, a.address(), b.address());
      assertSuspected(cm_a, c.address(), d.address());
      assertTrusted(cm_b, a.address(), b.address());
      assertSuspected(cm_b, c.address(), d.address());

      long suspicionTimeoutSec =
          ClusterMath.suspicionTimeout(ClusterConfig.DEFAULT_SUSPICION_MULT, 4, TEST_PING_INTERVAL) / 1000;
      awaitSeconds(suspicionTimeoutSec + 1); // > max suspect time

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
  public void testLimitedSeedMembers() {
    Transport a = Transport.bindAwait(true);
    Transport b = Transport.bindAwait(true);
    Transport c = Transport.bindAwait(true);
    Transport d = Transport.bindAwait(true);
    Transport e = Transport.bindAwait(true);

    MembershipProtocolImpl cm_a = createMembership(a, Collections.emptyList());
    MembershipProtocolImpl cm_b = createMembership(b, Collections.singletonList(a.address()));
    MembershipProtocolImpl cm_c = createMembership(c, Collections.singletonList(a.address()));
    MembershipProtocolImpl cm_d = createMembership(d, Collections.singletonList(b.address()));
    MembershipProtocolImpl cm_e = createMembership(e, Collections.singletonList(b.address()));

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

  @Test
  public void testOverrideMemberAddress() throws UnknownHostException {
    String localAddress = InetAddress.getLocalHost().getHostName();

    Transport a = Transport.bindAwait(true);
    Transport b = Transport.bindAwait(true);
    Transport c = Transport.bindAwait(true);
    Transport d = Transport.bindAwait(true);
    Transport e = Transport.bindAwait(true);

    MembershipProtocolImpl cm_a = createMembership(a, testConfig(Collections.emptyList()).memberHost(localAddress).build());
    MembershipProtocolImpl cm_b = createMembership(b, testConfig(Collections.singletonList(a.address())).memberHost(localAddress).build());
    MembershipProtocolImpl cm_c = createMembership(c, testConfig(Collections.singletonList(a.address())).memberHost(localAddress).build());
    MembershipProtocolImpl cm_d = createMembership(d, testConfig(Collections.singletonList(b.address())).memberHost(localAddress).build());
    MembershipProtocolImpl cm_e = createMembership(e, testConfig(Collections.singletonList(b.address())).memberHost(localAddress).build());

    try {
      awaitSeconds(3);

      assertTrusted(cm_a, cm_a.member().address(), cm_b.member().address(), cm_c.member().address(), cm_d.member().address(), cm_e.member().address());
      assertNoSuspected(cm_a);
      assertTrusted(cm_b, cm_a.member().address(), cm_b.member().address(), cm_c.member().address(), cm_d.member().address(), cm_e.member().address());
      assertNoSuspected(cm_b);
      assertTrusted(cm_c, cm_a.member().address(), cm_b.member().address(), cm_c.member().address(), cm_d.member().address(), cm_e.member().address());
      assertNoSuspected(cm_c);
      assertTrusted(cm_d, cm_a.member().address(), cm_b.member().address(), cm_c.member().address(), cm_d.member().address(), cm_e.member().address());
      assertNoSuspected(cm_d);
      assertTrusted(cm_e, cm_a.member().address(), cm_b.member().address(), cm_c.member().address(), cm_d.member().address(), cm_e.member().address());
      assertNoSuspected(cm_e);
    } finally {
      stopAll(cm_a, cm_b, cm_c, cm_d, cm_e);
    }
  }

  @Test
  public void testMemberAddressOverrides()  {
    Transport t = Transport.bindAwait(true);
    String host = "host1";

    // Default behavior
    Address address = MembershipProtocolImpl.memberAddress(t,
            testConfig(Collections.emptyList())
            .build());
    assertEquals(t.address(), address);

    // Override host only
    address = MembershipProtocolImpl.memberAddress(t,
              testConfig(Collections.emptyList())
                .memberHost(host)
              .build());
    assertEquals(Address.create(host, t.address().port()), address);

    // Override host and port
    address = MembershipProtocolImpl.memberAddress(t,
              testConfig(Collections.emptyList())
                .memberHost(host).memberPort(80)
              .build());
    assertEquals(Address.create(host, 80), address);

    // Override port only (override is ignored)
    address = MembershipProtocolImpl.memberAddress(t,
            testConfig(Collections.emptyList())
                .memberPort(8080)
            .build());
    assertEquals(t.address(), address);
  }

  private void awaitSeconds(long seconds) {
    try {
      TimeUnit.SECONDS.sleep(seconds);
    } catch (InterruptedException e) {
      Throwables.propagate(e);
    }
  }

  private ClusterConfig overrideConfig(Address seedAddress, String memberHost) {
    return testConfig(seedAddress != null
            ? Collections.singletonList(seedAddress)
            : Collections.emptyList()
    ).memberHost(memberHost).build();
  }

  private ClusterConfig.Builder testConfig(List<Address> seedAddresses) {
    // Create faster config for local testing
    return ClusterConfig.builder()
            .seedMembers(seedAddresses)
            .syncInterval(2000)
            .syncTimeout(1000)
            .pingInterval(TEST_PING_INTERVAL)
            .pingTimeout(100);
  }

  private MembershipProtocolImpl createMembership(Transport transport, List<Address> seedAddresses) {
    return createMembership(transport, testConfig(seedAddresses).build());
  }

  private MembershipProtocolImpl createMembership(Transport transport, ClusterConfig config) {
    // Create components
    MembershipProtocolImpl membership = new MembershipProtocolImpl(transport, config);
    FailureDetectorImpl failureDetector = new FailureDetectorImpl(transport, membership, config);
    GossipProtocolImpl gossipProtocol = new GossipProtocolImpl(transport, membership, config);
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

  private void stopAll(MembershipProtocolImpl... memberships) {
    for (MembershipProtocolImpl membership : memberships) {
      if (membership != null) {
        stop(membership);
      }
    }
  }

  private void stop(MembershipProtocolImpl membership) {
    membership.stop();
    membership.getGossipProtocol().stop();
    membership.getFailureDetector().stop();

    Transport transport = membership.getTransport();
    CompletableFuture<Void> close = new CompletableFuture<>();
    transport.stop(close);
    try {
      close.get(1, TimeUnit.SECONDS);
    } catch (Exception ignore) {
      // ignore
    }
  }

  private void assertTrusted(MembershipProtocolImpl membership, Address... expected) {
    List<Address> actual = getAddressesWithStatus(membership, MemberStatus.ALIVE);
    assertEquals("Expected " + expected.length + " trusted members " + Arrays.toString(expected)
        + ", but actual: " + actual, expected.length, actual.size());
    for (Address member : expected) {
      assertTrue("Expected to trust " + member + ", but actual: " + actual, actual.contains(member));
    }
  }

  private void assertSuspected(MembershipProtocolImpl membership, Address... expected) {
    List<Address> actual = getAddressesWithStatus(membership, MemberStatus.SUSPECT);
    assertEquals("Expected " + expected.length + " suspect members " + Arrays.toString(expected)
        + ", but actual: " + actual, expected.length, actual.size());
    for (Address member : expected) {
      assertTrue("Expected to suspect " + member + ", but actual: " + actual, actual.contains(member));
    }
  }

  private void assertNoSuspected(MembershipProtocolImpl membership) {
    List<Address> actual = getAddressesWithStatus(membership, MemberStatus.SUSPECT);
    assertEquals("Expected no suspected, but actual: " + actual, 0, actual.size());
  }

  private List<Address> getAddressesWithStatus(MembershipProtocolImpl membership, MemberStatus status) {
    return membership.getMembershipRecords().stream()
        .filter(member -> member.status() == status)
        .map(MembershipRecord::address)
        .collect(Collectors.toList());
  }
}
