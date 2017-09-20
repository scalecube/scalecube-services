package io.scalecube.cluster.fdetector;

import static com.google.common.base.Preconditions.checkArgument;
import static io.scalecube.cluster.membership.MemberStatus.SUSPECT;
import static io.scalecube.cluster.membership.MemberStatus.ALIVE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import io.scalecube.cluster.ClusterConfig;
import io.scalecube.cluster.Member;
import io.scalecube.cluster.membership.DummyMembershipProtocol;
import io.scalecube.cluster.membership.MembershipProtocol;
import io.scalecube.cluster.membership.MemberStatus;
import io.scalecube.testlib.BaseTest;
import io.scalecube.transport.Address;
import io.scalecube.transport.Transport;
import io.scalecube.transport.TransportConfig;

import com.google.common.collect.Lists;

import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class FailureDetectorTest extends BaseTest {

  @Test
  public void testTrusted() {
    // Create transports
    Transport a = Transport.bindAwait(true);
    Transport b = Transport.bindAwait(true);
    Transport c = Transport.bindAwait(true);
    List<Address> members = Arrays.asList(a.address(), b.address(), c.address());

    // Create failure detectors
    FailureDetectorImpl fd_a = createFD(a, members);
    FailureDetectorImpl fd_b = createFD(b, members);
    FailureDetectorImpl fd_c = createFD(c, members);
    List<FailureDetectorImpl> fdetectors = Arrays.asList(fd_a, fd_b, fd_c);

    try {
      start(fdetectors);

      Future<List<FailureDetectorEvent>> list_a = listenNextEventFor(fd_a, members);
      Future<List<FailureDetectorEvent>> list_b = listenNextEventFor(fd_b, members);
      Future<List<FailureDetectorEvent>> list_c = listenNextEventFor(fd_c, members);

      assertStatus(a.address(), ALIVE, awaitEvents(list_a), b.address(), c.address());
      assertStatus(b.address(), ALIVE, awaitEvents(list_b), a.address(), c.address());
      assertStatus(c.address(), ALIVE, awaitEvents(list_c), a.address(), b.address());
    } finally {
      stop(fdetectors);
    }
  }

  @Test
  public void testSuspected() {
    // Create transports
    Transport a = Transport.bindAwait(true);
    Transport b = Transport.bindAwait(true);
    Transport c = Transport.bindAwait(true);
    List<Address> members = Arrays.asList(a.address(), b.address(), c.address());

    // Create failure detectors
    FailureDetectorImpl fd_a = createFD(a, members);
    FailureDetectorImpl fd_b = createFD(b, members);
    FailureDetectorImpl fd_c = createFD(c, members);
    List<FailureDetectorImpl> fdetectors = Arrays.asList(fd_a, fd_b, fd_c);

    // block all traffic
    a.networkEmulator().block(members);
    b.networkEmulator().block(members);
    c.networkEmulator().block(members);

    try {
      start(fdetectors);

      Future<List<FailureDetectorEvent>> list_a = listenNextEventFor(fd_a, members);
      Future<List<FailureDetectorEvent>> list_b = listenNextEventFor(fd_b, members);
      Future<List<FailureDetectorEvent>> list_c = listenNextEventFor(fd_c, members);

      assertStatus(a.address(), SUSPECT, awaitEvents(list_a), b.address(), c.address());
      assertStatus(b.address(), SUSPECT, awaitEvents(list_b), a.address(), c.address());
      assertStatus(c.address(), SUSPECT, awaitEvents(list_c), a.address(), b.address());
    } finally {
      a.networkEmulator().unblockAll();
      b.networkEmulator().unblockAll();
      c.networkEmulator().unblockAll();
      stop(fdetectors);
    }
  }

  @Test
  public void testTrustedDespiteBadNetwork() throws Exception {
    // Create transports
    Transport a = Transport.bindAwait(true);
    Transport b = Transport.bindAwait(true);
    Transport c = Transport.bindAwait(true);
    List<Address> members = Arrays.asList(a.address(), b.address(), c.address());

    // Create failure detectors
    FailureDetectorImpl fd_a = createFD(a, members);
    FailureDetectorImpl fd_b = createFD(b, members);
    FailureDetectorImpl fd_c = createFD(c, members);
    List<FailureDetectorImpl> fdetectors = Arrays.asList(fd_a, fd_b, fd_c);

    // Traffic issue at connection A -> B
    a.networkEmulator().block(b.address());

    Future<List<FailureDetectorEvent>> list_a = listenNextEventFor(fd_a, members);
    Future<List<FailureDetectorEvent>> list_b = listenNextEventFor(fd_b, members);
    Future<List<FailureDetectorEvent>> list_c = listenNextEventFor(fd_c, members);

    try {
      start(fdetectors);

      assertStatus(a.address(), ALIVE, awaitEvents(list_a), b.address(), c.address());
      assertStatus(b.address(), ALIVE, awaitEvents(list_b), a.address(), c.address());
      assertStatus(c.address(), ALIVE, awaitEvents(list_c), a.address(), b.address());
    } finally {
      stop(fdetectors);
    }
  }

  @Test
  public void testTrustedDespiteDifferentPingTimings() throws Exception {
    // Create transports
    Transport a = Transport.bindAwait(true);
    Transport b = Transport.bindAwait(true);
    Transport c = Transport.bindAwait(true);
    List<Address> members = Arrays.asList(a.address(), b.address(), c.address());

    // Create failure detectors
    FailureDetectorImpl fd_a = createFD(a, members);
    FailureDetectorConfig fd_b_config = ClusterConfig.builder().pingTimeout(500).pingInterval(1000).build();
    FailureDetectorImpl fd_b = createFD(b, members, fd_b_config);
    FailureDetectorImpl fd_c = createFD(c, members, ClusterConfig.defaultConfig());
    List<FailureDetectorImpl> fdetectors = Arrays.asList(fd_a, fd_b, fd_c);

    try {
      start(fdetectors);

      Future<List<FailureDetectorEvent>> list_a = listenNextEventFor(fd_a, members);
      Future<List<FailureDetectorEvent>> list_b = listenNextEventFor(fd_b, members);
      Future<List<FailureDetectorEvent>> list_c = listenNextEventFor(fd_c, members);

      assertStatus(a.address(), ALIVE, awaitEvents(list_a), b.address(), c.address());
      assertStatus(b.address(), ALIVE, awaitEvents(list_b), a.address(), c.address());
      assertStatus(c.address(), ALIVE, awaitEvents(list_c), a.address(), b.address());
    } finally {
      stop(fdetectors);
    }
  }

  @Test
  public void testSuspectedMemberWithBadNetworkGetsPartitioned() throws Exception {
    // Create transports
    Transport a = Transport.bindAwait(true);
    Transport b = Transport.bindAwait(true);
    Transport c = Transport.bindAwait(true);
    Transport d = Transport.bindAwait(true);
    List<Address> members = Arrays.asList(a.address(), b.address(), c.address(), d.address());

    // Create failure detectors
    FailureDetectorImpl fd_a = createFD(a, members);
    FailureDetectorImpl fd_b = createFD(b, members);
    FailureDetectorImpl fd_c = createFD(c, members);
    FailureDetectorImpl fd_d = createFD(d, members);
    List<FailureDetectorImpl> fdetectors = Arrays.asList(fd_a, fd_b, fd_c, fd_d);

    // Block traffic on member A to all cluster members
    a.networkEmulator().block(members);

    try {
      Future<List<FailureDetectorEvent>> list_a = listenNextEventFor(fd_a, members);
      Future<List<FailureDetectorEvent>> list_b = listenNextEventFor(fd_b, members);
      Future<List<FailureDetectorEvent>> list_c = listenNextEventFor(fd_c, members);
      Future<List<FailureDetectorEvent>> list_d = listenNextEventFor(fd_d, members);

      start(fdetectors);

      assertStatus(a.address(), SUSPECT, awaitEvents(list_a), b.address(), c.address(), d.address()); // node A
      // partitioned
      assertStatus(b.address(), SUSPECT, awaitEvents(list_b), a.address());
      assertStatus(c.address(), SUSPECT, awaitEvents(list_c), a.address());
      assertStatus(d.address(), SUSPECT, awaitEvents(list_d), a.address());

      // Unblock traffic on member A
      a.networkEmulator().unblockAll();
      TimeUnit.SECONDS.sleep(4);

      list_a = listenNextEventFor(fd_a, members);
      list_b = listenNextEventFor(fd_b, members);
      list_c = listenNextEventFor(fd_c, members);
      list_d = listenNextEventFor(fd_d, members);

      // Check member A recovers

      assertStatus(a.address(), ALIVE, awaitEvents(list_a), b.address(), c.address(), d.address());
      assertStatus(b.address(), ALIVE, awaitEvents(list_b), a.address(), c.address(), d.address());
      assertStatus(c.address(), ALIVE, awaitEvents(list_c), a.address(), b.address(), d.address());
      assertStatus(d.address(), ALIVE, awaitEvents(list_d), a.address(), b.address(), c.address());
    } finally {
      stop(fdetectors);
    }
  }

  @Test
  public void testSuspectedMemberWithNormalNetworkGetsPartitioned() throws Exception {
    // Create transports
    Transport a = Transport.bindAwait(true);
    Transport b = Transport.bindAwait(true);
    Transport c = Transport.bindAwait(true);
    Transport d = Transport.bindAwait(true);
    List<Address> members = Arrays.asList(a.address(), b.address(), c.address(), d.address());

    // Create failure detectors
    FailureDetectorImpl fd_a = createFD(a, members);
    FailureDetectorImpl fd_b = createFD(b, members);
    FailureDetectorImpl fd_c = createFD(c, members);
    FailureDetectorImpl fd_d = createFD(d, members);
    List<FailureDetectorImpl> fdetectors = Arrays.asList(fd_a, fd_b, fd_c, fd_d);

    // Block traffic to node D on other members
    a.networkEmulator().block(d.address());
    b.networkEmulator().block(d.address());
    c.networkEmulator().block(d.address());

    try {
      Future<List<FailureDetectorEvent>> list_a = listenNextEventFor(fd_a, members);
      Future<List<FailureDetectorEvent>> list_b = listenNextEventFor(fd_b, members);
      Future<List<FailureDetectorEvent>> list_c = listenNextEventFor(fd_c, members);
      Future<List<FailureDetectorEvent>> list_d = listenNextEventFor(fd_d, members);

      start(fdetectors);

      assertStatus(a.address(), SUSPECT, awaitEvents(list_a), d.address());
      assertStatus(b.address(), SUSPECT, awaitEvents(list_b), d.address());
      assertStatus(c.address(), SUSPECT, awaitEvents(list_c), d.address());
      assertStatus(d.address(), SUSPECT, awaitEvents(list_d), a.address(), b.address(), c.address()); // node D
      // partitioned

      // Unblock traffic to member D on other members
      a.networkEmulator().unblockAll();
      b.networkEmulator().unblockAll();
      c.networkEmulator().unblockAll();
      TimeUnit.SECONDS.sleep(4);

      list_a = listenNextEventFor(fd_a, members);
      list_b = listenNextEventFor(fd_b, members);
      list_c = listenNextEventFor(fd_c, members);
      list_d = listenNextEventFor(fd_d, members);

      // Check member D recovers

      assertStatus(a.address(), ALIVE, awaitEvents(list_a), b.address(), c.address(), d.address());
      assertStatus(b.address(), ALIVE, awaitEvents(list_b), a.address(), c.address(), d.address());
      assertStatus(c.address(), ALIVE, awaitEvents(list_c), a.address(), b.address(), d.address());
      assertStatus(d.address(), ALIVE, awaitEvents(list_d), a.address(), b.address(), c.address());
    } finally {
      stop(fdetectors);
    }
  }

  @Test
  public void testMemberStatusChangeAfterNetworkRecovery() throws Exception {
    // Create transports
    Transport a = Transport.bindAwait(true);
    Transport b = Transport.bindAwait(true);
    List<Address> members = Arrays.asList(a.address(), b.address());

    // Create failure detectors
    FailureDetectorImpl fd_a = createFD(a, members);
    FailureDetectorImpl fd_b = createFD(b, members);
    List<FailureDetectorImpl> fdetectors = Arrays.asList(fd_a, fd_b);

    // Traffic is blocked initially on both sides: A--X-->B, B--X-->A
    a.networkEmulator().block(b.address());
    b.networkEmulator().block(a.address());

    Future<List<FailureDetectorEvent>> list_a = listenNextEventFor(fd_a, members);
    Future<List<FailureDetectorEvent>> list_b = listenNextEventFor(fd_b, members);

    try {
      start(fdetectors);

      assertStatus(a.address(), SUSPECT, awaitEvents(list_a), b.address());
      assertStatus(b.address(), SUSPECT, awaitEvents(list_b), a.address());

      // Unblock A and B members: A-->B, B-->A
      a.networkEmulator().unblockAll();
      b.networkEmulator().unblockAll();
      TimeUnit.SECONDS.sleep(2);

      // Check that members recover

      list_a = listenNextEventFor(fd_a, members);
      list_b = listenNextEventFor(fd_b, members);

      assertStatus(a.address(), ALIVE, awaitEvents(list_a), b.address());
      assertStatus(b.address(), ALIVE, awaitEvents(list_b), a.address());
    } finally {
      stop(fdetectors);
    }
  }

  @Test
  public void testStatusChangeAfterMemberRestart() throws Exception {
    // Create transports
    Transport a = Transport.bindAwait(true);
    Transport b = Transport.bindAwait(true);
    Transport x = Transport.bindAwait(true);
    List<Address> members = Arrays.asList(a.address(), b.address(), x.address());

    // Create failure detectors
    FailureDetectorImpl fd_a = createFD(a, members);
    FailureDetectorImpl fd_b = createFD(b, members);
    FailureDetectorImpl fd_x = createFD(x, members);
    List<FailureDetectorImpl> fdetectors = Arrays.asList(fd_a, fd_b, fd_x);

    Future<List<FailureDetectorEvent>> list_a = listenNextEventFor(fd_a, members);
    Future<List<FailureDetectorEvent>> list_b = listenNextEventFor(fd_b, members);
    Future<List<FailureDetectorEvent>> list_x = listenNextEventFor(fd_x, members);

    // Restarted member attributes are not initialized
    Transport xx;
    FailureDetectorImpl fd_xx;

    try {
      start(fdetectors);

      assertStatus(a.address(), ALIVE, awaitEvents(list_a), b.address(), x.address());
      assertStatus(b.address(), ALIVE, awaitEvents(list_b), a.address(), x.address());
      assertStatus(x.address(), ALIVE, awaitEvents(list_x), a.address(), b.address());

      // stop node X
      stop(Lists.newArrayList(fd_x));
      TimeUnit.SECONDS.sleep(2);

      // restart node X as XX
      xx = Transport.bindAwait(TransportConfig.builder()
          .port(x.address().port())
          .portAutoIncrement(false)
          .useNetworkEmulator(true)
          .build());
      assertEquals(x.address(), xx.address());
      fdetectors = Arrays.asList(fd_a, fd_b, fd_xx = createFD(xx, members));

      // actual restart here
      fd_xx.start();
      TimeUnit.SECONDS.sleep(2);

      list_a = listenNextEventFor(fd_a, members);
      list_b = listenNextEventFor(fd_b, members);
      Future<List<FailureDetectorEvent>> list_xx = listenNextEventFor(fd_xx, members);

      // TODO [AK]: It would be more correct to consider restarted member as a new member, so x is still suspected!

      assertStatus(a.address(), ALIVE, awaitEvents(list_a), b.address(), xx.address());
      assertStatus(b.address(), ALIVE, awaitEvents(list_b), a.address(), xx.address());
      assertStatus(xx.address(), ALIVE, awaitEvents(list_xx), a.address(), b.address());
    } finally {
      stop(fdetectors);
    }
  }

  private FailureDetectorImpl createFD(Transport transport, List<Address> members) {
    FailureDetectorConfig failureDetectorConfig = ClusterConfig.builder() // faster config for local testing
        .pingTimeout(100)
        .pingInterval(200)
        .pingReqMembers(2)
        .build();
    return createFD(transport, members, failureDetectorConfig);
  }

  private FailureDetectorImpl createFD(Transport transport, List<Address> addresses, FailureDetectorConfig config) {
    MembershipProtocol dummyMembership = new DummyMembershipProtocol(transport.address(), addresses);
    return new FailureDetectorImpl(transport, dummyMembership, config);
  }

  private void destroyTransport(Transport transport) {
    if (transport == null || transport.isStopped()) {
      return;
    }
    CompletableFuture<Void> close = new CompletableFuture<>();
    transport.stop(close);
    try {
      close.get(1, TimeUnit.SECONDS);
    } catch (Exception ignore) {
    }
  }

  private void start(List<FailureDetectorImpl> fdetectors) {
    for (FailureDetectorImpl fd : fdetectors) {
      fd.start();
    }
  }

  private void stop(List<FailureDetectorImpl> fdetectors) {
    for (FailureDetectorImpl fd : fdetectors) {
      fd.stop();
    }
    for (FailureDetectorImpl fd : fdetectors) {
      destroyTransport(fd.getTransport());
    }
  }

  /**
   * @param address target member to expect on
   * @param status expected listen status
   * @param events events collection of failure detector events
   * @param expected expected members of the given listenStatus
   */
  private void assertStatus(
      Address address, MemberStatus status, Collection<FailureDetectorEvent> events, Address... expected) {
    List<Address> actual = events.stream()
        .filter(event -> event.status() == status)
        .map(FailureDetectorEvent::member)
        .map(Member::address)
        .collect(Collectors.toList());

    String msg1 = String.format("Node %s expected %s %s members %s, but was: %s",
        address, expected.length, status, Arrays.toString(expected), events);
    assertEquals(msg1, expected.length, actual.size());

    for (Address member : expected) {
      String msg2 = String.format("Node %s expected as %s %s, but was: %s", address, status, member, events);
      assertTrue(msg2, actual.contains(member));
    }
  }

  private Future<List<FailureDetectorEvent>> listenNextEventFor(FailureDetectorImpl fd, List<Address> addresses) {
    addresses = new ArrayList<>(addresses);
    addresses.remove(fd.getTransport().address()); // exclude self
    checkArgument(!addresses.isEmpty());

    List<CompletableFuture<FailureDetectorEvent>> resultFuture = new ArrayList<>();
    for (final Address member : addresses) {
      final CompletableFuture<FailureDetectorEvent> future = new CompletableFuture<>();
      fd.listen()
          .filter(event -> event.member().address() == member)
          .subscribe(future::complete);
      resultFuture.add(future);
    }

    return allOf(resultFuture);
  }

  private Collection<FailureDetectorEvent> awaitEvents(Future<List<FailureDetectorEvent>> events) {
    try {
      return events.get(10, TimeUnit.SECONDS);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
  private <T> CompletableFuture<List<T>> allOf(List<CompletableFuture<T>> futuresList) {
    CompletableFuture<Void> allFuturesResult =
            CompletableFuture.allOf(futuresList.toArray(new CompletableFuture[futuresList.size()]));
    return allFuturesResult.thenApply(v ->
            futuresList.stream().
                    map(CompletableFuture::join).
                    collect(Collectors.toList())
    );
  }
}
