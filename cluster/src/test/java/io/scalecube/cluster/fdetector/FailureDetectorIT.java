package io.scalecube.cluster.fdetector;

import static com.google.common.base.Preconditions.checkArgument;
import static io.scalecube.cluster.membership.MemberStatus.SUSPECTED;
import static io.scalecube.cluster.membership.MemberStatus.TRUSTED;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import io.scalecube.cluster.membership.MemberStatus;
import io.scalecube.transport.Address;
import io.scalecube.transport.ITransport;
import io.scalecube.transport.Transport;
import io.scalecube.transport.TransportConfig;

import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;

import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class FailureDetectorIT {

  @Test
  public void testTrusted() {
    // Create transports
    Transport a = Transport.bindAwait(true);
    Transport b = Transport.bindAwait(true);
    Transport c = Transport.bindAwait(true);
    List<Address> members = Arrays.asList(a.address(), b.address(), c.address());

    // Create failure detectors
    FailureDetector fd_a = createFailureDetector(a, members);
    FailureDetector fd_b = createFailureDetector(b, members);
    FailureDetector fd_c = createFailureDetector(c, members);
    List<FailureDetector> fdetectors = Arrays.asList(fd_a, fd_b, fd_c);

    try {
      start(fdetectors);

      Future<List<FailureDetectorEvent>> list_a = listenEvents(fd_a);
      Future<List<FailureDetectorEvent>> list_b = listenEvents(fd_b);
      Future<List<FailureDetectorEvent>> list_c = listenEvents(fd_c);

      assertStatus(a.address(), TRUSTED, awaitEvents(list_a), b.address(), c.address());
      assertStatus(b.address(), TRUSTED, awaitEvents(list_b), a.address(), c.address());
      assertStatus(c.address(), TRUSTED, awaitEvents(list_c), a.address(), b.address());
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
    FailureDetector fd_a = createFailureDetector(a, members);
    FailureDetector fd_b = createFailureDetector(b, members);
    FailureDetector fd_c = createFailureDetector(c, members);
    List<FailureDetector> fdetectors = Arrays.asList(fd_a, fd_b, fd_c);

    // block all traffic
    a.block(members);
    b.block(members);
    c.block(members);

    try {
      start(fdetectors);

      Future<List<FailureDetectorEvent>> list_a = listenEvents(fd_a);
      Future<List<FailureDetectorEvent>> list_b = listenEvents(fd_b);
      Future<List<FailureDetectorEvent>> list_c = listenEvents(fd_c);

      assertStatus(a.address(), SUSPECTED, awaitEvents(list_a), b.address(), c.address());
      assertStatus(b.address(), SUSPECTED, awaitEvents(list_b), a.address(), c.address());
      assertStatus(c.address(), SUSPECTED, awaitEvents(list_c), a.address(), b.address());
    } finally {
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
    FailureDetector fd_a = createFailureDetector(a, members);
    FailureDetector fd_b = createFailureDetector(b, members);
    FailureDetector fd_c = createFailureDetector(c, members);
    List<FailureDetector> fdetectors = Arrays.asList(fd_a, fd_b, fd_c);

    // Traffic issue at connection A -> B
    a.block(b.address());

    Future<List<FailureDetectorEvent>> list_a = listenEvents(fd_a);
    Future<List<FailureDetectorEvent>> list_b = listenEvents(fd_b);
    Future<List<FailureDetectorEvent>> list_c = listenEvents(fd_c);

    try {
      start(fdetectors);

      assertStatus(a.address(), TRUSTED, awaitEvents(list_a), b.address(), c.address());
      assertStatus(b.address(), TRUSTED, awaitEvents(list_b), a.address(), c.address());
      assertStatus(c.address(), TRUSTED, awaitEvents(list_c), a.address(), b.address());
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
    FailureDetector fd_a = createFailureDetector(a, members);
    FailureDetector fd_b =
        createFailureDetector(b, members, FailureDetectorConfig.builder().pingTimeout(500).pingTime(1000).build());
    FailureDetector fd_c = createFailureDetector(c, members, FailureDetectorConfig.defaultConfig());
    List<FailureDetector> fdetectors = Arrays.asList(fd_a, fd_b, fd_c);

    try {
      start(fdetectors);

      Future<List<FailureDetectorEvent>> list_a = listenEvents(fd_a);
      Future<List<FailureDetectorEvent>> list_b = listenEvents(fd_b);
      Future<List<FailureDetectorEvent>> list_c = listenEvents(fd_c);

      assertStatus(a.address(), TRUSTED, awaitEvents(list_a), b.address(), c.address());
      assertStatus(b.address(), TRUSTED, awaitEvents(list_b), a.address(), c.address());
      assertStatus(c.address(), TRUSTED, awaitEvents(list_c), a.address(), b.address());
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
    FailureDetector fd_a = createFailureDetector(a, members);
    FailureDetector fd_b = createFailureDetector(b, members);
    FailureDetector fd_c = createFailureDetector(c, members);
    FailureDetector fd_d = createFailureDetector(d, members);
    List<FailureDetector> fdetectors = Arrays.asList(fd_a, fd_b, fd_c, fd_d);

    // Block traffic on member A to all cluster members
    a.block(members);

    try {
      Future<List<FailureDetectorEvent>> list_a = listenEvents(fd_a);
      Future<List<FailureDetectorEvent>> list_b = listenEvents(fd_b);
      Future<List<FailureDetectorEvent>> list_c = listenEvents(fd_c);
      Future<List<FailureDetectorEvent>> list_d = listenEvents(fd_d);

      start(fdetectors);

      assertStatus(a.address(), SUSPECTED, awaitEvents(list_a), b.address(), c.address(), d.address()); // node A
      // partitioned
      assertStatus(b.address(), SUSPECTED, awaitEvents(list_b), a.address());
      assertStatus(c.address(), SUSPECTED, awaitEvents(list_c), a.address());
      assertStatus(d.address(), SUSPECTED, awaitEvents(list_d), a.address());

      // Unblock traffic on member A
      a.unblockAll();
      TimeUnit.SECONDS.sleep(4);

      list_a = listenEvents(fd_a);
      list_b = listenEvents(fd_b);
      list_c = listenEvents(fd_c);
      list_d = listenEvents(fd_d);

      // Check member A recovers

      assertStatus(a.address(), TRUSTED, awaitEvents(list_a), b.address(), c.address(), d.address());
      assertStatus(b.address(), TRUSTED, awaitEvents(list_b), a.address(), c.address(), d.address());
      assertStatus(c.address(), TRUSTED, awaitEvents(list_c), a.address(), b.address(), d.address());
      assertStatus(d.address(), TRUSTED, awaitEvents(list_d), a.address(), b.address(), c.address());
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
    FailureDetector fd_a = createFailureDetector(a, members);
    FailureDetector fd_b = createFailureDetector(b, members);
    FailureDetector fd_c = createFailureDetector(c, members);
    FailureDetector fd_d = createFailureDetector(d, members);
    List<FailureDetector> fdetectors = Arrays.asList(fd_a, fd_b, fd_c, fd_d);

    // Block traffic to node D on other members
    a.block(d.address());
    b.block(d.address());
    c.block(d.address());

    try {
      Future<List<FailureDetectorEvent>> list_a = listenEvents(fd_a);
      Future<List<FailureDetectorEvent>> list_b = listenEvents(fd_b);
      Future<List<FailureDetectorEvent>> list_c = listenEvents(fd_c);
      Future<List<FailureDetectorEvent>> list_d = listenEvents(fd_d);

      start(fdetectors);

      assertStatus(a.address(), SUSPECTED, awaitEvents(list_a), d.address());
      assertStatus(b.address(), SUSPECTED, awaitEvents(list_b), d.address());
      assertStatus(c.address(), SUSPECTED, awaitEvents(list_c), d.address());
      assertStatus(d.address(), SUSPECTED, awaitEvents(list_d), a.address(), b.address(), c.address()); // node D
      // partitioned

      // Unblock traffic to member D on other members
      a.unblockAll();
      b.unblockAll();
      c.unblockAll();
      TimeUnit.SECONDS.sleep(4);

      list_a = listenEvents(fd_a);
      list_b = listenEvents(fd_b);
      list_c = listenEvents(fd_c);
      list_d = listenEvents(fd_d);

      // Check member D recovers

      assertStatus(a.address(), TRUSTED, awaitEvents(list_a), b.address(), c.address(), d.address());
      assertStatus(b.address(), TRUSTED, awaitEvents(list_b), a.address(), c.address(), d.address());
      assertStatus(c.address(), TRUSTED, awaitEvents(list_c), a.address(), b.address(), d.address());
      assertStatus(d.address(), TRUSTED, awaitEvents(list_d), a.address(), b.address(), c.address());
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
    FailureDetector fd_a = createFailureDetector(a, members);
    FailureDetector fd_b = createFailureDetector(b, members);
    List<FailureDetector> fdetectors = Arrays.asList(fd_a, fd_b);

    // Traffic is blocked initially on both sides: A--X-->B, B--X-->A
    a.block(b.address());
    b.block(a.address());

    Future<List<FailureDetectorEvent>> list_a = listenEvents(fd_a);
    Future<List<FailureDetectorEvent>> list_b = listenEvents(fd_b);

    try {
      start(fdetectors);

      assertStatus(a.address(), SUSPECTED, awaitEvents(list_a), b.address());
      assertStatus(b.address(), SUSPECTED, awaitEvents(list_b), a.address());

      // Unblock A and B members: A-->B, B-->A
      a.unblockAll();
      b.unblockAll();
      TimeUnit.SECONDS.sleep(2);

      // Check that members recover

      list_a = listenEvents(fd_a);
      list_b = listenEvents(fd_b);

      assertStatus(a.address(), TRUSTED, awaitEvents(list_a), b.address());
      assertStatus(b.address(), TRUSTED, awaitEvents(list_b), a.address());
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
    FailureDetector fd_a = createFailureDetector(a, members);
    FailureDetector fd_b = createFailureDetector(b, members);
    FailureDetector fd_x = createFailureDetector(x, members);
    List<FailureDetector> fdetectors = Arrays.asList(fd_a, fd_b, fd_x);

    Future<List<FailureDetectorEvent>> list_a = listenEvents(fd_a);
    Future<List<FailureDetectorEvent>> list_b = listenEvents(fd_b);
    Future<List<FailureDetectorEvent>> list_x = listenEvents(fd_x);

    // Restarted member attributes are not initialized
    Transport xx;
    FailureDetector fd_xx;

    try {
      start(fdetectors);

      assertStatus(a.address(), TRUSTED, awaitEvents(list_a), b.address(), x.address());
      assertStatus(b.address(), TRUSTED, awaitEvents(list_b), a.address(), x.address());
      assertStatus(x.address(), TRUSTED, awaitEvents(list_x), a.address(), b.address());

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
      fdetectors = Arrays.asList(fd_a, fd_b, fd_xx = createFailureDetector(xx, members));

      // actual restart here
      fd_xx.start();
      TimeUnit.SECONDS.sleep(2);

      list_a = listenEvents(fd_a);
      list_b = listenEvents(fd_b);
      Future<List<FailureDetectorEvent>> list_xx = listenEvents(fd_xx);

      // TODO [AK]: It would be more correct to consider restarted member as a new member, so x is still suspected!

      assertStatus(a.address(), TRUSTED, awaitEvents(list_a), b.address(), xx.address());
      assertStatus(b.address(), TRUSTED, awaitEvents(list_b), a.address(), xx.address());
      assertStatus(xx.address(), TRUSTED, awaitEvents(list_xx), a.address(), b.address());
    } finally {
      stop(fdetectors);
    }
  }

  private FailureDetector createFailureDetector(Transport transport, List<Address> members) {
    FailureDetectorConfig failureDetectorConfig = FailureDetectorConfig.builder() // faster config for local testing
        .pingTimeout(100)
        .pingTime(200)
        .pingReqMembers(2)
        .build();
    FailureDetector failureDetector = new FailureDetector(transport, failureDetectorConfig);
    failureDetector.setMembers(members);
    return createFailureDetector(transport, members, failureDetectorConfig);
  }

  private FailureDetector createFailureDetector(Transport transport, List<Address> members, FailureDetectorConfig config) {
    FailureDetector failureDetector = new FailureDetector(transport, config);
    failureDetector.setMembers(members);
    return failureDetector;
  }

  private void destroyTransport(ITransport transport) {
    if (((Transport) transport).isStopped()) {
      return;
    }
    SettableFuture<Void> close = SettableFuture.create();
    transport.stop(close);
    try {
      close.get(1, TimeUnit.SECONDS);
    } catch (Exception ignore) {
    }
  }

  private void start(List<FailureDetector> fdetectors) {
    for (FailureDetector fd : fdetectors) {
      fd.start();
    }
  }

  private void stop(List<FailureDetector> fdetectors) {
    for (FailureDetector fd : fdetectors) {
      destroyTransport(fd.getTransport());
      fd.stop();
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
        .map(FailureDetectorEvent::address)
        .collect(Collectors.toList());

    String msg1 = String.format("Node %s expected %s %s members %s, but was: %s",
        address, expected.length, status, Arrays.toString(expected), events);
    assertEquals(msg1, expected.length, actual.size());

    for (Address member : expected) {
      String msg2 = String.format("Node %s expected as %s %s, but was: %s", address, status, member, events);
      assertTrue(msg2, actual.contains(member));
    }
  }

  private Future<List<FailureDetectorEvent>> listenEvents(FailureDetector fd) {
    List<Address> members = fd.getMembers();
    checkArgument(!members.isEmpty());

    List<ListenableFuture<FailureDetectorEvent>> resultFuture = new ArrayList<>();
    for (final Address member : members) {
      final SettableFuture<FailureDetectorEvent> future = SettableFuture.create();
      fd.listenStatus()
          .filter(event -> event.address() == member)
          .subscribe(future::set);
      resultFuture.add(future);
    }

    return Futures.successfulAsList(resultFuture);
  }

  private Collection<FailureDetectorEvent> awaitEvents(Future<List<FailureDetectorEvent>> events) {
    try {
      return events.get(10, TimeUnit.SECONDS);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
