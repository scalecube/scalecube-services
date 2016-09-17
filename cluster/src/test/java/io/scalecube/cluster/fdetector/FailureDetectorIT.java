package io.scalecube.cluster.fdetector;

import static com.google.common.base.Preconditions.checkArgument;
import static io.scalecube.cluster.membership.MemberStatus.SUSPECT;
import static io.scalecube.cluster.membership.MemberStatus.ALIVE;
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
    FailureDetector fd_a = createFD(a, members);
    FailureDetector fd_b = createFD(b, members);
    FailureDetector fd_c = createFD(c, members);
    List<FailureDetector> fdetectors = Arrays.asList(fd_a, fd_b, fd_c);

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
    FailureDetector fd_a = createFD(a, members);
    FailureDetector fd_b = createFD(b, members);
    FailureDetector fd_c = createFD(c, members);
    List<FailureDetector> fdetectors = Arrays.asList(fd_a, fd_b, fd_c);

    // block all traffic
    a.block(members);
    b.block(members);
    c.block(members);

    try {
      start(fdetectors);

      Future<List<FailureDetectorEvent>> list_a = listenNextEventFor(fd_a, members);
      Future<List<FailureDetectorEvent>> list_b = listenNextEventFor(fd_b, members);
      Future<List<FailureDetectorEvent>> list_c = listenNextEventFor(fd_c, members);

      assertStatus(a.address(), SUSPECT, awaitEvents(list_a), b.address(), c.address());
      assertStatus(b.address(), SUSPECT, awaitEvents(list_b), a.address(), c.address());
      assertStatus(c.address(), SUSPECT, awaitEvents(list_c), a.address(), b.address());
    } finally {
      a.unblockAll();
      b.unblockAll();
      c.unblockAll();
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
    FailureDetector fd_a = createFD(a, members);
    FailureDetector fd_b = createFD(b, members);
    FailureDetector fd_c = createFD(c, members);
    List<FailureDetector> fdetectors = Arrays.asList(fd_a, fd_b, fd_c);

    // Traffic issue at connection A -> B
    a.block(b.address());

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
    FailureDetector fd_a = createFD(a, members);
    FailureDetectorConfig fd_b_config = FailureDetectorConfig.builder().pingTimeout(500).pingInterval(1000).build();
    FailureDetector fd_b = createFD(b, members, fd_b_config);
    FailureDetector fd_c = createFD(c, members, FailureDetectorConfig.defaultConfig());
    List<FailureDetector> fdetectors = Arrays.asList(fd_a, fd_b, fd_c);

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
    FailureDetector fd_a = createFD(a, members);
    FailureDetector fd_b = createFD(b, members);
    FailureDetector fd_c = createFD(c, members);
    FailureDetector fd_d = createFD(d, members);
    List<FailureDetector> fdetectors = Arrays.asList(fd_a, fd_b, fd_c, fd_d);

    // Block traffic on member A to all cluster members
    a.block(members);

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
      a.unblockAll();
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
    FailureDetector fd_a = createFD(a, members);
    FailureDetector fd_b = createFD(b, members);
    FailureDetector fd_c = createFD(c, members);
    FailureDetector fd_d = createFD(d, members);
    List<FailureDetector> fdetectors = Arrays.asList(fd_a, fd_b, fd_c, fd_d);

    // Block traffic to node D on other members
    a.block(d.address());
    b.block(d.address());
    c.block(d.address());

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
      a.unblockAll();
      b.unblockAll();
      c.unblockAll();
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
    FailureDetector fd_a = createFD(a, members);
    FailureDetector fd_b = createFD(b, members);
    List<FailureDetector> fdetectors = Arrays.asList(fd_a, fd_b);

    // Traffic is blocked initially on both sides: A--X-->B, B--X-->A
    a.block(b.address());
    b.block(a.address());

    Future<List<FailureDetectorEvent>> list_a = listenNextEventFor(fd_a, members);
    Future<List<FailureDetectorEvent>> list_b = listenNextEventFor(fd_b, members);

    try {
      start(fdetectors);

      assertStatus(a.address(), SUSPECT, awaitEvents(list_a), b.address());
      assertStatus(b.address(), SUSPECT, awaitEvents(list_b), a.address());

      // Unblock A and B members: A-->B, B-->A
      a.unblockAll();
      b.unblockAll();
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
    FailureDetector fd_a = createFD(a, members);
    FailureDetector fd_b = createFD(b, members);
    FailureDetector fd_x = createFD(x, members);
    List<FailureDetector> fdetectors = Arrays.asList(fd_a, fd_b, fd_x);

    Future<List<FailureDetectorEvent>> list_a = listenNextEventFor(fd_a, members);
    Future<List<FailureDetectorEvent>> list_b = listenNextEventFor(fd_b, members);
    Future<List<FailureDetectorEvent>> list_x = listenNextEventFor(fd_x, members);

    // Restarted member attributes are not initialized
    Transport xx;
    FailureDetector fd_xx;

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

  private FailureDetector createFD(Transport transport, List<Address> members) {
    FailureDetectorConfig failureDetectorConfig = FailureDetectorConfig.builder() // faster config for local testing
        .pingTimeout(100)
        .pingInterval(200)
        .pingReqMembers(2)
        .build();
    return createFD(transport, members, failureDetectorConfig);
  }

  private FailureDetector createFD(Transport transport, List<Address> members, FailureDetectorConfig config) {
    FailureDetector failureDetector = new FailureDetector(transport, config);
    failureDetector.setPingMembers(members);
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
      fd.stop();
    }
    for (FailureDetector fd : fdetectors) {
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

  private Future<List<FailureDetectorEvent>> listenNextEventFor(FailureDetector fd, List<Address> members) {
    members = new ArrayList<>(members);
    members.remove(fd.getTransport().address()); // exclude self
    checkArgument(!members.isEmpty());

    List<ListenableFuture<FailureDetectorEvent>> resultFuture = new ArrayList<>();
    for (final Address member : members) {
      final SettableFuture<FailureDetectorEvent> future = SettableFuture.create();
      fd.listen()
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
