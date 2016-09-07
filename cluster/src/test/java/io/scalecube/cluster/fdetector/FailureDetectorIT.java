package io.scalecube.cluster.fdetector;

import static com.google.common.base.Preconditions.checkArgument;
import static io.scalecube.cluster.ClusterMemberStatus.SUSPECTED;
import static io.scalecube.cluster.ClusterMemberStatus.TRUSTED;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import io.scalecube.cluster.ClusterMemberStatus;
import io.scalecube.transport.Address;
import io.scalecube.transport.ITransport;
import io.scalecube.transport.Transport;
import io.scalecube.transport.TransportConfig;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;

import org.junit.Test;

import rx.functions.Action1;
import rx.functions.Func1;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class FailureDetectorIT {

  @Test
  public void testTrusted() throws Exception {
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

      Future<List<FailureDetectorEvent>> list_a = listenStatus(fd_a);
      Future<List<FailureDetectorEvent>> list_b = listenStatus(fd_b);
      Future<List<FailureDetectorEvent>> list_c = listenStatus(fd_c);

      assertMember(a.address(), TRUSTED, get(list_a, 2), b.address(), c.address());
      assertMember(b.address(), TRUSTED, get(list_b, 2), a.address(), c.address());
      assertMember(c.address(), TRUSTED, get(list_c, 2), a.address(), b.address());
    } finally {
      stop(fdetectors);
    }
  }

  @Test
  public void testSuspected() throws Exception {
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

      Future<List<FailureDetectorEvent>> list_a = listenStatus(fd_a);
      Future<List<FailureDetectorEvent>> list_b = listenStatus(fd_b);
      Future<List<FailureDetectorEvent>> list_c = listenStatus(fd_c);

      assertMember(a.address(), SUSPECTED, get(list_a, 2), b.address(), c.address());
      assertMember(b.address(), SUSPECTED, get(list_b, 2), a.address(), c.address());
      assertMember(c.address(), SUSPECTED, get(list_c, 2), a.address(), b.address());
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
    FailureDetector fd_a = createFD(a, members);
    FailureDetector fd_b = createFD(b, members);
    FailureDetector fd_c = createFD(c, members);
    List<FailureDetector> fdetectors = Arrays.asList(fd_a, fd_b, fd_c);

    // Traffic issue at connection A -> B
    a.block(b.address());

    Future<List<FailureDetectorEvent>> list_a = listenStatus(fd_a);
    Future<List<FailureDetectorEvent>> list_b = listenStatus(fd_b);
    Future<List<FailureDetectorEvent>> list_c = listenStatus(fd_c);

    try {
      start(fdetectors);

      assertMember(a.address(), TRUSTED, get(list_a, 2), b.address(), c.address());
      assertMember(b.address(), TRUSTED, get(list_b, 2), a.address(), c.address());
      assertMember(c.address(), TRUSTED, get(list_c, 2), a.address(), b.address());
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
    FailureDetector fd_b =
        createFD(b, members, FailureDetectorConfig.builder().pingTimeout(1000).pingTime(500).build());
    FailureDetector fd_c = createFD(c, members, FailureDetectorConfig.defaultConfig());
    List<FailureDetector> fdetectors = Arrays.asList(fd_a, fd_b, fd_c);

    try {
      start(fdetectors);

      Future<List<FailureDetectorEvent>> list_a = listenStatus(fd_a);
      Future<List<FailureDetectorEvent>> list_b = listenStatus(fd_b);
      Future<List<FailureDetectorEvent>> list_c = listenStatus(fd_c);

      assertMember(a.address(), TRUSTED, get(list_a, 6), b.address(), c.address());
      assertMember(b.address(), TRUSTED, get(list_b, 6), a.address(), c.address());
      assertMember(c.address(), TRUSTED, get(list_c, 6), a.address(), b.address());
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
      Future<List<FailureDetectorEvent>> list_a = listenStatus(fd_a);
      Future<List<FailureDetectorEvent>> list_b = listenStatus(fd_b);
      Future<List<FailureDetectorEvent>> list_c = listenStatus(fd_c);
      Future<List<FailureDetectorEvent>> list_d = listenStatus(fd_d);

      start(fdetectors);

      assertMember(a.address(), SUSPECTED, get(list_a, 4), b.address(), c.address(), d.address()); // node A
                                                                                                   // partitioned
      assertMember(b.address(), SUSPECTED, get(list_b, 4), a.address());
      assertMember(c.address(), SUSPECTED, get(list_c, 4), a.address());
      assertMember(d.address(), SUSPECTED, get(list_d, 4), a.address());

      // Unblock traffic on member A
      a.unblockAll();
      TimeUnit.SECONDS.sleep(4);

      list_a = listenStatus(fd_a);
      list_b = listenStatus(fd_b);
      list_c = listenStatus(fd_c);
      list_d = listenStatus(fd_d);

      // Check member A recovers

      assertMember(a.address(), TRUSTED, get(list_a, 4), b.address(), c.address(), d.address());
      assertMember(b.address(), TRUSTED, get(list_b, 4), a.address(), c.address(), d.address());
      assertMember(c.address(), TRUSTED, get(list_c, 4), a.address(), b.address(), d.address());
      assertMember(d.address(), TRUSTED, get(list_d, 4), a.address(), b.address(), c.address());
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
      Future<List<FailureDetectorEvent>> list_a = listenStatus(fd_a);
      Future<List<FailureDetectorEvent>> list_b = listenStatus(fd_b);
      Future<List<FailureDetectorEvent>> list_c = listenStatus(fd_c);
      Future<List<FailureDetectorEvent>> list_d = listenStatus(fd_d);

      start(fdetectors);

      assertMember(a.address(), SUSPECTED, get(list_a, 4), d.address());
      assertMember(b.address(), SUSPECTED, get(list_b, 4), d.address());
      assertMember(c.address(), SUSPECTED, get(list_c, 4), d.address());
      assertMember(d.address(), SUSPECTED, get(list_d, 4), a.address(), b.address(), c.address()); // node D
                                                                                                   // partitioned

      // Unblock traffic to member D on other members
      a.unblockAll();
      b.unblockAll();
      c.unblockAll();
      TimeUnit.SECONDS.sleep(4);

      list_a = listenStatus(fd_a);
      list_b = listenStatus(fd_b);
      list_c = listenStatus(fd_c);
      list_d = listenStatus(fd_d);

      // Check member D recovers

      assertMember(a.address(), TRUSTED, get(list_a, 4), b.address(), c.address(), d.address());
      assertMember(b.address(), TRUSTED, get(list_b, 4), a.address(), c.address(), d.address());
      assertMember(c.address(), TRUSTED, get(list_c, 4), a.address(), b.address(), d.address());
      assertMember(d.address(), TRUSTED, get(list_d, 4), a.address(), b.address(), c.address());
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

    Future<List<FailureDetectorEvent>> list_a = listenStatus(fd_a);
    Future<List<FailureDetectorEvent>> list_b = listenStatus(fd_b);

    try {
      start(fdetectors);

      assertMember(a.address(), SUSPECTED, get(list_a, 2), b.address());
      assertMember(b.address(), SUSPECTED, get(list_b, 2), a.address());

      // Unblock A and B members: A-->B, B-->A
      a.unblockAll();
      b.unblockAll();
      TimeUnit.SECONDS.sleep(2);

      // Check that members recover

      list_a = listenStatus(fd_a);
      list_b = listenStatus(fd_b);

      assertMember(a.address(), TRUSTED, get(list_a, 2), b.address());
      assertMember(b.address(), TRUSTED, get(list_b, 2), a.address());
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

    Future<List<FailureDetectorEvent>> list_a = listenStatus(fd_a);
    Future<List<FailureDetectorEvent>> list_b = listenStatus(fd_b);
    Future<List<FailureDetectorEvent>> list_x = listenStatus(fd_x);

    // Restarted member attributes are not initialized
    Transport xx;
    FailureDetector fd_xx;

    try {
      start(fdetectors);

      assertMember(a.address(), TRUSTED, get(list_a, 2), b.address(), x.address());
      assertMember(b.address(), TRUSTED, get(list_b, 2), a.address(), x.address());
      assertMember(x.address(), TRUSTED, get(list_x, 2), a.address(), b.address());

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

      list_a = listenStatus(fd_a);
      list_b = listenStatus(fd_b);
      Future<List<FailureDetectorEvent>> list_xx = listenStatus(fd_xx);

      // TODO [AK]: It would be more correct to consider restarted member as a new member, so x is still suspected!

      assertMember(a.address(), TRUSTED, get(list_a, 2), b.address(), xx.address());
      assertMember(b.address(), TRUSTED, get(list_b, 2), a.address(), xx.address());
      assertMember(xx.address(), TRUSTED, get(list_xx, 2), a.address(), b.address());
    } finally {
      stop(fdetectors);
    }
  }

  private FailureDetector createFD(Transport transport, List<Address> members) {
    FailureDetectorConfig failureDetectorConfig = FailureDetectorConfig.builder() // faster config for local testing
        .pingTimeout(500)
        .pingTime(250)
        .pingReqMembers(2)
        .build();
    FailureDetector failureDetector = new FailureDetector(transport, failureDetectorConfig);
    failureDetector.setMembers(members);
    return createFD(transport, members, failureDetectorConfig);
  }

  private FailureDetector createFD(Transport transport, List<Address> members, FailureDetectorConfig config) {
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
   * @param status expected listenStatus
   * @param actual1 actual collection of member events
   * @param expected expected members of the given listenStatus
   */
  private void assertMember(Address address, final ClusterMemberStatus status, Collection<FailureDetectorEvent> actual1,
      Address... expected) {

    List<Address> actualAddresses = FluentIterable.from(actual1).filter(new Predicate<FailureDetectorEvent>() {
      @Override
      public boolean apply(FailureDetectorEvent input) {
        return input.status() == status;
      }
    }).transform(new Function<FailureDetectorEvent, Address>() {
      @Override
      public Address apply(FailureDetectorEvent input) {
        return input.address();
      }
    }).toList();

    String string1 = status == SUSPECTED ? " suspected members " : " trusted members ";
    String string2 = status == SUSPECTED ? " expected to suspect " : " expected to trust ";

    assertEquals("Node " + address + " expected " + expected.length + string1 + Arrays.toString(expected)
        + ", but was: " + actual1, expected.length, actualAddresses.size());

    for (Address member : expected) {
      assertTrue("Node " + address + string2 + member + ", but was: " + actual1, actualAddresses.contains(member));
    }
  }

  private Future<List<FailureDetectorEvent>> listenStatus(FailureDetector fd) {
    List<Address> members = fd.getMembers();
    checkArgument(!members.isEmpty());

    List<ListenableFuture<FailureDetectorEvent>> resultFuture = new ArrayList<>();
    for (final Address member : members) {
      final SettableFuture<FailureDetectorEvent> future = SettableFuture.create();
      fd.listenStatus()
          .filter(new Func1<FailureDetectorEvent, Boolean>() {
            @Override
            public Boolean call(FailureDetectorEvent event) {
              return event.address() == member;
            }
          })
          .subscribe(new Action1<FailureDetectorEvent>() {
            @Override
            public void call(FailureDetectorEvent event) {
              future.set(event);
            }
          });
      resultFuture.add(future);
    }

    return Futures.successfulAsList(resultFuture);
  }

  private Collection<FailureDetectorEvent> get(Future<List<FailureDetectorEvent>> listFuture, int secs)
      throws Exception {
    return listFuture.get(secs, TimeUnit.SECONDS);
  }
}
