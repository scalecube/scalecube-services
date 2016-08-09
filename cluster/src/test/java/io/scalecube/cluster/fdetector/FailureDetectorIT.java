package io.scalecube.cluster.fdetector;

import static com.google.common.collect.ImmutableList.of;
import static io.scalecube.cluster.fdetector.FailureDetectorBuilder.FDBuilder;
import static io.scalecube.cluster.fdetector.FailureDetectorBuilder.FDBuilderWithPingTime;
import static io.scalecube.cluster.fdetector.FailureDetectorBuilder.FDBuilderWithPingTimeout;
import static org.junit.Assert.assertEquals;

import io.scalecube.transport.ITransport;
import io.scalecube.transport.Address;
import io.scalecube.transport.Transport;
import io.scalecube.transport.TransportConfig;

import com.google.common.util.concurrent.SettableFuture;

import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class FailureDetectorIT {

  @Test
  public void testAllTrusted() throws Exception {
    Transport a = Transport.bindAwait(true);
    Transport b = Transport.bindAwait(true);
    Transport c = Transport.bindAwait(true);
    List<Address> members = Arrays.asList(a.address(), b.address(), c.address());

    List<FailureDetectorBuilder> builders = new ArrayList<>();
    builders.add(FDBuilder(a).members(members).pingMember(b.address()).noRandomMembers());
    builders.add(FDBuilder(b).members(members).pingMember(c.address()).noRandomMembers());
    builders.add(FDBuilder(c).members(members).pingMember(a.address()).noRandomMembers());

    try {
      startAll(builders);
      TimeUnit.SECONDS.sleep(4);
      Map<Address, Address> target = getSuspected(builders);
      assertEquals("No suspected members is expected: " + target, 0, target.size());
    } finally {
      destroy(builders);
    }
  }

  @Test
  public void testBasicTrusted() throws Exception {
    Transport a = Transport.bindAwait(true);
    Transport b = Transport.bindAwait(true);
    List<Address> members = Arrays.asList(a.address(), b.address());

    List<FailureDetectorBuilder> builders = new ArrayList<>();
    builders.add(FDBuilder(a).members(members));
    builders.add(FDBuilder(b).members(members));

    try {
      startAll(builders);
      TimeUnit.SECONDS.sleep(4);
      Map<Address, Address> target = getSuspected(builders);
      assertEquals("No suspected members is expected: " + target, 0, target.size());
    } finally {
      destroy(builders);
    }
  }

  @Test
  public void testTrustedDifferentPingTiming() throws Exception {
    Transport a = Transport.bindAwait(true);
    Transport b = Transport.bindAwait(true);
    List<Address> members = Arrays.asList(a.address(), b.address());

    List<FailureDetectorBuilder> builders = new ArrayList<>();
    builders.add(FDBuilderWithPingTime(a, 100).members(members));
    builders.add(FDBuilderWithPingTime(b, 300).members(members));

    try {
      startAll(builders);
      TimeUnit.SECONDS.sleep(4);
      Map<Address, Address> target = getSuspected(builders);
      assertEquals("No suspected members is expected: " + target, 0, target.size());
    } finally {
      destroy(builders);
    }
  }

  @Test
  public void testAllSuspected() throws Exception {
    Transport a = Transport.bindAwait(true);
    Transport b = Transport.bindAwait(true);
    Transport c = Transport.bindAwait(true);
    List<Address> members = Arrays.asList(a.address(), b.address(), c.address());

    List<FailureDetectorBuilder> builders = new ArrayList<>();
    builders.add(FDBuilder(a).members(members).pingMember(b.address()).block(members));
    builders.add(FDBuilder(b).members(members).pingMember(c.address()).block(members));
    builders.add(FDBuilder(c).members(members).pingMember(a.address()).block(members));

    try {
      startAll(builders);
      TimeUnit.SECONDS.sleep(4);
      Map<Address, Address> target = getSuspected(builders);
      assertEquals("Expected 3 suspected members: " + target, 3, target.size());
      assertEquals(b.address(), target.get(a.address()));
      assertEquals(c.address(), target.get(b.address()));
      assertEquals(a.address(), target.get(c.address()));
    } finally {
      destroy(builders);
    }
  }

  @Test
  public void testBasicSuspected() throws Exception {
    Transport a = Transport.bindAwait(true);
    Transport b = Transport.bindAwait(true);
    List<Address> members = Arrays.asList(a.address(), b.address());

    List<FailureDetectorBuilder> builders = new ArrayList<>();
    builders.add(FDBuilder(a).members(members).pingMember(b.address()).block(members));
    builders.add(FDBuilder(b).members(members).pingMember(a.address()).block(members));

    try {
      startAll(builders);
      TimeUnit.SECONDS.sleep(4);
      Map<Address, Address> target = getSuspected(builders);
      assertEquals("Expected 2 suspected members: " + target, 2, target.size());
      assertEquals(b.address(), target.get(a.address()));
      assertEquals(a.address(), target.get(b.address()));
    } finally {
      destroy(builders);
    }
  }

  @Test
  public void testAllTrustedDespiteTrafficIssue() throws Exception {
    Transport a = Transport.bindAwait(true);
    Transport b = Transport.bindAwait(true);
    Transport c = Transport.bindAwait(true);
    List<Address> members = Arrays.asList(a.address(), b.address(), c.address());

    List<FailureDetectorBuilder> builders = new ArrayList<>();
    builders.add(FDBuilder(a).members(members).pingMember(b.address()).block(b.address()));
    builders.add(FDBuilder(b).members(members).pingMember(c.address()));
    builders.add(FDBuilder(c).members(members).pingMember(a.address()));

    try {
      startAll(builders);
      TimeUnit.SECONDS.sleep(4);
      Map<Address, Address> target = getSuspected(builders);
      assertEquals("No suspected members is expected: " + target, 0, target.size());
    } finally {
      destroy(builders);
    }
  }

  @Test
  public void testSingleSuspectedNotAffectOthers() throws Exception {
    Transport a = Transport.bindAwait(true);
    Transport b = Transport.bindAwait(true);
    Transport c = Transport.bindAwait(true);
    Transport d = Transport.bindAwait(true);
    List<Address> members = Arrays.asList(a.address(), b.address(), c.address(), d.address());

    List<FailureDetectorBuilder> builders = new ArrayList<>();
    // a--X-->b and no neighbors
    builders.add(FDBuilderWithPingTimeout(a, 999).members(members).pingMember(b.address()).block(b.address())
        .noRandomMembers());
    builders.add(FDBuilderWithPingTime(b, 100).members(members).pingMember(a.address()).noRandomMembers()); // ping a
    builders.add(FDBuilderWithPingTime(c, 100).members(members).pingMember(a.address()).noRandomMembers()); // ping a
    builders.add(FDBuilderWithPingTime(d, 100).members(members).pingMember(a.address()).noRandomMembers()); // ping a

    try {
      startAll(builders);
      TimeUnit.SECONDS.sleep(4);
      Map<Address, Address> target = getSuspected(builders);
      assertEquals("Expected 2 suspected members: " + target, 2, target.size());
      assertEquals(b.address(), target.get(a.address()));
      assertEquals(a.address(), target.get(b.address()));
    } finally {
      destroy(builders);
    }
  }

  @Test
  public void testTwoSuspectedNotAffectOthers() throws Exception {
    Transport a = Transport.bindAwait(true);
    Transport b = Transport.bindAwait(true);
    Transport c = Transport.bindAwait(true);
    Transport d = Transport.bindAwait(true);
    Transport e = Transport.bindAwait(true);
    List<Address> members = Arrays.asList(a.address(), b.address(), c.address(), d.address(), e.address());

    List<FailureDetectorBuilder> builders = new ArrayList<>();
    // a--X-->b then a--X-->c
    builders.add(FDBuilderWithPingTimeout(a, 499).members(members).pingMember(b.address()).block(b.address())
        .randomMembers(of(c.address())).block(c.address()));
    builders.add(FDBuilderWithPingTime(b, 100).members(members).pingMember(a.address()).noRandomMembers()); // ping a
    builders.add(FDBuilderWithPingTime(c, 100).members(members).pingMember(a.address()).noRandomMembers()); // ping a
    builders.add(FDBuilderWithPingTime(d, 100).members(members).pingMember(a.address()).noRandomMembers()); // ping a
    builders.add(FDBuilderWithPingTime(e, 100).members(members).pingMember(a.address()).noRandomMembers()); // ping a

    try {
      startAll(builders);
      TimeUnit.SECONDS.sleep(4);
      Map<Address, Address> target = getSuspected(builders);
      assertEquals("Expected 3 suspected members: " + target, 3, target.size());
      assertEquals(b.address(), target.get(a.address()));
      assertEquals(a.address(), target.get(b.address()));
      assertEquals(a.address(), target.get(c.address()));
    } finally {
      destroy(builders);
    }
  }

  @Test
  public void testSuspectedNetworkPartition() throws Exception {
    Transport a = Transport.bindAwait(true);
    Transport b = Transport.bindAwait(true);
    Transport c = Transport.bindAwait(true);
    Transport x = Transport.bindAwait(true);
    List<Address> members = Arrays.asList(a.address(), b.address(), c.address(), x.address());


    List<FailureDetectorBuilder> builders = new ArrayList<>();
    builders.add(FDBuilder(a).members(members).pingMember(x.address()).block(x.address()));
    builders.add(FDBuilder(b).members(members).pingMember(x.address()).block(x.address()));
    builders.add(FDBuilder(c).members(members).pingMember(x.address()).block(x.address()));
    builders.add(FDBuilder(x).members(members).pingMember(a.address()));

    try {
      startAll(builders);
      TimeUnit.SECONDS.sleep(4);
      Map<Address, Address> target = getSuspected(builders);
      assertEquals("Expected 4 suspected members: " + target, 4, target.size());
      assertEquals(x.address(), target.get(a.address()));
      assertEquals(x.address(), target.get(b.address()));
      assertEquals(x.address(), target.get(c.address()));
      assertEquals(a.address(), target.get(x.address()));
    } finally {
      destroy(builders);
    }
  }

  @Test
  public void testSuspectedNeighborsHasTrafficIssue() throws Exception {
    Transport a = Transport.bindAwait(true);
    Transport b = Transport.bindAwait(true);
    Transport c = Transport.bindAwait(true);
    Transport x = Transport.bindAwait(true);
    List<Address> members = Arrays.asList(a.address(), b.address(), c.address(), x.address());

    List<FailureDetectorBuilder> builders = new ArrayList<>();
    builders.add(FDBuilder(a).members(members).pingMember(x.address()).block(x.address()));
    builders.add(FDBuilder(b).members(members).pingMember(a.address()).block(x.address()));
    builders.add(FDBuilder(c).members(members).pingMember(a.address()).block(x.address()));
    builders.add(FDBuilderWithPingTime(x, 100500).members(members).pingMember(b.address()));

    try {
      startAll(builders);
      TimeUnit.SECONDS.sleep(4);
      Map<Address, Address> target = getSuspected(builders);
      assertEquals("Expected 1 suspected members: " + target, 1, target.size());
      assertEquals(x.address(), target.get(a.address()));
    } finally {
      destroy(builders);
    }
  }

  @Test
  public void testMemberBecomeTrusted() throws Exception {
    Transport a = Transport.bindAwait(true);
    Transport b = Transport.bindAwait(true);
    List<Address> members = Arrays.asList(a.address(), b.address());

    List<FailureDetectorBuilder> builders = new ArrayList<>();
    builders.add(FDBuilder(a).members(members).block(b.address())); // traffic is blocked initially
    builders.add(FDBuilder(b).members(members).block(a.address())); // traffic is blocked initially

    try {
      startAll(builders);
      TimeUnit.SECONDS.sleep(4);
      Map<Address, Address> targetSuspect0 = getSuspected(builders);
      assertEquals("Expected 2 suspected members: " + targetSuspect0, 2, targetSuspect0.size());
      assertEquals(b.address(), targetSuspect0.get(a.address()));
      assertEquals(a.address(), targetSuspect0.get(b.address()));

      unblock(builders); // unblock all traffic
      TimeUnit.SECONDS.sleep(4);
      Map<Address, Address> targetSuspect1 = getSuspected(builders);
      assertEquals("No suspected members is expected: " + targetSuspect1, 0, targetSuspect1.size());
    } finally {
      destroy(builders);
    }
  }

  @Test
  public void testMemberBecomeSuspected() throws Exception {
    Transport a = Transport.bindAwait(true);
    Transport b = Transport.bindAwait(true);
    Transport x = Transport.bindAwait(true);
    Transport y = Transport.bindAwait(true);
    List<Address> members = Arrays.asList(a.address(), b.address(), x.address(), y.address());


    List<FailureDetectorBuilder> builders = new ArrayList<>();
    builders.add(FDBuilder(a).members(members).pingMember(x.address()));
    builders.add(FDBuilder(b).members(members).pingMember(y.address()));
    builders.add(FDBuilder(x).members(members));
    builders.add(FDBuilder(y).members(members));

    try {
      startAll(builders);
      TimeUnit.SECONDS.sleep(4);
      Map<Address, Address> targetSuspect0 = getSuspected(builders);
      assertEquals("No suspected members is expected: " + targetSuspect0, 0, targetSuspect0.size());

      destroy(x.address(), builders);
      destroy(y.address(), builders);
      TimeUnit.SECONDS.sleep(4);
      Map<Address, Address> targetSuspect1 = getSuspected(builders);
      assertEquals("Expected 2 suspected members: " + targetSuspect1, 2, targetSuspect1.size());
      assertEquals(x.address(), targetSuspect1.get(a.address()));
      assertEquals(y.address(), targetSuspect1.get(b.address()));
    } finally {
      destroy(builders);
    }
  }

  // TODO [AK]: Rewrite this test after fix. Incarnation isn't respected by FD!!!
  /*
  @Test
  public void testMemberBecomeSuspectedIncarnationRespected() throws Exception {
    Transport a = Transport.bindAwait(true);
    Transport b = Transport.bindAwait(true);
    Transport x = Transport.bindAwait(true);
    List<Address> members = new ArrayList<>(Arrays.asList(a.address(), b.address(), x.address()));

    List<FailureDetectorBuilder> builders = new ArrayList<>();
    builders.add(FDBuilderWithPingTime(a, 100).members(members).pingMember(x.address()));
    builders.add(FDBuilderWithPingTime(b, 100).members(members).pingMember(x.address()));
    builders.add(FDBuilderWithPingTime(x, 100500).members(members));

    try {
      startAll(builders);
      TimeUnit.SECONDS.sleep(4);
      Map<Address, Address> targetSuspect0 = getSuspected(builders);
      assertEquals("No suspected members is expected: " + targetSuspect0, 0, targetSuspect0.size());

      destroy(x.address(), builders);
      TimeUnit.SECONDS.sleep(4);
      Map<Address, Address> targetSuspect1 = getSuspected(builders);
      assertEquals("Expected 2 suspected members: " + targetSuspect1, 2, targetSuspect1.size());
      assertEquals(x.address(), targetSuspect1.get(a.address()));
      assertEquals(x.address(), targetSuspect1.get(b.address()));

      TransportConfig xxConfig = TransportConfig.builder()
          .port(x.address().port())
          .portAutoIncrement(false)
          .useNetworkEmulator(true)
          .build();
      Transport xx = Transport.bindAwait(xxConfig);
      members.add(xx.address());
      FailureDetectorBuilder xxBuilder = FDBuilderWithPingTime(xx, 100).members(members).pingMember(x.address());
      builders.add(xxBuilder);
      for (FailureDetectorBuilder builder : builders) {
        builder.members(members);
      }
      {
        xxBuilder.start();
      }
      TimeUnit.SECONDS.sleep(4);
      Map<Address, Address> targetSuspect2 = getSuspected(builders);
      assertEquals("Expected 3 suspected members: " + targetSuspect2, 3, targetSuspect2.size());
      assertEquals(x.address(), targetSuspect2.get(a.address()));
      assertEquals(x.address(), targetSuspect2.get(b.address()));
      assertEquals(x.address(), targetSuspect2.get(xx.address()));
    } finally {
      destroy(builders);
    }
  }
  */

  private Map<Address, Address> getSuspected(Iterable<FailureDetectorBuilder> builders) {
    Map<Address, Address> target = new HashMap<>();
    for (FailureDetectorBuilder builder : builders) {
      List<Address> suspectedMembers = builder.failureDetector.getSuspectedMembers();
      if (!suspectedMembers.isEmpty()) {
        Address localAddress = builder.failureDetector.getTransport().address();
        assertEquals(localAddress + ": " + suspectedMembers, 1, suspectedMembers.size());
        target.put(localAddress, suspectedMembers.get(0));
      }
    }
    return target;
  }

  private void destroy(Iterable<FailureDetectorBuilder> builders) {
    for (FailureDetectorBuilder builder : builders) {
      builder.failureDetector.stop();
      destroyTransport(builder.failureDetector.getTransport());
    }
  }

  private void destroy(Address address, Iterable<FailureDetectorBuilder> builders) {
    for (FailureDetectorBuilder builder : builders) {
      if (builder.failureDetector.getTransport().address() == address) {
        builder.failureDetector.stop();
        destroyTransport(builder.failureDetector.getTransport());
        return;
      }
    }
    throw new IllegalArgumentException(address.toString());
  }

  private void destroyTransport(ITransport transport) {
    SettableFuture<Void> close = SettableFuture.create();
    transport.stop(close);
    try {
      close.get(1, TimeUnit.SECONDS);
    } catch (Exception ignore) {
      // ignore
    }
  }

  private void startAll(Iterable<FailureDetectorBuilder> builders) {
    for (FailureDetectorBuilder builder : builders) {
      builder.start();
    }
  }

  private void unblock(Iterable<FailureDetectorBuilder> failureDetectorBuilders) {
    for (FailureDetectorBuilder builder : failureDetectorBuilders) {
      builder.unblockAll();
    }
  }
}
