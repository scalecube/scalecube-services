package io.scalecube.cluster.fdetector;

import static com.google.common.collect.ImmutableList.of;
import static io.scalecube.cluster.fdetector.FailureDetectorBuilder.FDBuilder;
import static io.scalecube.cluster.fdetector.FailureDetectorBuilder.FDBuilderWithPingTime;
import static io.scalecube.cluster.fdetector.FailureDetectorBuilder.FDBuilderWithPingTimeout;
import static io.scalecube.transport.Address.from;
import static org.junit.Assert.assertEquals;

import io.scalecube.transport.ITransport;
import io.scalecube.transport.Address;

import com.google.common.util.concurrent.SettableFuture;

import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class FailureDetectorIT {

  @Test
  public void testAllTrusted() throws Exception {
    List<Address> members = new ArrayList<>();
    Address a = from("localhost:20123");
    Address b = from("localhost:20124");
    Address c = from("localhost:20125");
    members.add(a);
    members.add(b);
    members.add(c);

    List<FailureDetectorBuilder> builders = new ArrayList<>();
    builders.add(FDBuilder(a).set(members).pingMember(b).noRandomMembers());
    builders.add(FDBuilder(b).set(members).pingMember(c).noRandomMembers());
    builders.add(FDBuilder(c).set(members).pingMember(a).noRandomMembers());

    try {
      create(builders);
      TimeUnit.SECONDS.sleep(4);
      Map<Address, Address> target = getSuspected(builders);
      assertEquals("No suspected members is expected: " + target, 0, target.size());
    } finally {
      destroy(builders);
    }
  }

  @Test
  public void testBasicTrusted() throws Exception {
    List<Address> members = new ArrayList<>();
    Address a = from("localhost:20123");
    Address b = from("localhost:20124");
    members.add(a);
    members.add(b);

    List<FailureDetectorBuilder> builders = new ArrayList<>();
    builders.add(FDBuilder(a).set(members));
    builders.add(FDBuilder(b).set(members));

    try {
      create(builders);
      TimeUnit.SECONDS.sleep(4);
      Map<Address, Address> target = getSuspected(builders);
      assertEquals("No suspected members is expected: " + target, 0, target.size());
    } finally {
      destroy(builders);
    }
  }

  @Test
  public void testTrustedDifferentPingTiming() throws Exception {
    List<Address> members = new ArrayList<>();
    Address a = from("localhost:20123");
    Address b = from("localhost:20124");
    members.add(a);
    members.add(b);

    List<FailureDetectorBuilder> builders = new ArrayList<>();
    builders.add(FDBuilderWithPingTime(a, 100).set(members));
    builders.add(FDBuilderWithPingTime(b, 300).set(members));

    try {
      create(builders);
      TimeUnit.SECONDS.sleep(4);
      Map<Address, Address> target = getSuspected(builders);
      assertEquals("No suspected members is expected: " + target, 0, target.size());
    } finally {
      destroy(builders);
    }
  }

  @Test
  public void testAllSuspected() throws Exception {
    List<Address> members = new ArrayList<>();
    Address a = from("localhost:20123");
    Address b = from("localhost:20124");
    Address c = from("localhost:20125");
    members.add(a);
    members.add(b);
    members.add(c);

    List<FailureDetectorBuilder> builders = new ArrayList<>();
    builders.add(FDBuilder(a).set(members).pingMember(b).block(members));
    builders.add(FDBuilder(b).set(members).pingMember(c).block(members));
    builders.add(FDBuilder(c).set(members).pingMember(a).block(members));

    try {
      create(builders);
      TimeUnit.SECONDS.sleep(4);
      Map<Address, Address> target = getSuspected(builders);
      assertEquals("Expected 3 suspected members: " + target, 3, target.size());
      assertEquals(b, target.get(a));
      assertEquals(c, target.get(b));
      assertEquals(a, target.get(c));
    } finally {
      destroy(builders);
    }
  }

  @Test
  public void testBasicSuspected() throws Exception {
    List<Address> members = new ArrayList<>();
    Address a = from("localhost:20123");
    Address b = from("localhost:20124");
    members.add(a);
    members.add(b);

    List<FailureDetectorBuilder> builders = new ArrayList<>();
    builders.add(FDBuilder(a).set(members).pingMember(b).block(members));
    builders.add(FDBuilder(b).set(members).pingMember(a).block(members));

    try {
      create(builders);
      TimeUnit.SECONDS.sleep(4);
      Map<Address, Address> target = getSuspected(builders);
      assertEquals("Expected 2 suspected members: " + target, 2, target.size());
      assertEquals(b, target.get(a));
      assertEquals(a, target.get(b));
    } finally {
      destroy(builders);
    }
  }

  @Test
  public void testAllTrustedDespiteTrafficIssue() throws Exception {
    List<Address> members = new ArrayList<>();
    Address a = from("localhost:20123");
    Address b = from("localhost:20124");
    Address c = from("localhost:20125");
    members.add(a);
    members.add(b);
    members.add(c);

    List<FailureDetectorBuilder> builders = new ArrayList<>();
    builders.add(FDBuilder(a).set(members).pingMember(b).block(b));
    builders.add(FDBuilder(b).set(members).pingMember(c));
    builders.add(FDBuilder(c).set(members).pingMember(a));

    try {
      create(builders);
      TimeUnit.SECONDS.sleep(4);
      Map<Address, Address> target = getSuspected(builders);
      assertEquals("No suspected members is expected: " + target, 0, target.size());
    } finally {
      destroy(builders);
    }
  }

  @Test
  public void testSingleSuspectedNotAffectOthers() throws Exception {
    List<Address> members = new ArrayList<>();
    Address a = from("localhost:20123");
    Address b = from("localhost:20124");
    Address c = from("localhost:20125");
    Address d = from("localhost:20126");
    members.add(a);
    members.add(b);
    members.add(c);
    members.add(d);

    List<FailureDetectorBuilder> builders = new ArrayList<>();
    // a--X-->b and no neighbors
    builders.add(FDBuilderWithPingTimeout(a, 999).set(members).pingMember(b).block(b).noRandomMembers());
    builders.add(FDBuilderWithPingTime(b, 100).set(members).pingMember(a).noRandomMembers()); // ping a
    builders.add(FDBuilderWithPingTime(c, 100).set(members).pingMember(a).noRandomMembers()); // ping a
    builders.add(FDBuilderWithPingTime(d, 100).set(members).pingMember(a).noRandomMembers()); // ping a

    try {
      create(builders);
      TimeUnit.SECONDS.sleep(4);
      Map<Address, Address> target = getSuspected(builders);
      assertEquals("Expected 2 suspected members: " + target, 2, target.size());
      assertEquals(b, target.get(a));
      assertEquals(a, target.get(b));
    } finally {
      destroy(builders);
    }
  }

  @Test
  public void testTwoSuspectedNotAffectOthers() throws Exception {
    List<Address> members = new ArrayList<>();
    Address a = from("localhost:20123");
    Address b = from("localhost:20124");
    Address c = from("localhost:20125");
    Address d = from("localhost:20126");
    Address e = from("localhost:20127");
    members.add(a);
    members.add(b);
    members.add(c);
    members.add(d);
    members.add(e);

    List<FailureDetectorBuilder> builders = new ArrayList<>();
    // a--X-->b then a--X-->c
    builders.add(FDBuilderWithPingTimeout(a, 499).set(members).pingMember(b).block(b).randomMembers(of(c)).block(c));

    builders.add(FDBuilderWithPingTime(b, 100).set(members).pingMember(a).noRandomMembers()); // ping a
    builders.add(FDBuilderWithPingTime(c, 100).set(members).pingMember(a).noRandomMembers()); // ping a
    builders.add(FDBuilderWithPingTime(d, 100).set(members).pingMember(a).noRandomMembers()); // ping a
    builders.add(FDBuilderWithPingTime(e, 100).set(members).pingMember(a).noRandomMembers()); // ping a

    try {
      create(builders);
      TimeUnit.SECONDS.sleep(4);
      Map<Address, Address> target = getSuspected(builders);
      assertEquals("Expected 3 suspected members: " + target, 3, target.size());
      assertEquals(b, target.get(a));
      assertEquals(a, target.get(b));
      assertEquals(a, target.get(c));
    } finally {
      destroy(builders);
    }
  }

  @Test
  public void testSuspectedNetworkPartition() throws Exception {
    List<Address> members = new ArrayList<>();
    Address a = from("localhost:20123");
    Address b = from("localhost:20124");
    Address c = from("localhost:20125");
    Address x = from("localhost:20126");
    members.add(a);
    members.add(b);
    members.add(c);
    members.add(x);

    List<FailureDetectorBuilder> builders = new ArrayList<>();
    builders.add(FDBuilder(a).set(members).pingMember(x).block(x));
    builders.add(FDBuilder(b).set(members).pingMember(x).block(x));
    builders.add(FDBuilder(c).set(members).pingMember(x).block(x));
    builders.add(FDBuilder(x).set(members).pingMember(a));

    try {
      create(builders);
      TimeUnit.SECONDS.sleep(4);
      Map<Address, Address> target = getSuspected(builders);
      assertEquals("Expected 4 suspected members: " + target, 4, target.size());
      assertEquals(x, target.get(a));
      assertEquals(x, target.get(b));
      assertEquals(x, target.get(c));
      assertEquals(a, target.get(x));
    } finally {
      destroy(builders);
    }
  }

  @Test
  public void testSuspectedNeighborsHasTrafficIssue() throws Exception {
    List<Address> members = new ArrayList<>();
    Address a = from("localhost:20123");
    Address b = from("localhost:20124");
    Address d = from("localhost:20125");
    Address x = from("localhost:20126");
    members.add(a);
    members.add(b);
    members.add(d);
    members.add(x);

    List<FailureDetectorBuilder> builders = new ArrayList<>();
    builders.add(FDBuilder(a).set(members).pingMember(x).block(x));
    builders.add(FDBuilder(b).set(members).pingMember(a).block(x));
    builders.add(FDBuilder(d).set(members).pingMember(a).block(x));
    builders.add(FDBuilderWithPingTime(x, 100500).set(members).pingMember(b));

    try {
      create(builders);
      TimeUnit.SECONDS.sleep(4);
      Map<Address, Address> target = getSuspected(builders);
      assertEquals("Expected 1 suspected members: " + target, 1, target.size());
      assertEquals(x, target.get(a));
    } finally {
      destroy(builders);
    }
  }

  @Test
  public void testMemberBecomeTrusted() throws Exception {
    List<Address> members = new ArrayList<>();
    Address a = from("localhost:20123");
    Address b = from("localhost:20124");
    members.add(a);
    members.add(b);

    List<FailureDetectorBuilder> builders = new ArrayList<>();
    builders.add(FDBuilder(a).set(members).block(b)); // traffic is blocked initially
    builders.add(FDBuilder(b).set(members).block(a)); // traffic is blocked initially

    try {
      create(builders);
      TimeUnit.SECONDS.sleep(4);
      Map<Address, Address> targetSuspect0 = getSuspected(builders);
      assertEquals("Expected 2 suspected members: " + targetSuspect0, 2, targetSuspect0.size());
      assertEquals(b, targetSuspect0.get(a));
      assertEquals(a, targetSuspect0.get(b));

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
    List<Address> members = new ArrayList<>();
    Address a = from("localhost:20123");
    Address b = from("localhost:20124");
    Address x = from("localhost:20125");
    Address y = from("localhost:20126");
    members.add(a);
    members.add(b);
    members.add(x);
    members.add(y);

    List<FailureDetectorBuilder> builders = new ArrayList<>();
    builders.add(FDBuilder(a).set(members).pingMember(x));
    builders.add(FDBuilder(b).set(members).pingMember(y));
    builders.add(FDBuilder(x).set(members));
    builders.add(FDBuilder(y).set(members));

    try {
      create(builders);
      TimeUnit.SECONDS.sleep(4);
      Map<Address, Address> targetSuspect0 = getSuspected(builders);
      assertEquals("No suspected members is expected: " + targetSuspect0, 0, targetSuspect0.size());

      destroy(x, builders);
      destroy(y, builders);
      TimeUnit.SECONDS.sleep(4);
      Map<Address, Address> targetSuspect1 = getSuspected(builders);
      assertEquals("Expected 2 suspected members: " + targetSuspect1, 2, targetSuspect1.size());
      assertEquals(x, targetSuspect1.get(a));
      assertEquals(y, targetSuspect1.get(b));
    } finally {
      destroy(builders);
    }
  }

  @Test
  public void testMemberBecomeSuspectedIncarnationRespected() throws Exception {
    List<Address> members = new ArrayList<>();
    Address a = from("localhost:20123");
    Address b = from("localhost:20124");
    Address x = from("localhost:20125");
    members.add(a);
    members.add(b);
    members.add(x);

    List<FailureDetectorBuilder> builders = new ArrayList<>();
    builders.add(FDBuilderWithPingTime(a, 100).set(members).pingMember(x));
    builders.add(FDBuilderWithPingTime(b, 100).set(members).pingMember(x));
    builders.add(FDBuilderWithPingTime(x, 100500).set(members));

    try {
      create(builders);
      TimeUnit.SECONDS.sleep(4);
      Map<Address, Address> targetSuspect0 = getSuspected(builders);
      assertEquals("No suspected members is expected: " + targetSuspect0, 0, targetSuspect0.size());

      destroy(x, builders);
      TimeUnit.SECONDS.sleep(4);
      Map<Address, Address> targetSuspect1 = getSuspected(builders);
      assertEquals("Expected 2 suspected members: " + targetSuspect1, 2, targetSuspect1.size());
      assertEquals(x, targetSuspect1.get(a));
      assertEquals(x, targetSuspect1.get(b));

      Address xx = from("localhost:20125");
      members.add(xx);
      FailureDetectorBuilder xxBuilder = FDBuilderWithPingTime(xx, 100).set(members).pingMember(x);
      builders.add(xxBuilder);
      for (FailureDetectorBuilder builder : builders) {
        builder.set(members);
      }
      {
        xxBuilder.init();
      }
      TimeUnit.SECONDS.sleep(4);
      Map<Address, Address> targetSuspect2 = getSuspected(builders);
      assertEquals("Expected 3 suspected members: " + targetSuspect2, 3, targetSuspect2.size());
      assertEquals(x, targetSuspect2.get(a));
      assertEquals(x, targetSuspect2.get(b));
      assertEquals(x, targetSuspect2.get(xx));
    } finally {
      destroy(builders);
    }
  }

  private Map<Address, Address> getSuspected(Iterable<FailureDetectorBuilder> builders) {
    Map<Address, Address> target = new HashMap<>();
    for (FailureDetectorBuilder builder : builders) {
      List<Address> suspectedMembers = builder.failureDetector.getSuspectedMembers();
      if (!suspectedMembers.isEmpty()) {
        Address localAddress = builder.failureDetector.getTransport().localAddress();
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
      if (builder.failureDetector.getTransport().localAddress() == address) {
        builder.failureDetector.stop();
        destroyTransport(builder.failureDetector.getTransport());
        return;
      }
    }
    throw new IllegalArgumentException(address.toString());
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

  private void create(Iterable<FailureDetectorBuilder> builders) {
    for (FailureDetectorBuilder builder : builders) {
      builder.init();
    }
  }

  private void unblock(Iterable<FailureDetectorBuilder> failureDetectorBuilders) {
    for (FailureDetectorBuilder builder : failureDetectorBuilders) {
      builder.unblockAll();
    }
  }
}
