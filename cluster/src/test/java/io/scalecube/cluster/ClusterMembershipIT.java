package io.scalecube.cluster;

import static com.google.common.base.Throwables.propagate;
import static io.scalecube.cluster.ClusterMembershipBuilder.CMBuilder;

import io.scalecube.transport.Address;

import com.google.common.collect.ImmutableList;

import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class ClusterMembershipIT {

  @Test
  public void testInitialPhaseOk() {
    Address a = Address.from("localhost:20123");
    Address b = Address.from("localhost:20124");
    Address c = Address.from("localhost:20125");
    List<Address> members = ImmutableList.of(a, b, c);

    ClusterMembershipBuilder cm_a = ClusterMembershipBuilder.CMBuilder(a, members).init();
    ClusterMembershipBuilder cm_b = ClusterMembershipBuilder.CMBuilder(b, members).init();
    ClusterMembershipBuilder cm_c = ClusterMembershipBuilder.CMBuilder(c, members).init();

    try {
      pause(3);

      cm_a.assertTrusted(a, b, c).assertNoSuspected();
      cm_b.assertTrusted(a, b, c).assertNoSuspected();
      cm_c.assertTrusted(a, b, c).assertNoSuspected();
    } finally {
      cm_a.destroy();
      cm_b.destroy();
      cm_c.destroy();
    }
  }

  @Test
  public void testInitialPhaseWithNetworkPartitionThenRecovery() {
    Address a = Address.from("localhost:20123");
    Address b = Address.from("localhost:20124");
    Address c = Address.from("localhost:20125");
    List<Address> members = ImmutableList.of(a, b, c);

    ClusterMembershipBuilder cm_a = ClusterMembershipBuilder.CMBuilder(a, members).block(b).block(c).init();
    ClusterMembershipBuilder cm_b = ClusterMembershipBuilder.CMBuilder(b, members).block(a).block(c).init();
    ClusterMembershipBuilder cm_c = ClusterMembershipBuilder.CMBuilder(c, members).block(a).block(b).init();

    try {
      pause(3);

      cm_a.assertTrusted(a).assertNoSuspected();
      cm_b.assertTrusted(b).assertNoSuspected();
      cm_c.assertTrusted(c).assertNoSuspected();

      cm_a.unblockAll();
      cm_b.unblockAll();
      cm_c.unblockAll();

      pause(3);

      cm_a.assertTrusted(a, b, c).assertNoSuspected();
      cm_b.assertTrusted(a, b, c).assertNoSuspected();
      cm_c.assertTrusted(a, b, c).assertNoSuspected();
    } finally {
      cm_a.destroy();
      cm_b.destroy();
      cm_c.destroy();
    }
  }

  @Test
  public void testRunningPhaseOk() {
    Address a = Address.from("localhost:20123");
    Address b = Address.from("localhost:20124");
    Address c = Address.from("localhost:20125");
    List<Address> members = ImmutableList.of(a, b, c);

    ClusterMembershipBuilder cm_a = ClusterMembershipBuilder.CMBuilder(a, members).init();
    ClusterMembershipBuilder cm_b = ClusterMembershipBuilder.CMBuilder(b, members).init();
    ClusterMembershipBuilder cm_c = ClusterMembershipBuilder.CMBuilder(c, members).init();

    try {
      pause(3);

      cm_a.assertTrusted(a, b, c).assertNoSuspected();
      cm_b.assertTrusted(a, b, c).assertNoSuspected();
      cm_c.assertTrusted(a, b, c).assertNoSuspected();

      cm_a.block(b).block(c);
      cm_b.block(a).block(c);
      cm_c.block(a).block(b);

      pause(3);

      cm_a.assertTrusted(a).assertSuspected(b, c);
      cm_b.assertTrusted(b).assertSuspected(a, c);
      cm_c.assertTrusted(c).assertSuspected(a, b);

      cm_a.unblockAll();
      cm_b.unblockAll();
      cm_c.unblockAll();

      pause(3);

      cm_a.assertTrusted(a, b, c).assertNoSuspected();
      cm_b.assertTrusted(a, b, c).assertNoSuspected();
      cm_c.assertTrusted(a, b, c).assertNoSuspected();
    } finally {
      cm_a.destroy();
      cm_b.destroy();
      cm_c.destroy();
    }
  }

  @Test
  public void testLongNetworkPartitionNoRecovery() {
    Address a = Address.from("localhost:20123");
    Address b = Address.from("localhost:20124");
    Address c = Address.from("localhost:20125");
    Address d = Address.from("localhost:20126");
    List<Address> members = ImmutableList.of(a, b, c, d);

    ClusterMembershipBuilder cm_a = ClusterMembershipBuilder.CMBuilder(a, members, 3000).init();
    ClusterMembershipBuilder cm_b = ClusterMembershipBuilder.CMBuilder(b, members, 3000).init();
    ClusterMembershipBuilder cm_c = ClusterMembershipBuilder.CMBuilder(c, members, 3000).init();
    ClusterMembershipBuilder cm_d = ClusterMembershipBuilder.CMBuilder(d, members, 3000).init();

    try {
      pause(3);

      cm_a.assertTrusted(a, b, c, d);
      cm_b.assertTrusted(a, b, c, d);
      cm_c.assertTrusted(a, b, c, d);
      cm_d.assertTrusted(a, b, c, d);

      cm_a.block(c).block(d);
      cm_b.block(c).block(d);
      cm_c.block(a).block(b);
      cm_d.block(a).block(b);

      pause(3);

      cm_a.assertTrusted(a, b).assertSuspected(c, d);
      cm_b.assertTrusted(a, b).assertSuspected(c, d);
      cm_c.assertTrusted(c, d).assertSuspected(a, b);
      cm_d.assertTrusted(c, d).assertSuspected(a, b);

      pause(3);
      pause(3);

      cm_a.assertTrusted(a, b).assertNoSuspected();
      cm_b.assertTrusted(a, b).assertNoSuspected();
      cm_c.assertTrusted(c, d).assertNoSuspected();
      cm_d.assertTrusted(c, d).assertNoSuspected();
    } finally {
      cm_a.destroy();
      cm_b.destroy();
      cm_c.destroy();
      cm_d.destroy();
    }
  }

  @Test
  public void testRestartFailedMembers() {
    Address a = Address.from("localhost:20123");
    Address b = Address.from("localhost:20124");
    Address c = Address.from("localhost:20125");
    Address d = Address.from("localhost:20126");
    List<Address> members = ImmutableList.of(a, b, c, d);

    ClusterMembershipBuilder cm_a = ClusterMembershipBuilder.CMBuilder(a, members, 3000).init();
    ClusterMembershipBuilder cm_b = ClusterMembershipBuilder.CMBuilder(b, members, 3000).init();
    ClusterMembershipBuilder cm_c = ClusterMembershipBuilder.CMBuilder(c, members).init();
    ClusterMembershipBuilder cm_d = ClusterMembershipBuilder.CMBuilder(d, members).init();

    ClusterMembershipBuilder cm_restartedC = null;
    ClusterMembershipBuilder cm_restartedD = null;

    try {
      pause(3);

      cm_a.assertTrusted(a, b, c, d);
      cm_b.assertTrusted(a, b, c, d);
      cm_c.assertTrusted(a, b, c, d);
      cm_d.assertTrusted(a, b, c, d);

      cm_c.destroy();
      cm_d.destroy();

      pause(3);

      cm_a.assertTrusted(a, b).assertSuspected(c, d);
      cm_b.assertTrusted(a, b).assertSuspected(c, d);

      pause(3);
      pause(3);

      cm_a.assertTrusted(a, b).assertNoSuspected();
      cm_b.assertTrusted(a, b).assertNoSuspected();

      cm_restartedC = CMBuilder(c, Arrays.asList(a, b)).init();
      cm_restartedD = CMBuilder(d, Arrays.asList(a, b)).init();

      pause(3);

      cm_restartedC.assertTrusted(a, b, c, d).assertNoSuspected();
      cm_restartedD.assertTrusted(a, b, c, d).assertNoSuspected();
      cm_a.assertTrusted(a, b, c, d).assertNoSuspected();
      cm_b.assertTrusted(a, b, c, d).assertNoSuspected();
    } finally {
      cm_a.destroy();
      cm_b.destroy();
      cm_restartedC.destroy();
      cm_restartedD.destroy();
    }
  }

  @Test
  public void testClusterMembersWellknownMembersLimited() {
    Address a = Address.from("localhost:20123");
    Address b = Address.from("localhost:20124");
    Address c = Address.from("localhost:20125");
    Address d = Address.from("localhost:20126");
    Address e = Address.from("localhost:20127");

    ClusterMembershipBuilder cm_a = CMBuilder(a, Collections.<Address>emptyList()).init();
    ClusterMembershipBuilder cm_b = CMBuilder(b, Collections.singletonList(a)).init();
    ClusterMembershipBuilder cm_c = CMBuilder(c, Collections.singletonList(a)).init();
    ClusterMembershipBuilder cm_d = CMBuilder(d, Collections.singletonList(b)).init();
    ClusterMembershipBuilder cm_e = CMBuilder(e, Collections.singletonList(b)).init();

    try {
      pause(3);

      cm_a.assertTrusted(a, b, c, d, e).assertNoSuspected();
      cm_b.assertTrusted(a, b, c, d, e).assertNoSuspected();
      cm_c.assertTrusted(a, b, c, d, e).assertNoSuspected();
      cm_d.assertTrusted(a, b, c, d, e).assertNoSuspected();
      cm_e.assertTrusted(a, b, c, d, e).assertNoSuspected();
    } finally {
      cm_a.destroy();
      cm_b.destroy();
      cm_c.destroy();
      cm_d.destroy();
      cm_e.destroy();
    }
  }

  private void pause(int timeout) {
    try {
      TimeUnit.SECONDS.sleep(timeout);
    } catch (InterruptedException e) {
      propagate(e);
    }
  }
}
