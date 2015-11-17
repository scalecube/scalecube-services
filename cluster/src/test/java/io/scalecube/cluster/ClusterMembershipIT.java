package io.scalecube.cluster;

import static com.google.common.base.Throwables.propagate;
import static io.scalecube.cluster.ClusterMembershipBuilder.CMBuilder;

import io.scalecube.transport.TransportAddress;
import io.scalecube.transport.TransportEndpoint;

import com.google.common.collect.ImmutableList;

import org.junit.Test;

import java.util.List;
import java.util.concurrent.TimeUnit;

public class ClusterMembershipIT {

  @Test
  public void testInitialPhaseOk() {
    TransportEndpoint a = TransportEndpoint.from("tcp://a@localhost:20123");
    TransportEndpoint b = TransportEndpoint.from("tcp://b@localhost:20124");
    TransportEndpoint c = TransportEndpoint.from("tcp://c@localhost:20125");
    List<TransportAddress> members = ImmutableList.of(a.address(), b.address(), c.address());

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
    TransportEndpoint a = TransportEndpoint.from("tcp://a@localhost:20123");
    TransportEndpoint b = TransportEndpoint.from("tcp://b@localhost:20124");
    TransportEndpoint c = TransportEndpoint.from("tcp://c@localhost:20125");
    List<TransportAddress> members = ImmutableList.of(a.address(), b.address(), c.address());

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
    TransportEndpoint a = TransportEndpoint.from("tcp://a@localhost:20123");
    TransportEndpoint b = TransportEndpoint.from("tcp://b@localhost:20124");
    TransportEndpoint c = TransportEndpoint.from("tcp://c@localhost:20125");
    List<TransportAddress> members = ImmutableList.of(a.address(), b.address(), c.address());

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
    TransportEndpoint a = TransportEndpoint.from("tcp://a@localhost:20123");
    TransportEndpoint b = TransportEndpoint.from("tcp://b@localhost:20124");
    TransportEndpoint c = TransportEndpoint.from("tcp://c@localhost:20125");
    TransportEndpoint d = TransportEndpoint.from("tcp://d@localhost:20126");
    List<TransportAddress> members = ImmutableList.of(a.address(), b.address(), c.address(), d.address());

    ClusterMembershipBuilder cm_a = ClusterMembershipBuilder.CMBuilder(a, members).maxSuspectTime(3000).init();
    ClusterMembershipBuilder cm_b = ClusterMembershipBuilder.CMBuilder(b, members).maxSuspectTime(3000).init();
    ClusterMembershipBuilder cm_c = ClusterMembershipBuilder.CMBuilder(c, members).maxSuspectTime(3000).init();
    ClusterMembershipBuilder cm_d = ClusterMembershipBuilder.CMBuilder(d, members).maxSuspectTime(3000).init();

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
    TransportEndpoint a = TransportEndpoint.from("tcp://a@localhost:20123");
    TransportEndpoint b = TransportEndpoint.from("tcp://b@localhost:20124");
    TransportEndpoint c = TransportEndpoint.from("tcp://c@localhost:20125");
    TransportEndpoint d = TransportEndpoint.from("tcp://d@localhost:20126");
    List<TransportAddress> members = ImmutableList.of(a.address(), b.address(), c.address(), d.address());

    ClusterMembershipBuilder cm_a = ClusterMembershipBuilder.CMBuilder(a, members).maxSuspectTime(3000).init();
    ClusterMembershipBuilder cm_b = ClusterMembershipBuilder.CMBuilder(b, members).maxSuspectTime(3000).init();
    ClusterMembershipBuilder cm_c = ClusterMembershipBuilder.CMBuilder(c, members).init();
    ClusterMembershipBuilder cm_d = ClusterMembershipBuilder.CMBuilder(d, members).init();

    TransportEndpoint rc = TransportEndpoint.from("tcp://restarted_c@localhost:20125");
    TransportEndpoint rd = TransportEndpoint.from("tcp://restarted_d@localhost:20126");
    ClusterMembershipBuilder cm_rc = ClusterMembershipBuilder.CMBuilder(rc, a.address(), b.address());
    ClusterMembershipBuilder cm_rd = ClusterMembershipBuilder.CMBuilder(rd, a.address(), b.address());

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

      cm_rc.init();
      cm_rd.init();

      pause(3);

      cm_a.assertTrusted(a, b, rc, rd).assertNoSuspected();
      cm_b.assertTrusted(a, b, rc, rd).assertNoSuspected();
      cm_rc.assertTrusted(a, b, rc, rd).assertNoSuspected();
      cm_rd.assertTrusted(a, b, rc, rd).assertNoSuspected();
    } finally {
      cm_a.destroy();
      cm_b.destroy();
      cm_rc.destroy();
      cm_rd.destroy();
    }
  }

  @Test
  public void testClusterMembersWellknownMembersLimited() {
    TransportEndpoint a = TransportEndpoint.from("tcp://a@localhost:20123");
    TransportEndpoint b = TransportEndpoint.from("tcp://b@localhost:20124");
    TransportEndpoint c = TransportEndpoint.from("tcp://c@localhost:20125");
    TransportEndpoint d = TransportEndpoint.from("tcp://d@localhost:20126");
    TransportEndpoint e = TransportEndpoint.from("tcp://e@localhost:20127");

    ClusterMembershipBuilder cm_a = ClusterMembershipBuilder.CMBuilder(a).init();
    ClusterMembershipBuilder cm_b = CMBuilder(b, a.address()).init();
    ClusterMembershipBuilder cm_c = CMBuilder(c, a.address()).init();
    ClusterMembershipBuilder cm_d = CMBuilder(d, b.address()).init();
    ClusterMembershipBuilder cm_e = CMBuilder(e, b.address()).init();

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
