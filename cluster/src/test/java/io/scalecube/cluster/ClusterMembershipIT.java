package io.scalecube.cluster;

import static com.google.common.base.Throwables.propagate;

import io.scalecube.transport.Address;
import io.scalecube.transport.Transport;

import com.google.common.collect.ImmutableList;

import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class ClusterMembershipIT {

  @Test
  public void testInitialPhaseOk() {
    Transport a = Transport.bindAwait(true);
    Transport b = Transport.bindAwait(true);
    Transport c = Transport.bindAwait(true);
    List<Address> members = ImmutableList.of(a.address(), b.address(), c.address());

    ClusterMembershipBuilder cm_a = ClusterMembershipBuilder.CMBuilder(a, members).init();
    ClusterMembershipBuilder cm_b = ClusterMembershipBuilder.CMBuilder(b, members).init();
    ClusterMembershipBuilder cm_c = ClusterMembershipBuilder.CMBuilder(c, members).init();

    try {
      awaitSeconds(3);

      cm_a.assertTrusted(a.address(), b.address(), c.address()).assertNoSuspected();
      cm_b.assertTrusted(a.address(), b.address(), c.address()).assertNoSuspected();
      cm_c.assertTrusted(a.address(), b.address(), c.address()).assertNoSuspected();
    } finally {
      cm_a.destroy();
      cm_b.destroy();
      cm_c.destroy();
    }
  }

  @Test
  public void testInitialPhaseWithNetworkPartitionThenRecovery() {
    Transport a = Transport.bindAwait(true);
    Transport b = Transport.bindAwait(true);
    Transport c = Transport.bindAwait(true);
    List<Address> members = ImmutableList.of(a.address(), b.address(), c.address());

    ClusterMembershipBuilder cm_a = ClusterMembershipBuilder.CMBuilder(a, members).block(b.address()).block(c.address()).init();
    ClusterMembershipBuilder cm_b = ClusterMembershipBuilder.CMBuilder(b, members).block(a.address()).block(c.address()).init();
    ClusterMembershipBuilder cm_c = ClusterMembershipBuilder.CMBuilder(c, members).block(a.address()).block(b.address()).init();

    try {
      awaitSeconds(3);

      cm_a.assertTrusted(a.address()).assertNoSuspected();
      cm_b.assertTrusted(b.address()).assertNoSuspected();
      cm_c.assertTrusted(c.address()).assertNoSuspected();

      cm_a.unblockAll();
      cm_b.unblockAll();
      cm_c.unblockAll();

      awaitSeconds(3);

      cm_a.assertTrusted(a.address(), b.address(), c.address()).assertNoSuspected();
      cm_b.assertTrusted(a.address(), b.address(), c.address()).assertNoSuspected();
      cm_c.assertTrusted(a.address(), b.address(), c.address()).assertNoSuspected();
    } finally {
      cm_a.destroy();
      cm_b.destroy();
      cm_c.destroy();
    }
  }

  @Test
  public void testRunningPhaseOk() {
    Transport a = Transport.bindAwait(true);
    Transport b = Transport.bindAwait(true);
    Transport c = Transport.bindAwait(true);
    List<Address> members = ImmutableList.of(a.address(), b.address(), c.address());

    ClusterMembershipBuilder cm_a = ClusterMembershipBuilder.CMBuilder(a, members).init();
    ClusterMembershipBuilder cm_b = ClusterMembershipBuilder.CMBuilder(b, members).init();
    ClusterMembershipBuilder cm_c = ClusterMembershipBuilder.CMBuilder(c, members).init();

    try {
      awaitSeconds(3);

      cm_a.assertTrusted(a.address(), b.address(), c.address()).assertNoSuspected();
      cm_b.assertTrusted(a.address(), b.address(), c.address()).assertNoSuspected();
      cm_c.assertTrusted(a.address(), b.address(), c.address()).assertNoSuspected();

      cm_a.block(b.address()).block(c.address());
      cm_b.block(a.address()).block(c.address());
      cm_c.block(a.address()).block(b.address());

      awaitSeconds(3);

      cm_a.assertTrusted(a.address()).assertSuspected(b.address(), c.address());
      cm_b.assertTrusted(b.address()).assertSuspected(a.address(), c.address());
      cm_c.assertTrusted(c.address()).assertSuspected(a.address(), b.address());

      cm_a.unblockAll();
      cm_b.unblockAll();
      cm_c.unblockAll();

      awaitSeconds(3);

      cm_a.assertTrusted(a.address(), b.address(), c.address()).assertNoSuspected();
      cm_b.assertTrusted(a.address(), b.address(), c.address()).assertNoSuspected();
      cm_c.assertTrusted(a.address(), b.address(), c.address()).assertNoSuspected();
    } finally {
      cm_a.destroy();
      cm_b.destroy();
      cm_c.destroy();
    }
  }

  @Test
  public void testLongNetworkPartitionNoRecovery() {
    Transport a = Transport.bindAwait(true);
    Transport b = Transport.bindAwait(true);
    Transport c = Transport.bindAwait(true);
    Transport d = Transport.bindAwait(true);
    List<Address> members = ImmutableList.of(a.address(), b.address(), c.address(), d.address());

    ClusterMembershipBuilder cm_a = ClusterMembershipBuilder.CMBuilder(a, members, 3000).init();
    ClusterMembershipBuilder cm_b = ClusterMembershipBuilder.CMBuilder(b, members, 3000).init();
    ClusterMembershipBuilder cm_c = ClusterMembershipBuilder.CMBuilder(c, members, 3000).init();
    ClusterMembershipBuilder cm_d = ClusterMembershipBuilder.CMBuilder(d, members, 3000).init();

    try {
      awaitSeconds(3);

      cm_a.assertTrusted(a.address(), b.address(), c.address(), d.address());
      cm_b.assertTrusted(a.address(), b.address(), c.address(), d.address());
      cm_c.assertTrusted(a.address(), b.address(), c.address(), d.address());
      cm_d.assertTrusted(a.address(), b.address(), c.address(), d.address());

      cm_a.block(c.address()).block(d.address());
      cm_b.block(c.address()).block(d.address());
      cm_c.block(a.address()).block(b.address());
      cm_d.block(a.address()).block(b.address());

      awaitSeconds(3);

      cm_a.assertTrusted(a.address(), b.address()).assertSuspected(c.address(), d.address());
      cm_b.assertTrusted(a.address(), b.address()).assertSuspected(c.address(), d.address());
      cm_c.assertTrusted(c.address(), d.address()).assertSuspected(a.address(), b.address());
      cm_d.assertTrusted(c.address(), d.address()).assertSuspected(a.address(), b.address());

      awaitSeconds(3);
      awaitSeconds(3);

      cm_a.assertTrusted(a.address(), b.address()).assertNoSuspected();
      cm_b.assertTrusted(a.address(), b.address()).assertNoSuspected();
      cm_c.assertTrusted(c.address(), d.address()).assertNoSuspected();
      cm_d.assertTrusted(c.address(), d.address()).assertNoSuspected();
    } finally {
      cm_a.destroy();
      cm_b.destroy();
      cm_c.destroy();
      cm_d.destroy();
    }
  }

  @Test
  public void testRestartFailedMembers() {
    Transport a = Transport.bindAwait(true);
    Transport b = Transport.bindAwait(true);
    Transport c = Transport.bindAwait(true);
    Transport d = Transport.bindAwait(true);
    List<Address> members = ImmutableList.of(a.address(), b.address(), c.address(), d.address());

    ClusterMembershipBuilder cm_a = ClusterMembershipBuilder.CMBuilder(a, members, 3000).init();
    ClusterMembershipBuilder cm_b = ClusterMembershipBuilder.CMBuilder(b, members, 3000).init();
    ClusterMembershipBuilder cm_c = ClusterMembershipBuilder.CMBuilder(c, members).init();
    ClusterMembershipBuilder cm_d = ClusterMembershipBuilder.CMBuilder(d, members).init();

    ClusterMembershipBuilder cm_restartedC = null;
    ClusterMembershipBuilder cm_restartedD = null;

    try {
      awaitSeconds(3);

      cm_a.assertTrusted(a.address(), b.address(), c.address(), d.address());
      cm_b.assertTrusted(a.address(), b.address(), c.address(), d.address());
      cm_c.assertTrusted(a.address(), b.address(), c.address(), d.address());
      cm_d.assertTrusted(a.address(), b.address(), c.address(), d.address());

      cm_c.destroy();
      cm_d.destroy();

      awaitSeconds(3);

      cm_a.assertTrusted(a.address(), b.address()).assertSuspected(c.address(), d.address());
      cm_b.assertTrusted(a.address(), b.address()).assertSuspected(c.address(), d.address());

      awaitSeconds(3);
      awaitSeconds(3);

      cm_a.assertTrusted(a.address(), b.address()).assertNoSuspected();
      cm_b.assertTrusted(a.address(), b.address()).assertNoSuspected();

      cm_restartedC = ClusterMembershipBuilder.CMBuilder(c, Arrays.asList(a.address(), b.address())).init();
      cm_restartedD = ClusterMembershipBuilder.CMBuilder(d, Arrays.asList(a.address(), b.address())).init();

      awaitSeconds(3);

      cm_restartedC.assertTrusted(a.address(), b.address(), c.address(), d.address()).assertNoSuspected();
      cm_restartedD.assertTrusted(a.address(), b.address(), c.address(), d.address()).assertNoSuspected();
      cm_a.assertTrusted(a.address(), b.address(), c.address(), d.address()).assertNoSuspected();
      cm_b.assertTrusted(a.address(), b.address(), c.address(), d.address()).assertNoSuspected();
    } finally {
      cm_a.destroy();
      cm_b.destroy();
      cm_restartedC.destroy();
      cm_restartedD.destroy();
    }
  }

  @Test
  public void testClusterMembersWellknownMembersLimited() {
    Transport a = Transport.bindAwait(true);
    Transport b = Transport.bindAwait(true);
    Transport c = Transport.bindAwait(true);
    Transport d = Transport.bindAwait(true);
    Transport e = Transport.bindAwait(true);

    ClusterMembershipBuilder cm_a = ClusterMembershipBuilder.CMBuilder(a, Collections.<Address>emptyList()).init();
    ClusterMembershipBuilder cm_b = ClusterMembershipBuilder.CMBuilder(b, Collections.singletonList(a.address())).init();
    ClusterMembershipBuilder cm_c = ClusterMembershipBuilder.CMBuilder(c, Collections.singletonList(a.address())).init();
    ClusterMembershipBuilder cm_d = ClusterMembershipBuilder.CMBuilder(d, Collections.singletonList(b.address())).init();
    ClusterMembershipBuilder cm_e = ClusterMembershipBuilder.CMBuilder(e, Collections.singletonList(b.address())).init();

    try {
      awaitSeconds(3);

      cm_a.assertTrusted(a.address(), b.address(), c.address(), d.address(), e.address()).assertNoSuspected();
      cm_b.assertTrusted(a.address(), b.address(), c.address(), d.address(), e.address()).assertNoSuspected();
      cm_c.assertTrusted(a.address(), b.address(), c.address(), d.address(), e.address()).assertNoSuspected();
      cm_d.assertTrusted(a.address(), b.address(), c.address(), d.address(), e.address()).assertNoSuspected();
      cm_e.assertTrusted(a.address(), b.address(), c.address(), d.address(), e.address()).assertNoSuspected();
    } finally {
      cm_a.destroy();
      cm_b.destroy();
      cm_c.destroy();
      cm_d.destroy();
      cm_e.destroy();
    }
  }

  private void awaitSeconds(int seconds) {
    try {
      TimeUnit.SECONDS.sleep(seconds);
    } catch (InterruptedException e) {
      propagate(e);
    }
  }
}
