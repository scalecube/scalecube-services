package io.scalecube.cluster.membership;

import static com.google.common.base.Throwables.propagate;

import io.scalecube.transport.Address;
import io.scalecube.transport.Transport;

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.SettableFuture;

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

    ClusterMembershipBuilder cm_a = ClusterMembershipBuilder.CMBuilder(a, members).start();
    ClusterMembershipBuilder cm_b = ClusterMembershipBuilder.CMBuilder(b, members).start();
    ClusterMembershipBuilder cm_c = ClusterMembershipBuilder.CMBuilder(c, members).start();

    try {
      awaitSeconds(3);

      cm_a.assertTrusted(a.address(), b.address(), c.address()).assertNoSuspected();
      cm_b.assertTrusted(a.address(), b.address(), c.address()).assertNoSuspected();
      cm_c.assertTrusted(a.address(), b.address(), c.address()).assertNoSuspected();
    } finally {
      cm_a.stop();
      cm_b.stop();
      cm_c.stop();
      destroyTransports(a, b, c);
    }
  }

  @Test
  public void testInitialPhaseWithNetworkPartitionThenRecovery() {
    Transport a = Transport.bindAwait(true);
    Transport b = Transport.bindAwait(true);
    Transport c = Transport.bindAwait(true);
    List<Address> members = ImmutableList.of(a.address(), b.address(), c.address());

    ClusterMembershipBuilder cm_a = ClusterMembershipBuilder.CMBuilder(a, members).start();
    ClusterMembershipBuilder cm_b = ClusterMembershipBuilder.CMBuilder(b, members).start();
    ClusterMembershipBuilder cm_c = ClusterMembershipBuilder.CMBuilder(c, members).start();

    // Block traffic
    a.block(members);
    b.block(members);
    c.block(members);

    try {
      awaitSeconds(3);

      cm_a.assertTrusted(a.address()).assertNoSuspected();
      cm_b.assertTrusted(b.address()).assertNoSuspected();
      cm_c.assertTrusted(c.address()).assertNoSuspected();

      a.unblockAll();
      b.unblockAll();
      c.unblockAll();

      awaitSeconds(3);

      cm_a.assertTrusted(a.address(), b.address(), c.address()).assertNoSuspected();
      cm_b.assertTrusted(a.address(), b.address(), c.address()).assertNoSuspected();
      cm_c.assertTrusted(a.address(), b.address(), c.address()).assertNoSuspected();
    } finally {
      cm_a.stop();
      cm_b.stop();
      cm_c.stop();
      destroyTransports(a, b, c);
    }
  }

  @Test
  public void testRunningPhaseOk() {
    Transport a = Transport.bindAwait(true);
    Transport b = Transport.bindAwait(true);
    Transport c = Transport.bindAwait(true);
    List<Address> members = ImmutableList.of(a.address(), b.address(), c.address());

    ClusterMembershipBuilder cm_a = ClusterMembershipBuilder.CMBuilder(a, members).start();
    ClusterMembershipBuilder cm_b = ClusterMembershipBuilder.CMBuilder(b, members).start();
    ClusterMembershipBuilder cm_c = ClusterMembershipBuilder.CMBuilder(c, members).start();

    try {
      awaitSeconds(3);

      cm_a.assertTrusted(a.address(), b.address(), c.address()).assertNoSuspected();
      cm_b.assertTrusted(a.address(), b.address(), c.address()).assertNoSuspected();
      cm_c.assertTrusted(a.address(), b.address(), c.address()).assertNoSuspected();

      a.block(members);
      b.block(members);
      c.block(members);

      awaitSeconds(3);

      cm_a.assertTrusted(a.address()).assertSuspected(b.address(), c.address());
      cm_b.assertTrusted(b.address()).assertSuspected(a.address(), c.address());
      cm_c.assertTrusted(c.address()).assertSuspected(a.address(), b.address());

      a.unblockAll();
      b.unblockAll();
      c.unblockAll();

      awaitSeconds(3);

      cm_a.assertTrusted(a.address(), b.address(), c.address()).assertNoSuspected();
      cm_b.assertTrusted(a.address(), b.address(), c.address()).assertNoSuspected();
      cm_c.assertTrusted(a.address(), b.address(), c.address()).assertNoSuspected();
    } finally {
      cm_a.stop();
      cm_b.stop();
      cm_c.stop();
      destroyTransports(a, b, c);
    }
  }

  @Test
  public void testLongNetworkPartitionNoRecovery() {
    Transport a = Transport.bindAwait(true);
    Transport b = Transport.bindAwait(true);
    Transport c = Transport.bindAwait(true);
    Transport d = Transport.bindAwait(true);
    List<Address> members = ImmutableList.of(a.address(), b.address(), c.address(), d.address());

    ClusterMembershipBuilder cm_a = ClusterMembershipBuilder.CMBuilder(a, members).start();
    ClusterMembershipBuilder cm_b = ClusterMembershipBuilder.CMBuilder(b, members).start();
    ClusterMembershipBuilder cm_c = ClusterMembershipBuilder.CMBuilder(c, members).start();
    ClusterMembershipBuilder cm_d = ClusterMembershipBuilder.CMBuilder(d, members).start();

    try {
      awaitSeconds(3);

      cm_a.assertTrusted(a.address(), b.address(), c.address(), d.address());
      cm_b.assertTrusted(a.address(), b.address(), c.address(), d.address());
      cm_c.assertTrusted(a.address(), b.address(), c.address(), d.address());
      cm_d.assertTrusted(a.address(), b.address(), c.address(), d.address());

      a.block(Arrays.asList(c.address(), d.address()));
      b.block(Arrays.asList(c.address(), d.address()));
      c.block(Arrays.asList(a.address(), b.address()));
      d.block(Arrays.asList(a.address(), b.address()));

      awaitSeconds(3);

      cm_a.assertTrusted(a.address(), b.address()).assertSuspected(c.address(), d.address());
      cm_b.assertTrusted(a.address(), b.address()).assertSuspected(c.address(), d.address());
      cm_c.assertTrusted(c.address(), d.address()).assertSuspected(a.address(), b.address());
      cm_d.assertTrusted(c.address(), d.address()).assertSuspected(a.address(), b.address());

      awaitSeconds(3); // 3 + 3 > max suspect time (5)

      cm_a.assertTrusted(a.address(), b.address()).assertNoSuspected();
      cm_b.assertTrusted(a.address(), b.address()).assertNoSuspected();
      cm_c.assertTrusted(c.address(), d.address()).assertNoSuspected();
      cm_d.assertTrusted(c.address(), d.address()).assertNoSuspected();
    } finally {
      cm_a.stop();
      cm_b.stop();
      cm_c.stop();
      cm_d.stop();
      destroyTransports(a, b, c, d);
    }
  }

  @Test
  public void testRestartFailedMembers() {
    Transport a = Transport.bindAwait(true);
    Transport b = Transport.bindAwait(true);
    Transport c = Transport.bindAwait(true);
    Transport d = Transport.bindAwait(true);
    List<Address> members = ImmutableList.of(a.address(), b.address(), c.address(), d.address());

    ClusterMembershipBuilder cm_a = ClusterMembershipBuilder.CMBuilder(a, members).start();
    ClusterMembershipBuilder cm_b = ClusterMembershipBuilder.CMBuilder(b, members).start();
    ClusterMembershipBuilder cm_c = ClusterMembershipBuilder.CMBuilder(c, members).start();
    ClusterMembershipBuilder cm_d = ClusterMembershipBuilder.CMBuilder(d, members).start();

    ClusterMembershipBuilder cm_restartedC = null;
    ClusterMembershipBuilder cm_restartedD = null;

    try {
      awaitSeconds(3);

      cm_a.assertTrusted(a.address(), b.address(), c.address(), d.address());
      cm_b.assertTrusted(a.address(), b.address(), c.address(), d.address());
      cm_c.assertTrusted(a.address(), b.address(), c.address(), d.address());
      cm_d.assertTrusted(a.address(), b.address(), c.address(), d.address());

      cm_c.stop();
      cm_d.stop();

      awaitSeconds(3);

      cm_a.assertTrusted(a.address(), b.address()).assertSuspected(c.address(), d.address());
      cm_b.assertTrusted(a.address(), b.address()).assertSuspected(c.address(), d.address());

      awaitSeconds(3); // 3 + 3 > max suspect time (5)

      cm_a.assertTrusted(a.address(), b.address()).assertNoSuspected();
      cm_b.assertTrusted(a.address(), b.address()).assertNoSuspected();

      cm_restartedC = ClusterMembershipBuilder.CMBuilder(c, Arrays.asList(a.address(), b.address())).start();
      cm_restartedD = ClusterMembershipBuilder.CMBuilder(d, Arrays.asList(a.address(), b.address())).start();

      awaitSeconds(3);

      cm_restartedC.assertTrusted(a.address(), b.address(), c.address(), d.address()).assertNoSuspected();
      cm_restartedD.assertTrusted(a.address(), b.address(), c.address(), d.address()).assertNoSuspected();
      cm_a.assertTrusted(a.address(), b.address(), c.address(), d.address()).assertNoSuspected();
      cm_b.assertTrusted(a.address(), b.address(), c.address(), d.address()).assertNoSuspected();
    } finally {
      cm_a.stop();
      cm_b.stop();
      cm_restartedC.stop();
      cm_restartedD.stop();
      destroyTransports(a, b, c, d);
    }
  }

  @Test
  public void testClusterMembersWellknownMembersLimited() {
    Transport a = Transport.bindAwait(true);
    Transport b = Transport.bindAwait(true);
    Transport c = Transport.bindAwait(true);
    Transport d = Transport.bindAwait(true);
    Transport e = Transport.bindAwait(true);

    ClusterMembershipBuilder cm_a = ClusterMembershipBuilder.CMBuilder(a, Collections.<Address>emptyList()).start();
    ClusterMembershipBuilder cm_b = ClusterMembershipBuilder.CMBuilder(b, Collections.singletonList(a.address())).start();
    ClusterMembershipBuilder cm_c = ClusterMembershipBuilder.CMBuilder(c, Collections.singletonList(a.address())).start();
    ClusterMembershipBuilder cm_d = ClusterMembershipBuilder.CMBuilder(d, Collections.singletonList(b.address())).start();
    ClusterMembershipBuilder cm_e = ClusterMembershipBuilder.CMBuilder(e, Collections.singletonList(b.address())).start();

    try {
      awaitSeconds(3);

      cm_a.assertTrusted(a.address(), b.address(), c.address(), d.address(), e.address()).assertNoSuspected();
      cm_b.assertTrusted(a.address(), b.address(), c.address(), d.address(), e.address()).assertNoSuspected();
      cm_c.assertTrusted(a.address(), b.address(), c.address(), d.address(), e.address()).assertNoSuspected();
      cm_d.assertTrusted(a.address(), b.address(), c.address(), d.address(), e.address()).assertNoSuspected();
      cm_e.assertTrusted(a.address(), b.address(), c.address(), d.address(), e.address()).assertNoSuspected();
    } finally {
      cm_a.stop();
      cm_b.stop();
      cm_c.stop();
      cm_d.stop();
      cm_e.stop();
      destroyTransports(a, b, c, d, e);
    }
  }

  private void awaitSeconds(int seconds) {
    try {
      TimeUnit.SECONDS.sleep(seconds);
    } catch (InterruptedException e) {
      propagate(e);
    }
  }

  private void destroyTransports(Transport... transports) {
    for (Transport transport : transports) {
      destroyTransport(transport);
    }
  }

  private void destroyTransport(Transport transport) {
    if (transport != null && !transport.isStopped()) {
      SettableFuture<Void> close = SettableFuture.create();
      transport.stop(close);
      try {
        close.get(1, TimeUnit.SECONDS);
      } catch (Exception ignore) {
        // ignore
      }
    }
  }
}
