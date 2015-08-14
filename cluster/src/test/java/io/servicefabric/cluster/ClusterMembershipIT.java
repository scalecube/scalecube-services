package io.servicefabric.cluster;

import static com.google.common.base.Throwables.propagate;
import static io.servicefabric.cluster.ClusterMembershipBuilder.CMBuilder;

import java.util.List;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import com.google.common.collect.ImmutableList;
import io.servicefabric.transport.TransportEndpoint;

public class ClusterMembershipIT {

	@Test
	public void testInitialPhaseOk() {
		ClusterEndpoint a = ClusterEndpoint.from("tcp://a@localhost:20123");
		ClusterEndpoint b = ClusterEndpoint.from("tcp://b@localhost:20124");
		ClusterEndpoint c = ClusterEndpoint.from("tcp://c@localhost:20125");
		List<TransportEndpoint> members = ImmutableList.of(a.endpoint(), b.endpoint(), c.endpoint());

		ClusterMembershipBuilder cm_a = CMBuilder(a, members).init();
		ClusterMembershipBuilder cm_b = CMBuilder(b, members).init();
		ClusterMembershipBuilder cm_c = CMBuilder(c, members).init();

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
		ClusterEndpoint a = ClusterEndpoint.from("tcp://a@localhost:20123");
		ClusterEndpoint b = ClusterEndpoint.from("tcp://b@localhost:20124");
		ClusterEndpoint c = ClusterEndpoint.from("tcp://c@localhost:20125");
		List<TransportEndpoint> members = ImmutableList.of(a.endpoint(), b.endpoint(), c.endpoint());

		ClusterMembershipBuilder cm_a = CMBuilder(a, members).block(b).block(c).init();
		ClusterMembershipBuilder cm_b = CMBuilder(b, members).block(a).block(c).init();
		ClusterMembershipBuilder cm_c = CMBuilder(c, members).block(a).block(b).init();

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
		ClusterEndpoint a = ClusterEndpoint.from("tcp://a@localhost:20123");
		ClusterEndpoint b = ClusterEndpoint.from("tcp://b@localhost:20124");
		ClusterEndpoint c = ClusterEndpoint.from("tcp://c@localhost:20125");
		List<TransportEndpoint> members = ImmutableList.of(a.endpoint(), b.endpoint(), c.endpoint());

		ClusterMembershipBuilder cm_a = CMBuilder(a, members).init();
		ClusterMembershipBuilder cm_b = CMBuilder(b, members).init();
		ClusterMembershipBuilder cm_c = CMBuilder(c, members).init();

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
		ClusterEndpoint a = ClusterEndpoint.from("tcp://a@localhost:20123");
		ClusterEndpoint b = ClusterEndpoint.from("tcp://b@localhost:20124");
		ClusterEndpoint c = ClusterEndpoint.from("tcp://c@localhost:20125");
		ClusterEndpoint d = ClusterEndpoint.from("tcp://d@localhost:20126");
		List<TransportEndpoint> members = ImmutableList.of(a.endpoint(), b.endpoint(), c.endpoint(), d.endpoint());

		ClusterMembershipBuilder cm_a = CMBuilder(a, members).maxSuspectTime(3000).init();
		ClusterMembershipBuilder cm_b = CMBuilder(b, members).maxSuspectTime(3000).init();
		ClusterMembershipBuilder cm_c = CMBuilder(c, members).maxSuspectTime(3000).init();
		ClusterMembershipBuilder cm_d = CMBuilder(d, members).maxSuspectTime(3000).init();

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
		ClusterEndpoint a = ClusterEndpoint.from("tcp://a@localhost:20123");
		ClusterEndpoint b = ClusterEndpoint.from("tcp://b@localhost:20124");
		ClusterEndpoint c = ClusterEndpoint.from("tcp://c@localhost:20125");
		ClusterEndpoint d = ClusterEndpoint.from("tcp://d@localhost:20126");
		List<TransportEndpoint> members = ImmutableList.of(a.endpoint(), b.endpoint(), c.endpoint(), d.endpoint());

		ClusterMembershipBuilder cm_a = CMBuilder(a, members).maxSuspectTime(3000).init();
		ClusterMembershipBuilder cm_b = CMBuilder(b, members).maxSuspectTime(3000).init();
		ClusterMembershipBuilder cm_c = CMBuilder(c, members).init();
		ClusterMembershipBuilder cm_d = CMBuilder(d, members).init();

		ClusterEndpoint rc = ClusterEndpoint.from("tcp://restarted_c@localhost:20125");
		ClusterEndpoint rd = ClusterEndpoint.from("tcp://restarted_d@localhost:20126");
		ClusterMembershipBuilder cm_rc = CMBuilder(rc, a.endpoint(), b.endpoint());
		ClusterMembershipBuilder cm_rd = CMBuilder(rd, a.endpoint(), b.endpoint());

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
		ClusterEndpoint a = ClusterEndpoint.from("tcp://a@localhost:20123");
		ClusterEndpoint b = ClusterEndpoint.from("tcp://b@localhost:20124");
		ClusterEndpoint c = ClusterEndpoint.from("tcp://c@localhost:20125");
		ClusterEndpoint d = ClusterEndpoint.from("tcp://d@localhost:20126");
		ClusterEndpoint e = ClusterEndpoint.from("tcp://e@localhost:20127");

		ClusterMembershipBuilder cm_a = CMBuilder(a).init();
		ClusterMembershipBuilder cm_b = CMBuilder(b, a.endpoint()).init();
		ClusterMembershipBuilder cm_c = CMBuilder(c, a.endpoint()).init();
		ClusterMembershipBuilder cm_d = CMBuilder(d, b.endpoint()).init();
		ClusterMembershipBuilder cm_e = CMBuilder(e, b.endpoint()).init();

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
