package io.servicefabric.cluster.fdetector;

import static com.google.common.collect.ImmutableList.of;
import static io.servicefabric.cluster.ClusterEndpoint.from;
import static io.servicefabric.cluster.fdetector.FailureDetectorBuilder.FDBuilder;
import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import com.google.common.util.concurrent.SettableFuture;
import io.servicefabric.transport.ITransport;
import org.junit.Test;

import io.servicefabric.cluster.ClusterEndpoint;

public class FailureDetectorIT {

	@Test
	public void testAllTrusted() throws Exception {
		List<ClusterEndpoint> members = new ArrayList<>();
		ClusterEndpoint a = from("tcp://a@localhost:20123");
		ClusterEndpoint b = from("tcp://b@localhost:20124");
		ClusterEndpoint c = from("tcp://c@localhost:20125");
		members.add(a);
		members.add(b);
		members.add(c);

		List<FailureDetectorBuilder> builders = new ArrayList<>();
		builders.add(FDBuilder(a).set(members).ping(b).noRandomMembers());
		builders.add(FDBuilder(b).set(members).ping(c).noRandomMembers());
		builders.add(FDBuilder(c).set(members).ping(a).noRandomMembers());

		try {
			create(builders);
			TimeUnit.SECONDS.sleep(4);
			Map<ClusterEndpoint, ClusterEndpoint> target = getSuspected(builders);
			assertEquals("No suspected members is expected: " + target, 0, target.size());
		} finally {
			destroy(builders);
		}
	}

	@Test
	public void testBasicTrusted() throws Exception {
		List<ClusterEndpoint> members = new ArrayList<>();
		ClusterEndpoint a = from("tcp://a@localhost:20123");
		ClusterEndpoint b = from("tcp://b@localhost:20124");
		members.add(a);
		members.add(b);

		List<FailureDetectorBuilder> builders = new ArrayList<>();
		builders.add(FDBuilder(a).set(members));
		builders.add(FDBuilder(b).set(members));

		try {
			create(builders);
			TimeUnit.SECONDS.sleep(4);
			Map<ClusterEndpoint, ClusterEndpoint> target = getSuspected(builders);
			assertEquals("No suspected members is expected: " + target, 0, target.size());
		} finally {
			destroy(builders);
		}
	}

	@Test
	public void testTrustedDifferentPingTiming() throws Exception {
		List<ClusterEndpoint> members = new ArrayList<>();
		ClusterEndpoint a = from("tcp://a@localhost:20123");
		ClusterEndpoint b = from("tcp://b@localhost:20124");
		members.add(a);
		members.add(b);

		List<FailureDetectorBuilder> builders = new ArrayList<>();
		builders.add(FDBuilder(a).pingTime(100).set(members));
		builders.add(FDBuilder(b).pingTime(300).set(members));

		try {
			create(builders);
			TimeUnit.SECONDS.sleep(4);
			Map<ClusterEndpoint, ClusterEndpoint> target = getSuspected(builders);
			assertEquals("No suspected members is expected: " + target, 0, target.size());
		} finally {
			destroy(builders);
		}
	}

	@Test
	public void testAllSuspected() throws Exception {
		List<ClusterEndpoint> members = new ArrayList<>();
		ClusterEndpoint a = from("tcp://a@localhost:20123");
		ClusterEndpoint b = from("tcp://b@localhost:20124");
		ClusterEndpoint c = from("tcp://c@localhost:20125");
		members.add(a);
		members.add(b);
		members.add(c);

		List<FailureDetectorBuilder> builders = new ArrayList<>();
		builders.add(FDBuilder(a).set(members).ping(b).block(members));
		builders.add(FDBuilder(b).set(members).ping(c).block(members));
		builders.add(FDBuilder(c).set(members).ping(a).block(members));

		try {
			create(builders);
			TimeUnit.SECONDS.sleep(4);
			Map<ClusterEndpoint, ClusterEndpoint> target = getSuspected(builders);
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
		List<ClusterEndpoint> members = new ArrayList<>();
		ClusterEndpoint a = from("tcp://a@localhost:20123");
		ClusterEndpoint b = from("tcp://b@localhost:20124");
		members.add(a);
		members.add(b);

		List<FailureDetectorBuilder> builders = new ArrayList<>();
		builders.add(FDBuilder(a).set(members).ping(b).block(members));
		builders.add(FDBuilder(b).set(members).ping(a).block(members));

		try {
			create(builders);
			TimeUnit.SECONDS.sleep(4);
			Map<ClusterEndpoint, ClusterEndpoint> target = getSuspected(builders);
			assertEquals("Expected 2 suspected members: " + target, 2, target.size());
			assertEquals(b, target.get(a));
			assertEquals(a, target.get(b));
		} finally {
			destroy(builders);
		}
	}

	@Test
	public void testAllTrustedDespiteTrafficIssue() throws Exception {
		List<ClusterEndpoint> members = new ArrayList<>();
		ClusterEndpoint a = from("tcp://a@localhost:20123");
		ClusterEndpoint b = from("tcp://b@localhost:20124");
		ClusterEndpoint c = from("tcp://c@localhost:20125");
		members.add(a);
		members.add(b);
		members.add(c);

		List<FailureDetectorBuilder> builders = new ArrayList<>();
		builders.add(FDBuilder(a).set(members).ping(b).block(b));
		builders.add(FDBuilder(b).set(members).ping(c));
		builders.add(FDBuilder(c).set(members).ping(a));

		try {
			create(builders);
			TimeUnit.SECONDS.sleep(4);
			Map<ClusterEndpoint, ClusterEndpoint> target = getSuspected(builders);
			assertEquals("No suspected members is expected: " + target, 0, target.size());
		} finally {
			destroy(builders);
		}
	}

	@Test
	public void testSingleSuspectedNotAffectOthers() throws Exception {
		List<ClusterEndpoint> members = new ArrayList<>();
		ClusterEndpoint a = from("tcp://a@localhost:20123");
		ClusterEndpoint b = from("tcp://b@localhost:20124");
		ClusterEndpoint c = from("tcp://c@localhost:20125");
		ClusterEndpoint d = from("tcp://d@localhost:20126");
		members.add(a);
		members.add(b);
		members.add(c);
		members.add(d);

		List<FailureDetectorBuilder> builders = new ArrayList<>();
		builders.add(FDBuilder(a).pingTimeout(999).set(members).ping(b).block(b).noRandomMembers()); // a--X-->b and no neighbors
		builders.add(FDBuilder(b).pingTime(100).set(members).ping(a).noRandomMembers()); // ping a
		builders.add(FDBuilder(c).pingTime(100).set(members).ping(a).noRandomMembers()); // ping a
		builders.add(FDBuilder(d).pingTime(100).set(members).ping(a).noRandomMembers()); // ping a

		try {
			create(builders);
			TimeUnit.SECONDS.sleep(4);
			Map<ClusterEndpoint, ClusterEndpoint> target = getSuspected(builders);
			assertEquals("Expected 2 suspected members: " + target, 2, target.size());
			assertEquals(b, target.get(a));
			assertEquals(a, target.get(b));
		} finally {
			destroy(builders);
		}
	}

	@Test
	public void testTwoSuspectedNotAffectOthers() throws Exception {
		List<ClusterEndpoint> members = new ArrayList<>();
		ClusterEndpoint a = from("tcp://a@localhost:20123");
		ClusterEndpoint b = from("tcp://b@localhost:20124");
		ClusterEndpoint c = from("tcp://c@localhost:20125");
		ClusterEndpoint d = from("tcp://d@localhost:20126");
		ClusterEndpoint e = from("tcp://e@localhost:20127");
		members.add(a);
		members.add(b);
		members.add(c);
		members.add(d);
		members.add(e);

		List<FailureDetectorBuilder> builders = new ArrayList<>();
		builders.add(FDBuilder(a).pingTimeout(499).set(members).ping(b).block(b).randomMembers(of(c)).block(c)); // a--X-->b then a--X-->c
		builders.add(FDBuilder(b).pingTime(100).set(members).ping(a).noRandomMembers()); // ping a
		builders.add(FDBuilder(c).pingTime(100).set(members).ping(a).noRandomMembers()); // ping a
		builders.add(FDBuilder(d).pingTime(100).set(members).ping(a).noRandomMembers()); // ping a
		builders.add(FDBuilder(e).pingTime(100).set(members).ping(a).noRandomMembers()); // ping a

		try {
			create(builders);
			TimeUnit.SECONDS.sleep(4);
			Map<ClusterEndpoint, ClusterEndpoint> target = getSuspected(builders);
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
		List<ClusterEndpoint> members = new ArrayList<>();
		ClusterEndpoint a = from("tcp://a@localhost:20123");
		ClusterEndpoint b = from("tcp://b@localhost:20124");
		ClusterEndpoint c = from("tcp://c@localhost:20125");
		ClusterEndpoint x = from("tcp://x@localhost:20126");
		members.add(a);
		members.add(b);
		members.add(c);
		members.add(x);

		List<FailureDetectorBuilder> builders = new ArrayList<>();
		builders.add(FDBuilder(a).set(members).ping(x).block(x));
		builders.add(FDBuilder(b).set(members).ping(x).block(x));
		builders.add(FDBuilder(c).set(members).ping(x).block(x));
		builders.add(FDBuilder(x).set(members).ping(a));

		try {
			create(builders);
			TimeUnit.SECONDS.sleep(4);
			Map<ClusterEndpoint, ClusterEndpoint> target = getSuspected(builders);
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
		List<ClusterEndpoint> members = new ArrayList<>();
		ClusterEndpoint a = from("tcp://a@localhost:20123");
		ClusterEndpoint b = from("tcp://b@localhost:20124");
		ClusterEndpoint d = from("tcp://d@localhost:20125");
		ClusterEndpoint x = from("tcp://x@localhost:20126");
		members.add(a);
		members.add(b);
		members.add(d);
		members.add(x);

		List<FailureDetectorBuilder> builders = new ArrayList<>();
		builders.add(FDBuilder(a).set(members).ping(x).block(x));
		builders.add(FDBuilder(b).set(members).ping(a).block(x));
		builders.add(FDBuilder(d).set(members).ping(a).block(x));
		builders.add(FDBuilder(x).pingTime(100500).set(members).ping(b));

		try {
			create(builders);
			TimeUnit.SECONDS.sleep(4);
			Map<ClusterEndpoint, ClusterEndpoint> target = getSuspected(builders);
			assertEquals("Expected 1 suspected members: " + target, 1, target.size());
			assertEquals(x, target.get(a));
		} finally {
			destroy(builders);
		}
	}

	@Test
	public void testMemberBecomeTrusted() throws Exception {
		List<ClusterEndpoint> members = new ArrayList<>();
		ClusterEndpoint a = from("tcp://a@localhost:20123");
		ClusterEndpoint b = from("tcp://b@localhost:20124");
		members.add(a);
		members.add(b);

		List<FailureDetectorBuilder> builders = new ArrayList<>();
		builders.add(FDBuilder(a).set(members).block(b)); // traffic is blocked initially
		builders.add(FDBuilder(b).set(members).block(a)); // traffic is blocked initially

		try {
			create(builders);
			TimeUnit.SECONDS.sleep(4);
			Map<ClusterEndpoint, ClusterEndpoint> targetSuspect0 = getSuspected(builders);
			assertEquals("Expected 2 suspected members: " + targetSuspect0, 2, targetSuspect0.size());
			assertEquals(b, targetSuspect0.get(a));
			assertEquals(a, targetSuspect0.get(b));

			unblock(builders); // unblock all traffic
			TimeUnit.SECONDS.sleep(4);
			Map<ClusterEndpoint, ClusterEndpoint> targetSuspect1 = getSuspected(builders);
			assertEquals("No suspected members is expected: " + targetSuspect1, 0, targetSuspect1.size());
		} finally {
			destroy(builders);
		}
	}

	@Test
	public void testMemberBecomeSuspected() throws Exception {
		List<ClusterEndpoint> members = new ArrayList<>();
		ClusterEndpoint a = from("tcp://a@localhost:20123");
		ClusterEndpoint b = from("tcp://b@localhost:20124");
		ClusterEndpoint x = from("tcp://x@localhost:20125");
		ClusterEndpoint y = from("tcp://y@localhost:20126");
		members.add(a);
		members.add(b);
		members.add(x);
		members.add(y);

		List<FailureDetectorBuilder> builders = new ArrayList<>();
		builders.add(FDBuilder(a).set(members).ping(x));
		builders.add(FDBuilder(b).set(members).ping(y));
		builders.add(FDBuilder(x).set(members));
		builders.add(FDBuilder(y).set(members));

		try {
			create(builders);
			TimeUnit.SECONDS.sleep(4);
			Map<ClusterEndpoint, ClusterEndpoint> targetSuspect0 = getSuspected(builders);
			assertEquals("No suspected members is expected: " + targetSuspect0, 0, targetSuspect0.size());

			destroy(x, builders);
			destroy(y, builders);
			TimeUnit.SECONDS.sleep(4);
			Map<ClusterEndpoint, ClusterEndpoint> targetSuspect1 = getSuspected(builders);
			assertEquals("Expected 2 suspected members: " + targetSuspect1, 2, targetSuspect1.size());
			assertEquals(x, targetSuspect1.get(a));
			assertEquals(y, targetSuspect1.get(b));
		} finally {
			destroy(builders);
		}
	}

	@Test
	public void testMemberBecomeSuspectedIncarnationRespected() throws Exception {
		List<ClusterEndpoint> members = new ArrayList<>();
		ClusterEndpoint a = from("tcp://a@localhost:20123");
		ClusterEndpoint b = from("tcp://b@localhost:20124");
		ClusterEndpoint x = from("tcp://x@localhost:20125");
		members.add(a);
		members.add(b);
		members.add(x);

		List<FailureDetectorBuilder> builders = new ArrayList<>();
		builders.add(FDBuilder(a).set(members).pingTime(100).ping(x));
		builders.add(FDBuilder(b).set(members).pingTime(100).ping(x));
		builders.add(FDBuilder(x).pingTime(100500).set(members));

		try {
			create(builders);
			TimeUnit.SECONDS.sleep(4);
			Map<ClusterEndpoint, ClusterEndpoint> targetSuspect0 = getSuspected(builders);
			assertEquals("No suspected members is expected: " + targetSuspect0, 0, targetSuspect0.size());

			destroy(x, builders);
			TimeUnit.SECONDS.sleep(4);
			Map<ClusterEndpoint, ClusterEndpoint> targetSuspect1 = getSuspected(builders);
			assertEquals("Expected 2 suspected members: " + targetSuspect1, 2, targetSuspect1.size());
			assertEquals(x, targetSuspect1.get(a));
			assertEquals(x, targetSuspect1.get(b));

			ClusterEndpoint xx = from("tcp://xx@localhost:20125");
			members.add(xx);
			FailureDetectorBuilder xxBuilder = FDBuilder(xx).set(members).pingTime(100).ping(x);
			builders.add(xxBuilder);
			for (FailureDetectorBuilder builder : builders) {
				builder.set(members);
			}
			{
				xxBuilder.init();
			}
			TimeUnit.SECONDS.sleep(4);
			Map<ClusterEndpoint, ClusterEndpoint> targetSuspect2 = getSuspected(builders);
			assertEquals("Expected 3 suspected members: " + targetSuspect2, 3, targetSuspect2.size());
			assertEquals(x, targetSuspect2.get(a));
			assertEquals(x, targetSuspect2.get(b));
			assertEquals(x, targetSuspect2.get(xx));
		} finally {
			destroy(builders);
		}
	}

	private Map<ClusterEndpoint, ClusterEndpoint> getSuspected(Iterable<FailureDetectorBuilder> builders) {
		Map<ClusterEndpoint, ClusterEndpoint> target = new HashMap<>();
		for (FailureDetectorBuilder builder : builders) {
			List<ClusterEndpoint> suspectedMembers = builder.target.getSuspectedMembers();
			if (!suspectedMembers.isEmpty()) {
				ClusterEndpoint localEndpoint = builder.target.getLocalEndpoint();
				assertEquals(localEndpoint + ": " + suspectedMembers, 1, suspectedMembers.size());
				target.put(localEndpoint, suspectedMembers.get(0));
			}
		}
		return target;
	}

	private void destroy(Iterable<FailureDetectorBuilder> builders) {
		for (FailureDetectorBuilder builder : builders) {
			builder.target.stop();
			destroyTransport(builder.target.getTransport());
		}
	}

	private void destroy(ClusterEndpoint endpoint, Iterable<FailureDetectorBuilder> builders) {
		for (FailureDetectorBuilder builder : builders) {
			if (builder.target.getLocalEndpoint() == endpoint) {
				builder.target.stop();
				destroyTransport(builder.target.getTransport());
				return;
			}
		}
		throw new IllegalArgumentException(endpoint.toString());
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
