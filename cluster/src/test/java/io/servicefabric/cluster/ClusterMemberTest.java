package io.servicefabric.cluster;

import static io.servicefabric.cluster.ClusterEndpoint.from;
import static io.servicefabric.cluster.ClusterMemberStatus.*;
import static org.junit.Assert.assertEquals;

import java.util.HashMap;

import org.junit.Test;

public class ClusterMemberTest {

	@Test
	public void testCompareSameStatus() {
		assertEquals(0, r0(SUSPECTED).compareTo(r1(SUSPECTED)));
		assertEquals(0, r0(TRUSTED).compareTo(r1(TRUSTED)));
		assertEquals(0, r0(REMOVED).compareTo(r1(REMOVED)));
		assertEquals(0, r0(SHUTDOWN).compareTo(r1(SHUTDOWN)));
	}

	@Test
	public void testCompareShutdown() throws Exception {
		assertEquals(1, r0(SHUTDOWN).compareTo(r1(TRUSTED)));
		assertEquals(1, r0(SHUTDOWN).compareTo(r1(SUSPECTED)));
		assertEquals(1, r0(SHUTDOWN).compareTo(r1(REMOVED)));

		assertEquals(-1, r0(TRUSTED).compareTo(r1(SHUTDOWN)));
		assertEquals(-1, r0(SUSPECTED).compareTo(r1(SHUTDOWN)));
		assertEquals(-1, r0(REMOVED).compareTo(r1(SHUTDOWN)));
	}

	@Test
	public void testCompareWithTimestamp() {
		assertEquals(1, r0(SUSPECTED, 1).compareTo(r1(TRUSTED, 1)));
		assertEquals(-1, r0(TRUSTED, 1).compareTo(r1(SUSPECTED, 1)));

		assertEquals(-1, r0(SUSPECTED, 1).compareTo(r1(TRUSTED, 2)));
		assertEquals(1, r0(SUSPECTED, 2).compareTo(r1(TRUSTED, 1)));

		assertEquals(-1, r0(TRUSTED, 1).compareTo(r1(SUSPECTED, 2)));
		assertEquals(1, r0(TRUSTED, 2).compareTo(r1(SUSPECTED, 1)));
	}

	private ClusterMember r0(ClusterMemberStatus status) {
		return new ClusterMember(from("tcp://id0@host:0"), status, new HashMap<String, String>());
	}

	private ClusterMember r1(ClusterMemberStatus status) {
		return new ClusterMember(from("tcp://id1@host:1"), status, new HashMap<String, String>());
	}

	private ClusterMember r0(ClusterMemberStatus status, long timestamp) {
		return new ClusterMember(from("tcp://id0@host:0"), status, new HashMap<String, String>(), timestamp);
	}

	private ClusterMember r1(ClusterMemberStatus status, long timestamp) {
		return new ClusterMember(from("tcp://id1@host:1"), status, new HashMap<String, String>(), timestamp);
	}
}
