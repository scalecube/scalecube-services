package io.servicefabric.cluster;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.net.UnknownHostException;
import java.util.List;

import io.servicefabric.transport.utils.IpAddressResolver;
import org.junit.Before;
import org.junit.Test;

import io.servicefabric.transport.TransportEndpoint;

public class ClusterMembershipTest {
	private String ipAddress;

	@Before
	public void setup() throws UnknownHostException {
		ipAddress = IpAddressResolver.resolveIpAddress().getHostAddress();
	}

	@Test
	public void testSetWellknownMembers() {
		ClusterMembership target0 = new ClusterMembership(ClusterEndpoint.from("tcp://yadayada@5810"), null);
		target0.setWellknownMembers("localhost:5810, 127.0.0.1:5810, 10.10.10.10:5810, watwat:1234");
		List<TransportEndpoint> list0 = target0.getWellknownMembers();
		assertEquals("" + list0, 2, list0.size());
		assertTrue(list0.contains(TransportEndpoint.from("tcp://watwat:1234")));
		assertTrue(list0.contains(TransportEndpoint.from("tcp://10.10.10.10:5810")));

		ClusterMembership target1 = new ClusterMembership(ClusterEndpoint.from("tcp://yadayada@5810"), null);
		target1.setWellknownMembers(ipAddress + ":5810, localhost:5810, 127.0.0.1:5810, some:crap, watwat:1234");
		List<TransportEndpoint> list1 = target1.getWellknownMembers();
		assertEquals("" + list1, 1, list1.size());
		assertTrue(list1.contains(TransportEndpoint.from("tcp://watwat:1234")));

		ClusterMembership target2 = new ClusterMembership(ClusterEndpoint.from("tcp://yadayada@5810"), null);
		target2.setWellknownMembers(ipAddress + ":6810, 127.0.0.1:7810, 10.10.10.10:5810");
		List<TransportEndpoint> list2 = target2.getWellknownMembers();
		assertEquals("" + list2, 3, list2.size());
		assertTrue(list2.contains(TransportEndpoint.from("tcp://6810")));
		assertTrue(list2.contains(TransportEndpoint.from("tcp://7810")));
		assertTrue(list2.contains(TransportEndpoint.from("tcp://10.10.10.10:5810")));
	}
}
