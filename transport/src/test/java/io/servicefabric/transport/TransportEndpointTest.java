package io.servicefabric.transport;

import static io.servicefabric.transport.TransportEndpoint.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.net.UnknownHostException;

import org.junit.Test;

import io.servicefabric.transport.utils.IpAddressResolver;

public class TransportEndpointTest {

	@Test
	public void testUri() throws UnknownHostException {
		TransportEndpoint endpoint0 = from("tcp://5810");
		assertEquals("tcp", endpoint0.getScheme());
		assertEquals(5810, endpoint0.getPort());
		String hostAddress = IpAddressResolver.resolveIpAddress().getHostAddress();
		assertEquals(hostAddress, endpoint0.getHostAddress());
		assertEquals("tcp://" + hostAddress + ":5810", endpoint0.toString());

		TransportEndpoint endpoint1 = from("tcp://10.10.10.10:5810");
		assertEquals("tcp", endpoint1.getScheme());
		assertEquals(5810, endpoint1.getPort());
		assertEquals("10.10.10.10", endpoint1.getHostAddress());
		assertEquals("tcp://10.10.10.10:5810", endpoint1.toString());

		TransportEndpoint endpoint2 = from("tcp://hostname:5810");
		assertEquals("tcp", endpoint2.getScheme());
		assertEquals(5810, endpoint2.getPort());
		assertEquals("hostname", endpoint2.getHostAddress());
		assertEquals("tcp://hostname:5810", endpoint2.toString());

		TransportEndpoint endpoint3 = from("local://hostname:5810");
		assertEquals("local", endpoint3.getScheme());
		assertEquals(-1, endpoint3.getPort());
		assertEquals("hostname:5810", endpoint3.getHostAddress());
		assertEquals("local://hostname:5810", endpoint3.toString());

		TransportEndpoint endpoint4 = from("local://hostname/5/8/1/0");
		assertEquals("local", endpoint4.getScheme());
		assertEquals(-1, endpoint4.getPort());
		assertEquals("hostname/5/8/1/0", endpoint4.getHostAddress());
		assertEquals("local://hostname/5/8/1/0", endpoint4.toString());
	}

	@Test
	public void testLocalTcp() throws UnknownHostException {
		TransportEndpoint endpoint0 = localTcp(4800, 30);
		String hostAddress0 = endpoint0.getHostAddress();
		assertTrue("" + hostAddress0, !hostAddress0.equals("localhost"));
		assertTrue("" + hostAddress0, !hostAddress0.equals("127.0.0.1"));
		assertEquals("tcp", endpoint0.getScheme());
		assertEquals(4830, endpoint0.getPort());

		TransportEndpoint endpoint1 = tcp("localhost:8080");
		String hostAddress1 = endpoint1.getHostAddress();
		assertTrue("" + hostAddress1, !hostAddress1.equals("localhost"));
		assertTrue("" + hostAddress1, !hostAddress1.equals("127.0.0.1"));
		assertEquals("tcp", endpoint1.getScheme());
		assertEquals(8080, endpoint1.getPort());
	}

	@Test
	public void testEq() throws UnknownHostException {
		assertEquals(localTcp(5800, 10), from("tcp://5810"));
		assertEquals(localTcp(5800, 10), from("tcp://localhost:5810"));
		assertEquals(localTcp(5800, 10), from("tcp://127.0.0.1:5810"));

		assertEquals(from("tcp://localhost:5810"), from("tcp://5810"));
		assertEquals(from("tcp://127.0.0.1:5810"), from("tcp://5810"));

		assertEquals(tcp("localhost:8080"), from("tcp://8080"));
		assertEquals(tcp("127.0.0.1:8080"), from("tcp://8080"));
	}
}
