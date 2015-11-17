package io.scalecube.transport;

import static io.scalecube.transport.TransportAddress.from;
import static io.scalecube.transport.TransportAddress.localTcp;
import static io.scalecube.transport.TransportAddress.tcp;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import io.scalecube.transport.utils.IpAddressResolver;

import org.junit.Test;

import java.net.UnknownHostException;

public class TransportAddressTest {

  @Test
  public void testUri() throws UnknownHostException {
    TransportAddress endpoint0 = from("tcp://5810");
    assertEquals("tcp", endpoint0.scheme());
    assertEquals(5810, endpoint0.port());
    String hostAddress = IpAddressResolver.resolveIpAddress().getHostAddress();
    assertEquals(hostAddress, endpoint0.hostAddress());
    assertEquals("tcp://" + hostAddress + ":5810", endpoint0.toString());

    TransportAddress endpoint1 = from("tcp://10.10.10.10:5810");
    assertEquals("tcp", endpoint1.scheme());
    assertEquals(5810, endpoint1.port());
    assertEquals("10.10.10.10", endpoint1.hostAddress());
    assertEquals("tcp://10.10.10.10:5810", endpoint1.toString());

    TransportAddress endpoint2 = from("tcp://hostname:5810");
    assertEquals("tcp", endpoint2.scheme());
    assertEquals(5810, endpoint2.port());
    assertEquals("hostname", endpoint2.hostAddress());
    assertEquals("tcp://hostname:5810", endpoint2.toString());
  }

  @Test
  public void testLocalTcp() throws UnknownHostException {
    TransportAddress endpoint0 = localTcp(4830);
    String hostAddress0 = endpoint0.hostAddress();
    assertTrue("" + hostAddress0, !hostAddress0.equals("localhost"));
    assertTrue("" + hostAddress0, !hostAddress0.equals("127.0.0.1"));
    assertEquals("tcp", endpoint0.scheme());
    assertEquals(4830, endpoint0.port());

    TransportAddress endpoint1 = tcp("localhost:8080");
    String hostAddress1 = endpoint1.hostAddress();
    assertTrue("" + hostAddress1, !hostAddress1.equals("localhost"));
    assertTrue("" + hostAddress1, !hostAddress1.equals("127.0.0.1"));
    assertEquals("tcp", endpoint1.scheme());
    assertEquals(8080, endpoint1.port());
  }

  @Test
  public void testEq() throws UnknownHostException {
    assertEquals(localTcp(5810), from("tcp://5810"));
    assertEquals(localTcp(5810), from("tcp://localhost:5810"));
    assertEquals(localTcp(5810), from("tcp://127.0.0.1:5810"));

    assertEquals(from("tcp://localhost:5810"), from("tcp://5810"));
    assertEquals(from("tcp://127.0.0.1:5810"), from("tcp://5810"));

    assertEquals(tcp("localhost:8080"), from("tcp://8080"));
    assertEquals(tcp("127.0.0.1:8080"), from("tcp://8080"));
  }
}
