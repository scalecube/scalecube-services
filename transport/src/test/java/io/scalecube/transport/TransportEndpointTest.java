package io.scalecube.transport;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import io.scalecube.transport.utils.IpAddressResolver;

import org.junit.Test;

import java.net.InetSocketAddress;

public class TransportEndpointTest {

  @Test
  public void testParseHostPortEndpointId() throws Exception {
    TransportEndpoint te1 = TransportEndpoint.from("localhost:5810:0A1B2C3");
    assertEquals("0A1B2C3", te1.id());
    assertEquals(IpAddressResolver.resolveIpAddress().getHostAddress() + ":5810" + ":0A1B2C3", te1.getString());

    TransportEndpoint te2 = TransportEndpoint.from("127.0.0.1:5810:0A1B2C3");
    assertEquals("0A1B2C3", te2.id());
    assertEquals(IpAddressResolver.resolveIpAddress().getHostAddress() + ":5810" + ":0A1B2C3", te2.getString());

    assertEquals(te1, te2);
    assertEquals(te1.host(), te2.host());
    assertEquals(te1.port(), te2.port());
    assertEquals(te1.socketAddress(), te2.socketAddress());
  }

  @Test
  public void testParseUnknownHostPortEndpoitId() throws Exception {
    TransportEndpoint te = TransportEndpoint.from("host:1111:0A1B2C3");
    assertEquals("0A1B2C3", te.id());
    assertEquals("host:1111:0A1B2C3", te.getString());
    assertTrue(te.socketAddress().isUnresolved());
  }

  @Test
  public void testParseSocketAddress() throws Exception {
    InetSocketAddress sa = TransportEndpoint.parseSocketAddress("host:1111");
    assertEquals("host", sa.getHostName());
    assertEquals(1111, sa.getPort());
    assertTrue(sa.isUnresolved());
  }
}
