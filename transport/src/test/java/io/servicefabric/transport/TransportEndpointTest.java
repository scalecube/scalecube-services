package io.servicefabric.transport;

import static org.junit.Assert.assertEquals;

import io.servicefabric.transport.TransportAddress;
import io.servicefabric.transport.TransportEndpoint;
import io.servicefabric.transport.utils.IpAddressResolver;

import org.junit.Test;

import java.net.URISyntaxException;
import java.net.UnknownHostException;

public class TransportEndpointTest {

  @Test
  public void testUri() throws URISyntaxException, UnknownHostException {
    TransportEndpoint ce0 = TransportEndpoint.from("tcp://0A1B2C3@5810");
    assertEquals("0A1B2C3", ce0.id());
    assertEquals(TransportAddress.from("tcp://5810"), ce0.address());
    assertEquals("tcp://0A1B2C3@" + IpAddressResolver.resolveIpAddress().getHostAddress() + ":5810", ce0.toString());

    TransportEndpoint ce1 = TransportEndpoint.from("tcp://0A1B2C3@hostname:5810");
    assertEquals("0A1B2C3", ce1.id());
    assertEquals(TransportAddress.from("tcp://hostname:5810"), ce1.address());
    assertEquals("tcp://0A1B2C3@hostname:5810", ce1.toString());

    TransportEndpoint ce2 = TransportEndpoint.from("tcp://0A1B2C3@host:1");
    assertEquals("0A1B2C3", ce2.id());
    assertEquals(TransportAddress.from("tcp://0A1B2C3@host:1"), ce2.address());
    assertEquals("tcp://0A1B2C3@host:1", ce2.toString());

    TransportEndpoint ce3 = TransportEndpoint.from("tcp://0A1B2C3@10.10.10.10:5810");
    assertEquals("0A1B2C3", ce3.id());
    assertEquals(TransportAddress.from("tcp://10.10.10.10:5810"), ce3.address());
    assertEquals("tcp://0A1B2C3@10.10.10.10:5810", ce3.toString());

    TransportEndpoint ce4 = TransportEndpoint.from("tcp://0A1B2C3@localhost:5810");
    assertEquals("0A1B2C3", ce4.id());
    assertEquals(TransportAddress.from("tcp://5810"), ce4.address());
    assertEquals("tcp://0A1B2C3@" + IpAddressResolver.resolveIpAddress().getHostAddress() + ":5810", ce4.toString());
  }

  @Test
  public void testEq() {
    TransportAddress te = TransportEndpoint.from("tcp://0A1B2C3@5810").address();
    assertEquals(TransportAddress.from("tcp://5810"), te);
    assertEquals(TransportAddress.from("tcp://localhost:5810"), te);
    assertEquals(TransportAddress.from("tcp://127.0.0.1:5810"), te);

    assertEquals(TransportEndpoint.from("tcp://0A1B2C3@localhost:5810"), TransportEndpoint.from("tcp://0A1B2C3@5810"));
    assertEquals(TransportEndpoint.from("tcp://0A1B2C3@127.0.0.1:5810"), TransportEndpoint.from("tcp://0A1B2C3@5810"));
  }
}
