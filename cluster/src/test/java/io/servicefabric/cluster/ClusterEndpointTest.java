package io.servicefabric.cluster;

import static org.junit.Assert.assertEquals;

import io.servicefabric.transport.TransportEndpoint;
import io.servicefabric.transport.utils.IpAddressResolver;

import org.junit.Test;

import java.net.URISyntaxException;
import java.net.UnknownHostException;

public class ClusterEndpointTest {

  @Test
  public void testUri() throws URISyntaxException, UnknownHostException {
    ClusterEndpoint ce0 = ClusterEndpoint.from("tcp://0A1B2C3@5810");
    assertEquals("0A1B2C3", ce0.endpointId());
    assertEquals(TransportEndpoint.from("tcp://5810"), ce0.endpoint());
    assertEquals("tcp://0A1B2C3@" + IpAddressResolver.resolveIpAddress().getHostAddress() + ":5810", ce0.toString());

    ClusterEndpoint ce1 = ClusterEndpoint.from("tcp://0A1B2C3@hostname:5810");
    assertEquals("0A1B2C3", ce1.endpointId());
    assertEquals(TransportEndpoint.from("tcp://hostname:5810"), ce1.endpoint());
    assertEquals("tcp://0A1B2C3@hostname:5810", ce1.toString());

    ClusterEndpoint ce2 = ClusterEndpoint.from("tcp://0A1B2C3@host:1");
    assertEquals("0A1B2C3", ce2.endpointId());
    assertEquals(TransportEndpoint.from("tcp://0A1B2C3@host:1"), ce2.endpoint());
    assertEquals("tcp://0A1B2C3@host:1", ce2.toString());

    ClusterEndpoint ce3 = ClusterEndpoint.from("tcp://0A1B2C3@10.10.10.10:5810");
    assertEquals("0A1B2C3", ce3.endpointId());
    assertEquals(TransportEndpoint.from("tcp://10.10.10.10:5810"), ce3.endpoint());
    assertEquals("tcp://0A1B2C3@10.10.10.10:5810", ce3.toString());

    ClusterEndpoint ce4 = ClusterEndpoint.from("tcp://0A1B2C3@localhost:5810");
    assertEquals("0A1B2C3", ce4.endpointId());
    assertEquals(TransportEndpoint.from("tcp://5810"), ce4.endpoint());
    assertEquals("tcp://0A1B2C3@" + IpAddressResolver.resolveIpAddress().getHostAddress() + ":5810", ce4.toString());
  }

  @Test
  public void testEq() {
    TransportEndpoint te = ClusterEndpoint.from("tcp://0A1B2C3@5810").endpoint();
    assertEquals(TransportEndpoint.from("tcp://5810"), te);
    assertEquals(TransportEndpoint.from("tcp://localhost:5810"), te);
    assertEquals(TransportEndpoint.from("tcp://127.0.0.1:5810"), te);

    assertEquals(ClusterEndpoint.from("tcp://0A1B2C3@localhost:5810"), ClusterEndpoint.from("tcp://0A1B2C3@5810"));
    assertEquals(ClusterEndpoint.from("tcp://0A1B2C3@127.0.0.1:5810"), ClusterEndpoint.from("tcp://0A1B2C3@5810"));
  }
}
