package io.servicefabric.cluster;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import io.servicefabric.transport.TransportAddress;
import io.servicefabric.transport.TransportEndpoint;
import io.servicefabric.transport.utils.IpAddressResolver;

import org.junit.Before;
import org.junit.Test;

import java.net.UnknownHostException;
import java.util.List;

public class ClusterMembershipTest {
  private String ipAddress;

  @Before
  public void setup() throws UnknownHostException {
    ipAddress = IpAddressResolver.resolveIpAddress().getHostAddress();
  }

  @Test
  public void testSetWellknownMembers() {
    ClusterMembership target0 = new ClusterMembership(TransportEndpoint.from("tcp://yadayada@5810"), null);
    target0.setSeedMembers("localhost:5810, 127.0.0.1:5810, 10.10.10.10:5810, watwat:1234");
    List<TransportAddress> list0 = target0.getSeedMembers();
    assertEquals("" + list0, 2, list0.size());
    assertTrue(list0.contains(TransportAddress.from("tcp://watwat:1234")));
    assertTrue(list0.contains(TransportAddress.from("tcp://10.10.10.10:5810")));

    ClusterMembership target1 = new ClusterMembership(TransportEndpoint.from("tcp://yadayada@5810"), null);
    target1.setSeedMembers(ipAddress + ":5810, localhost:5810, 127.0.0.1:5810, some:crap, watwat:1234");
    List<TransportAddress> list1 = target1.getSeedMembers();
    assertEquals("" + list1, 1, list1.size());
    assertTrue(list1.contains(TransportAddress.from("tcp://watwat:1234")));

    ClusterMembership target2 = new ClusterMembership(TransportEndpoint.from("tcp://yadayada@5810"), null);
    target2.setSeedMembers(ipAddress + ":6810, 127.0.0.1:7810, 10.10.10.10:5810");
    List<TransportAddress> list2 = target2.getSeedMembers();
    assertEquals("" + list2, 3, list2.size());
    assertTrue(list2.contains(TransportAddress.from("tcp://6810")));
    assertTrue(list2.contains(TransportAddress.from("tcp://7810")));
    assertTrue(list2.contains(TransportAddress.from("tcp://10.10.10.10:5810")));
  }
}
