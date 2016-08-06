package io.scalecube.transport;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

public class AddressTest {

  @Test
  public void testParseHostPort() throws Exception {
    Address address1 = Address.from("localhost:5810");
    assertEquals(5810, address1.port());
    assertEquals(Address.getLocalIpAddress(), address1.host());

    Address address2 = Address.from("127.0.0.1:5810");
    assertEquals(5810, address1.port());
    assertEquals(Address.getLocalIpAddress(), address2.host());

    assertEquals(address1, address2);
    assertEquals(address1.host(), address2.host());
    assertEquals(address1.port(), address2.port());
    assertEquals(address1.socketAddress(), address2.socketAddress());
  }

  @Test
  public void testParseUnknownHostPort() throws Exception {
    Address address = Address.from("host:1111");
    assertEquals(1111, address.port());
    assertEquals("host", address.host());
    assertTrue(address.socketAddress().isUnresolved());
  }

}
