package io.scalecube.utils;

import static org.junit.Assert.assertEquals;

import io.scalecube.testlib.BaseTest;
import io.scalecube.transport.Address;
import io.scalecube.transport.Addressing;

import org.junit.Test;

public class AddressTest extends BaseTest {

  @Test
  public void testParseHostPort() throws Exception {
    Address address1 = Address.from("localhost:5810");
    assertEquals(5810, address1.port());
    assertEquals(Addressing.getLocalIpAddress().getHostAddress(), address1.host());

    Address address2 = Address.from("127.0.0.1:5810");
    assertEquals(5810, address1.port());
    assertEquals(Addressing.getLocalIpAddress().getHostAddress(), address2.host());

    assertEquals(address1, address2);
  }

  @Test
  public void testParseUnknownHostPort() throws Exception {
    Address address = Address.from("host:1111");
    assertEquals(1111, address.port());
    assertEquals("host", address.host());
  }

}
