package io.scalecube.services;

import io.scalecube.testlib.BaseTest;
import io.scalecube.transport.Address;

import org.junit.Test;

public class ServiceEndpointTest extends BaseTest {

  @Test
  public void test_ServiceReference() {
    Address addr = Address.create("localhost", 4000);
    //ServiceEndpoint reference = new ServiceEndpoint("a", "b", null, addr);
    //assertTrue(reference.address().equals(addr));
    //assertTrue(reference.id().equals("a"));
    //assertTrue(reference.serviceName().equals("b"));

    //ServiceEndpoint aref = new ServiceEndpoint("a", "b", null, addr);
    //ServiceEndpoint bref = new ServiceEndpoint("a", "b", null, addr);

    //assertEquals(aref, aref);
    //assertEquals(aref, bref);
    //assertNotEquals(aref, null);

    //assertEquals(reference.toString(), "ServiceEndpoint [memberId=a, qualifier=b, address=localhost:4000]");
  }

}
