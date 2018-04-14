package io.scalecube.services;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

import io.scalecube.testlib.BaseTest;
import io.scalecube.transport.Address;

import org.junit.Test;

public class ServiceReferenceTest extends BaseTest {

  @Test
  public void test_ServiceReference() {
    Address addr = Address.create("localhost", 4000);
    ServiceReference reference = new ServiceReference("a", "b",null, addr);
    assertTrue(reference.address().equals(addr));
    assertTrue(reference.memberId().equals("a"));
    assertTrue(reference.serviceName().equals("b"));

    ServiceReference aref = new ServiceReference("a", "b",null, addr);
    ServiceReference bref = new ServiceReference("a", "b",null, addr);

    assertEquals(aref, aref);
    assertEquals(aref, bref);
    assertNotEquals(aref, null);

    assertEquals(reference.toString(), "ServiceReference [memberId=a, qualifier=b, address=localhost:4000]");
  }

}
