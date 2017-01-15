package io.scalecube.cluster.membership;

import static org.junit.Assert.assertTrue;

import io.scalecube.testlib.BaseTest;
import io.scalecube.transport.Address;
import io.scalecube.transport.ThreadFactory;

import org.junit.Test;

public class MembersipProtocolTest extends BaseTest{

  @Test
  public void test_ipv6_thread_factory_addressing(){
    
    ThreadFactory factory = new ThreadFactory();
    
    Address address = Address.create("/fe80:0:0:0:8cf6:f5c8:c946:2c30%eno1", 4001);
    String nameFormat = "sc-membership-" + address.toString();
    factory.newSingleScheduledExecutorService(nameFormat);

    assertTrue(true); // if we reached here no IllegalFormatConversionException is thrown.
  }
}
