package io.scalecube.transport;

import static org.junit.Assert.assertEquals;

import io.scalecube.testlib.BaseTest;

import org.junit.Test;

public class ThreadFactoryTest extends BaseTest {

  @Test
  public void test_ipv6_thread_factory_addressing() {

    // verify issue: https://github.com/scalecube/scalecube/issues/171
    ThreadFactory factory = new ThreadFactory();

    Address address = Address.create("/fe80:0:0:0:8cf6:f5c8:c946:2c30%eno1", 4001);
    String nameFormat = "sc-membership-" + address.toString();
    factory.newSingleScheduledExecutorService(nameFormat);
    // if we reached here no IllegalFormatConversionException is thrown.
    
    try {
      factory.newSingleScheduledExecutorService(null);
    } catch (Exception ex) {
      assertEquals("name can't be null", ex.getMessage().toString());
    }
    
    
  }
}
