package io.scalecube.services;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import io.scalecube.services.Microservices.Builder;
import io.scalecube.testlib.BaseTest;

import org.junit.Test;

public class MicroservicesTest extends BaseTest {

  @Test
  public void test_microservices_config() {
    Builder builder = new Builder();
    ServicesConfig servicesConfig = ServicesConfig.builder(builder).create();
    Microservices micro = Microservices.builder().services(servicesConfig).build();
    assertTrue(servicesConfig.services().isEmpty());
    assertTrue(micro.services().isEmpty());
    micro.shutdown();
  }

  @Test
  public void test_microservices_unregister() {
    GreetingServiceImpl greeting = new GreetingServiceImpl();
    Microservices micro = Microservices.builder().services(greeting).build();
    assertEquals(micro.services().size(), 1);
    micro.unregisterService(greeting);
    assertEquals(micro.services().size(), 0);

    try {
      micro.unregisterService(null);
    } catch (Exception ex) {
      assertEquals("Service object can't be null.", ex.getMessage().toString());
    }
    micro.shutdown();
  }

}
