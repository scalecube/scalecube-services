package io.scalecube.services;

import static org.junit.Assert.assertTrue;

import io.scalecube.services.Microservices.Builder;
import io.scalecube.services.registry.api.ServicesConfig;
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

}
