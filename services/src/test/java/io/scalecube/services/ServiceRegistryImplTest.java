package io.scalecube.services;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import io.scalecube.testlib.BaseTest;

import org.junit.Test;

import java.util.concurrent.ExecutionException;

public class ServiceRegistryImplTest extends BaseTest {

  @Test
  public void test_service_registry() throws InterruptedException, ExecutionException {
    ServicesConfig services = ServicesConfig.empty();
    Microservices ms = Microservices.builder().build();
    ServiceRegistryImpl registry = new ServiceRegistryImpl(Microservices.builder().build(), services);
    assertTrue(registry.services().isEmpty());
    ms.shutdown().get();
  }

  @Test
  public void test_service_registry_errors() throws InterruptedException, ExecutionException {

    ServicesConfig services = ServicesConfig.empty();

    try {
      new ServiceRegistryImpl(null, services);
    } catch (Exception ex) {
      assertEquals(ex.toString(), "java.lang.IllegalArgumentException: microservices can't be null");
    }

    Microservices ms = Microservices.builder().build();
    try {
      new ServiceRegistryImpl(ms, null);
    } catch (Exception ex) {
      assertEquals(ex.toString(), "java.lang.IllegalArgumentException: services can't be null");
    }
    ms.shutdown().get();
  }

}
