package io.scalecube.services;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

public class ServiceDefinitionTest {

  @Test
  public void test_service_definition() {
    ServiceDefinition def = ServiceDefinition.from("service_name");
    assertEquals(def.serviceName(), "service_name");
  }

}
