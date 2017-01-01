package io.scalecube.services;

import static org.junit.Assert.assertTrue;

import org.junit.Test;

public class ServiceDefinitionTest {

  @Test
  public void test_service_definition() {
    ServiceDefinition def = ServiceDefinition.from("service_name");
    assertTrue(def.serviceName().equals("service_name"));
  }
  
}
