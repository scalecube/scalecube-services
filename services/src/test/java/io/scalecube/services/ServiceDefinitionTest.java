package io.scalecube.services;

import static org.junit.Assert.assertEquals;

import io.scalecube.testlib.BaseTest;

import org.junit.Test;

public class ServiceDefinitionTest extends BaseTest {

  @Test
  public void test_service_definition() {
    ServiceDefinition def = ServiceDefinition.from("service_name");
    assertEquals(def.serviceName(), "service_name");
  }

}
