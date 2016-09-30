package io.scalecube.services.tests;

import org.junit.Test;

import io.scalecube.services.ConsulServiceRegistry;

public class ConsulRegistryTests {

  @Test
  public void registerServiceTest(){
    ConsulServiceRegistry reg = new ConsulServiceRegistry(null, "localhost");
    reg.registerService("A", "com/myservice/doSomthing", "localhost", 6000);
    reg.registerService("B", "com/myservice/doSomthing", "localhost", 6000);
    reg.registerService("C", "com/myservice/doSomthing", "localhost", 6000);
    
    reg.serviceLookup("com/myservice/doSomthing");
  }
}
