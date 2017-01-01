package io.scalecube.services;

import static org.junit.Assert.assertTrue;

import io.scalecube.services.ServicesConfig.Builder.ServiceConfig;
import io.scalecube.services.annotations.AnnotationServiceProcessor;
import io.scalecube.services.annotations.ServiceProcessor;
import io.scalecube.transport.Address;

import org.junit.Test;

import java.util.HashMap;

public class ServiceInstanceTest {

  @Test
  public void test_localService_instance() {

    LocalServiceInstance instance = new LocalServiceInstance(new ServiceConfig(new GreetingServiceImpl()),
        "a", "b", new HashMap<>());
    assertTrue(instance.toString().equals("LocalServiceInstance [serviceObject=GreetingServiceImpl [], memberId=a]"));
    assertTrue(instance.tags().isEmpty());
    assertTrue(instance.memberId().equals("a"));
    assertTrue(instance.serviceName().equals("b"));
  }

  @Test
  public void test_remote_service_instance() {

    Microservices member = Microservices.builder().build();
    ServiceReference reference = new ServiceReference("a", "b", Address.create("localhost", 4000));
    ServicesConfig config = ServicesConfig.empty();

    ServiceProcessor processor = new AnnotationServiceProcessor();
    ServiceRegistry registry = new ServiceRegistryImpl(member.cluster(), config, processor);
    RemoteServiceInstance instance = new RemoteServiceInstance(registry, reference, new HashMap<>());

    assertTrue(instance.toString().equals("RemoteServiceInstance [address=localhost:4000, memberId=a]"));
    assertTrue(instance.tags().isEmpty());
    assertTrue(instance.memberId().equals("a"));
    assertTrue(instance.address().equals(Address.create("localhost", 4000)));
    assertTrue(!instance.isReachable());
    assertTrue(!instance.isLocal());
    assertTrue(instance.serviceName().equals("b"));
    member.cluster().shutdown();
  }

  
}
