package io.scalecube.services;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import io.scalecube.services.ServicesConfig.Builder.ServiceConfig;
import io.scalecube.services.annotations.AnnotationServiceProcessor;
import io.scalecube.services.annotations.ServiceProcessor;
import io.scalecube.transport.Address;
import io.scalecube.transport.Message;

import org.junit.Test;

import java.util.HashMap;

public class ServiceInstanceTest {

  @Test
  public void test_localService_instance() {

    Address address = Address.create("localhost", 4000);
    ServiceConfig conf = new ServiceConfig(new GreetingServiceImpl());
    LocalServiceInstance instance = new LocalServiceInstance(conf, address,
        "a", "b", new HashMap<>());
    assertEquals(instance.toString(), "LocalServiceInstance [serviceObject=GreetingServiceImpl [], memberId=a]");
    assertTrue(instance.tags().isEmpty());
    assertEquals(instance.memberId(), "a");
    assertEquals(instance.serviceName(), "b");

    try {
      new LocalServiceInstance(null, address, "a", "b", new HashMap<>());
    } catch (Exception ex) {
      assertEquals(ex.toString(), "java.lang.IllegalArgumentException: serviceConfig can't be null");
    }

    try {
      new LocalServiceInstance(conf, null, "a", "b", new HashMap<>());
    } catch (Exception ex) {
      assertEquals(ex.toString(), "java.lang.IllegalArgumentException: address can't be null");
    }

    try {
      new LocalServiceInstance(conf, address, null, "b", new HashMap<>());
    } catch (Exception ex) {
      assertEquals(ex.toString(), "java.lang.IllegalArgumentException: memberId can't be null");
    }

    try {
      new LocalServiceInstance(conf, address, "a", null, new HashMap<>());
    } catch (Exception ex) {
      assertEquals(ex.toString(), "java.lang.IllegalArgumentException: serviceName can't be null");
    }

    try {
      new LocalServiceInstance(conf, address, "a", "b", null);
    } catch (Exception ex) {
      assertEquals(ex.toString(), "java.lang.IllegalArgumentException: methods can't be null");
    }

    try {
      new LocalServiceInstance(conf, address,
          "a", "b", new HashMap<>()).invoke(null);
    } catch (Exception ex) {
      assertEquals(ex.toString(), "java.lang.IllegalArgumentException: message can't be null");
    }
  }

  @Test
  public void test_remote_service_instance() {

    Microservices member = Microservices.builder().build();
    ServiceReference reference = new ServiceReference("a", "b", Address.create("localhost", 4000));
    ServicesConfig config = ServicesConfig.empty();

    ServiceProcessor processor = new AnnotationServiceProcessor();
    Sender sender = new ClusterSender(member.cluster());
    ServiceRegistry registry = new ServiceRegistryImpl(member.cluster(), sender, config, processor);

    RemoteServiceInstance instance =
        new RemoteServiceInstance(registry, sender, reference, new HashMap<>());

    assertEquals(instance.toString(), "RemoteServiceInstance [address=localhost:4000, memberId=a]");
    assertTrue(instance.tags().isEmpty());
    assertEquals(instance.memberId(), "a");
    assertEquals(instance.address(), Address.create("localhost", 4000));
    assertTrue(!instance.isLocal());
    assertEquals(instance.serviceName(), "b");

    try {
      instance.dispatch(Message.builder()
          .header(ServiceHeaders.METHOD, null)
          .header(ServiceHeaders.SERVICE, "s")
          .build());
    } catch (Exception ex) {
      assertEquals(ex.toString(), "java.lang.IllegalArgumentException: Method name can't be null");
    }


    try {
      instance.dispatch(Message.builder()
          .header(ServiceHeaders.METHOD, "m")
          .header(ServiceHeaders.SERVICE, null)
          .build());
    } catch (Exception ex) {
      assertEquals(ex.toString(), "java.lang.IllegalArgumentException: Service request can't be null");
    }

    try {
      instance.invoke(Message.builder()
          .header(ServiceHeaders.METHOD, null)
          .build());
    } catch (Exception ex) {
      assertEquals(ex.toString(), "java.lang.IllegalArgumentException: Method name can't be null");
    }

    try {
      instance.invoke(null);
    } catch (Exception ex) {
      assertEquals(ex.toString(), "java.lang.IllegalArgumentException: Service request can't be null");
    }

    member.cluster().shutdown();

  }


}
