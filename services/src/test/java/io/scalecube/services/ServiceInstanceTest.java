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

    ServiceConfig conf = new ServiceConfig(new GreetingServiceImpl());
    LocalServiceInstance instance = new LocalServiceInstance(conf,
        "a", "b", new HashMap<>());
    assertEquals(instance.toString(), "LocalServiceInstance [serviceObject=GreetingServiceImpl [], memberId=a]");
    assertTrue(instance.tags().isEmpty());
    assertEquals(instance.memberId(), "a");
    assertEquals(instance.serviceName(), "b");

    try {
      new LocalServiceInstance(conf, null, "b", new HashMap<>());
    } catch (Exception ex) {
      assertEquals(ex.toString(), "java.lang.IllegalArgumentException");
    }

    try {
      new LocalServiceInstance(conf, "a", null, new HashMap<>());
    } catch (Exception ex) {
      assertEquals(ex.toString(), "java.lang.IllegalArgumentException");
    }

    try {
      new LocalServiceInstance(conf, "a", "b", null);
    } catch (Exception ex) {
      assertEquals(ex.toString(), "java.lang.IllegalArgumentException");
    }

    try {
      new LocalServiceInstance(conf,
          "a", "b", new HashMap<>()).invoke(null);
    } catch (Exception ex) {
      assertEquals(ex.toString(), "java.lang.IllegalArgumentException");
    }
  }

  @Test
  public void test_remote_service_instance() {

    Microservices member = Microservices.builder().build();
    ServiceReference reference = new ServiceReference("a", "b", Address.create("localhost", 4000));
    ServicesConfig config = ServicesConfig.empty();

    ServiceProcessor processor = new AnnotationServiceProcessor();
    ServiceRegistry registry = new ServiceRegistryImpl(member.cluster(), config, processor);
    RemoteServiceInstance instance = new RemoteServiceInstance(registry, reference, new HashMap<>());

    assertEquals(instance.toString(), "RemoteServiceInstance [address=localhost:4000, memberId=a]");
    assertTrue(instance.tags().isEmpty());
    assertEquals(instance.memberId(), "a");
    assertEquals(instance.address(), Address.create("localhost", 4000));
    assertTrue(!instance.isReachable());
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
