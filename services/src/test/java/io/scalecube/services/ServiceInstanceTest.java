package io.scalecube.services;

import static org.hamcrest.CoreMatchers.hasItem;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import io.scalecube.services.ServicesConfig.Builder.ServiceConfig;
import io.scalecube.testlib.BaseTest;
import io.scalecube.transport.Address;
import io.scalecube.transport.Message;
import io.scalecube.transport.Transport;

import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;

public class ServiceInstanceTest extends BaseTest {

  @Test
  public void test_localService_instance() {

    Address address = Address.create("localhost", 4000);
    ServiceConfig conf = new ServiceConfig(new GreetingServiceImpl());
    LocalServiceInstance instance = new LocalServiceInstance(conf, address, "a", "b", new HashMap<>());
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
    ServiceReference reference =
        new ServiceReference("a", "b", Collections.singleton("sayHello"), Address.create("localhost", 4000));

    ServiceCommunicator sender = new ServiceTransport(Transport.bindAwait());

    RemoteServiceInstance instance =
        new RemoteServiceInstance(sender, reference, new HashMap<>());

    assertEquals(instance.toString(),
        "RemoteServiceInstance [serviceName=b, address=localhost:4000, memberId=a, methods=[sayHello], tags={}]");
    
    assertTrue(instance.tags().isEmpty());
    assertEquals(instance.memberId(), "a");
    assertEquals(instance.address(), Address.create("localhost", 4000));
    assertTrue(!instance.isLocal());
    assertEquals(instance.serviceName(), "b");
    assertEquals(1, instance.methods().size());
    assertThat(instance.methods(), hasItem("sayHello"));

    try {
      instance.dispatch(Message.builder()
          .header(ServiceHeaders.METHOD, null)
          .header(ServiceHeaders.SERVICE_REQUEST, "s")
          .correlationId("1")
          .build());
    } catch (Exception ex) {
      assertEquals(ex.toString(), "java.lang.IllegalArgumentException: Method name can't be null");
    }

    try {
      instance.invoke(Message.builder()
          .header(ServiceHeaders.METHOD, "unkonwn")
          .header(ServiceHeaders.SERVICE_REQUEST, "s")
          .correlationId("1")
          .build());
    } catch (Exception ex) {
      assertEquals(ex.toString(), "java.util.NoSuchElementException: No value present");
    }

    try {
      instance.dispatch(Message.builder()
          .header(ServiceHeaders.METHOD, "m")
          .header(ServiceHeaders.SERVICE_REQUEST, null)
          .correlationId("1")
          .build());
    } catch (Exception ex) {
      assertEquals(ex.toString(), "java.lang.IllegalArgumentException: Service request can't be null");
    }

    try {
      instance.invoke(Message.builder()
          .header(ServiceHeaders.METHOD, null)
          .header(ServiceHeaders.SERVICE_REQUEST, "s")
          .correlationId("1")
          .build());
    } catch (Exception ex) {
      assertEquals(ex.toString(), "java.lang.IllegalArgumentException: Method name can't be null");
    }

    try {
      instance.invoke(null);
    } catch (Exception ex) {
      assertEquals(ex.toString(), "java.lang.IllegalArgumentException: Service request can't be null");
    }

    member.shutdown();

  }


}
