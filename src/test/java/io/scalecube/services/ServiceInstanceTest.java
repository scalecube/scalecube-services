package io.scalecube.services;

import static org.hamcrest.CoreMatchers.hasItem;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import io.scalecube.streams.ClientStreamProcessors;
import io.scalecube.streams.StreamProcessors;
import io.scalecube.testlib.BaseTest;
import io.scalecube.transport.Address;

import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;

public class ServiceInstanceTest extends BaseTest {

  @Test
  public void test_remote_service_instance() {

    Microservices member = Microservices.builder().build();
    ServiceReference reference =
        new ServiceReference("a", "b", Collections.singleton("sayHello"), Address.create("localhost", 4000));

    ClientStreamProcessors sender = StreamProcessors.newClient();

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
      instance.invoke(Messages.builder().request("", "s").build());
    } catch (Exception ex) {
      assertEquals(ex.toString(), "java.lang.IllegalArgumentException: Method name can't be null");
    }

    try {
      instance.invoke(Messages.builder().request("unknown", "s").build());
    } catch (Exception ex) {
      assertEquals(ex.toString(), "java.util.NoSuchElementException: No value present");
    }

    try {
      instance.invoke(Messages.builder().request("unknown", null).build());
    } catch (Exception ex) {
      assertEquals(ex.toString(), "java.lang.IllegalArgumentException: Service request can't be null");
    }

    try {
      instance.invoke(Messages.builder().request("s", null).build());
    } catch (Exception ex) {
      assertEquals(ex.toString(), "java.lang.IllegalArgumentException: Method name can't be null");
    }

    member.shutdown();

  }


}
