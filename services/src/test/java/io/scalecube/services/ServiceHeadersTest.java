package io.scalecube.services;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import io.scalecube.testlib.BaseTest;
import io.scalecube.transport.Message;

import org.junit.Test;

import java.lang.reflect.Constructor;

public class ServiceHeadersTest extends BaseTest {

  @Test
  public void test_ServiceHeaders() {
    Message request = Message.builder().header(ServiceHeaders.METHOD, "m")
        .header(ServiceHeaders.SERVICE_REQUEST, "req")
        .header(ServiceHeaders.SERVICE_RESPONSE, "res").build();

    assertEquals(ServiceHeaders.serviceMethod(request), "m");
    assertEquals(ServiceHeaders.serviceRequest(request), "req");
    assertEquals(ServiceHeaders.serviceResponse(request), "res");

    try {
      Constructor<ServiceHeaders> constructor= (Constructor<ServiceHeaders>) ServiceHeaders.class.getDeclaredConstructors()[0];
      constructor.setAccessible(true);
      ServiceHeaders foo = constructor.newInstance();
      assertTrue(foo!=null);

    } catch (Exception e) {
    }

  }

}
