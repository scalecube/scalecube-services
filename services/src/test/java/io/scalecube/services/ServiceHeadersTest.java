package io.scalecube.services;

import static org.junit.Assert.assertTrue;

import io.scalecube.transport.Message;

import org.junit.Test;

public class ServiceHeadersTest {

  @Test
  public void test_ServiceHeaders() {
    Message request = Message.builder().header(ServiceHeaders.METHOD, "m")
        .header(ServiceHeaders.SERVICE, "s")
        .header(ServiceHeaders.SERVICE_REQUEST, "req")
        .header(ServiceHeaders.SERVICE_RESPONSE, "res").build();

    assertTrue(ServiceHeaders.serviceMethod(request).equals("m"));
    assertTrue(ServiceHeaders.service(request).equals("s"));
    assertTrue(ServiceHeaders.serviceRequest(request).equals("req"));
    assertTrue(ServiceHeaders.serviceResponse(request).equals("res"));
  }
  
}
