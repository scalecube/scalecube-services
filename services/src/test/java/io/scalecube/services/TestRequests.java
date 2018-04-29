package io.scalecube.services;

import io.scalecube.services.api.ServiceMessage;

public interface TestRequests {

  String SERVICE_NAME = "io.scalecube.services.GreetingService";

  ServiceMessage GREETING_VOID_REQ = Messages.builder()
      .request(SERVICE_NAME, "greetingVoid")
      .data(new GreetingRequest("joe"))
      .build();

  ServiceMessage GREETING_FAIL_REQ = Messages.builder()
      .request(SERVICE_NAME, "failingVoid")
      .data(new GreetingRequest("joe"))
      .build();

  ServiceMessage GREETING_ERROR_REQ = Messages.builder()
      .request(SERVICE_NAME, "exceptionVoid")
      .data(new GreetingRequest("joe"))
      .build();

  ServiceMessage GREETING_NO_PARAMS_REQUEST = Messages.builder()
      .request(SERVICE_NAME, "greetingNoParams").build();

}
