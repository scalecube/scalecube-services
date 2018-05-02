package io.scalecube.services;

import io.scalecube.services.api.ServiceMessage;

import java.time.Duration;

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

  ServiceMessage GREETING_REQ = Messages.builder()
      .request(SERVICE_NAME, "greeting")
      .data("joe")
      .build();

  ServiceMessage GREETING_REQUEST_REQ = Messages.builder()
      .request(SERVICE_NAME, "greetingRequest")
      .data(new GreetingRequest("joe"))
      .build();

  ServiceMessage GREETING_REQUEST_TIMEOUT_REQ = Messages.builder()
      .request(SERVICE_NAME, "greetingRequestTimeout")
      .data(new GreetingRequest("joe", Duration.ofSeconds(3)))
      .build();

  ServiceMessage NOT_FOUND_REQ = Messages.builder()
      .request(SERVICE_NAME, "unknown")
      .data("joe").build();

  ServiceMessage GREETING_CORRUPTED_PAYLOAD_REQUEST = Messages.builder()
      .request(SERVICE_NAME, "greetingPojo").data(new Integer(-1)).build();

  ServiceMessage GREETING_UNAUTHORIZED_REQUEST = Messages.builder()
      .request(SERVICE_NAME, "greetingNotAuthorized").data(new GreetingRequest("joe")).build();

  ServiceMessage GREETING_NULL_PAYLOAD = Messages.builder()
      .request(SERVICE_NAME, "greetingPojo").build();

  ServiceMessage SERVICE_NOT_FOUND = Messages.builder()
      .request(SERVICE_NAME, "unknown_service").build();

}
