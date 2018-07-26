package io.scalecube.services;

import io.scalecube.services.api.ServiceMessage;
import io.scalecube.services.sut.GreetingRequest;

import java.time.Duration;

public interface TestRequests {

  String SERVICE_NAME = "greetings";

  ServiceMessage GREETING_VOID_REQ = ServiceMessage.builder()
      .qualifier(SERVICE_NAME, "greetingVoid")
      .data(new GreetingRequest("joe"))
      .build();

  ServiceMessage GREETING_FAILING_VOID_REQ = ServiceMessage.builder()
      .qualifier(SERVICE_NAME, "failingVoid")
      .data(new GreetingRequest("joe"))
      .build();

  ServiceMessage GREETING_THROWING_VOID_REQ = ServiceMessage.builder()
      .qualifier(SERVICE_NAME, "throwingVoid")
      .data(new GreetingRequest("joe"))
      .build();

  ServiceMessage GREETING_FAIL_REQ = ServiceMessage.builder()
      .qualifier(SERVICE_NAME, "failingRequest")
      .data(new GreetingRequest("joe"))
      .build();

  ServiceMessage GREETING_ERROR_REQ = ServiceMessage.builder()
      .qualifier(SERVICE_NAME, "exceptionRequest")
      .data(new GreetingRequest("joe"))
      .build();

  ServiceMessage GREETING_NO_PARAMS_REQUEST = ServiceMessage.builder()
      .qualifier(SERVICE_NAME, "greetingNoParams").build();

  ServiceMessage GREETING_REQ = ServiceMessage.builder()
      .qualifier(SERVICE_NAME, "greeting")
      .data("joe")
      .build();

  ServiceMessage GREETING_REQUEST_REQ = ServiceMessage.builder()
      .qualifier(SERVICE_NAME, "greetingRequest")
      .data(new GreetingRequest("joe"))
      .build();

  ServiceMessage GREETING_REQUEST_REQ2 = ServiceMessage.builder()
      .qualifier(SERVICE_NAME, "greetingRequest")
      .data(new GreetingRequest("fransin"))
      .build();

  ServiceMessage GREETING_REQUEST_TIMEOUT_REQ = ServiceMessage.builder()
      .qualifier(SERVICE_NAME, "greetingRequestTimeout")
      .data(new GreetingRequest("joe", Duration.ofSeconds(3)))
      .build();

  ServiceMessage NOT_FOUND_REQ = ServiceMessage.builder()
      .qualifier(SERVICE_NAME, "unknown")
      .data("joe")
      .build();

  ServiceMessage GREETING_CORRUPTED_PAYLOAD_REQUEST = ServiceMessage.builder()
      .qualifier(SERVICE_NAME, "greetingPojo").data(-1).build();

  ServiceMessage GREETING_UNAUTHORIZED_REQUEST = ServiceMessage.builder()
      .qualifier(SERVICE_NAME, "greetingNotAuthorized")
      .data(new GreetingRequest("joe"))
      .build();

  ServiceMessage GREETING_NULL_PAYLOAD = ServiceMessage.builder()
      .qualifier(SERVICE_NAME, "greetingPojo")
      .build();

  ServiceMessage SERVICE_NOT_FOUND = ServiceMessage.builder()
      .qualifier(SERVICE_NAME, "unknown_service")
      .build();

}
