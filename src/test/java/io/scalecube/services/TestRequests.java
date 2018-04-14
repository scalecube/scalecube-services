package io.scalecube.services;

import io.scalecube.services.transport.api.ServiceMessage;

public interface TestRequests {
    String SERVICE_NAME = "io.scalecube.services.GreetingService";

    ServiceMessage GREETING_VOID_REQ = Messages.builder()
            .request(SERVICE_NAME, "greetingVoid")
            .data(new GreetingRequest("joe"))
            .build();

    ServiceMessage GREETING_NO_PARAMS_REQUEST = Messages.builder()
            .request(SERVICE_NAME, "greetingNoParams").build();


}
