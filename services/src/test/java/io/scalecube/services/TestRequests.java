package io.scalecube.services;

import io.scalecube.streams.StreamMessage;

public interface TestRequests {
    String SERVICE_NAME = "io.scalecube.services.GreetingService";

    StreamMessage GREETING_VOID_REQ = Messages.builder()
            .request(SERVICE_NAME, "greetingVoid")
            .data(new GreetingRequest("joe"))
            .build();

    StreamMessage GREETING_NO_PARAMS_REQUEST = Messages.builder()
            .request(SERVICE_NAME, "greetingNoParams").build();


}
