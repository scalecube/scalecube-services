package io.scalecube.services.transport.api;

import io.scalecube.services.api.ServiceMessage;

import org.reactivestreams.Publisher;

public interface ServiceMethodDispatcher<REQ> {

  Publisher<ServiceMessage> invoke(REQ request);

  Class requestType();

  Class returnType();
}
