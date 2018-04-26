package io.scalecube.services.transport.server.api;

import io.scalecube.services.api.ServiceMessage;

import org.reactivestreams.Publisher;

public interface ServerMessageAcceptor {

  Publisher<ServiceMessage> requestChannel(Publisher<ServiceMessage> payloads);

  Publisher<ServiceMessage> requestStream(ServiceMessage payload);

  Publisher<ServiceMessage> requestResponse(ServiceMessage payload);

  Publisher<ServiceMessage> fireAndForget(ServiceMessage payload);

}
