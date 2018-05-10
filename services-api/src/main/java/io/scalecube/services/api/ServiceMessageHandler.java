package io.scalecube.services.api;

import org.reactivestreams.Publisher;

public interface ServiceMessageHandler {

  Publisher<ServiceMessage> invoke(Publisher<ServiceMessage> publisher);

}
