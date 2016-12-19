package io.scalecube.services.routing;

import io.scalecube.services.ServiceInstance;
import io.scalecube.transport.Message;

import java.util.Optional;

public interface Router {

  Optional<ServiceInstance> route(Message request);

}
