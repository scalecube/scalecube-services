package io.scalecube.services.routing;

import io.scalecube.services.ServiceDefinition;
import io.scalecube.services.ServiceInstance;

import java.util.Optional;

public interface Router {

  Optional<ServiceInstance> route(ServiceDefinition serviceDefinition, Object data);

}
