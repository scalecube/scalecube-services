package io.scalecube.services.routing;

import java.util.Optional;

import io.scalecube.services.ServiceDefinition;
import io.scalecube.services.ServiceInstance;

public interface Router {

  Optional<ServiceInstance> route(ServiceDefinition serviceDefinition);

}
