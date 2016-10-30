package io.scalecube.services.routing;

import io.scalecube.services.ServiceDefinition;
import io.scalecube.services.ServiceInstance;

import java.util.Optional;

public interface RouteSelectionStrategy {

  Optional<ServiceInstance> route(ServiceDefinition serviceDefinition);

}
