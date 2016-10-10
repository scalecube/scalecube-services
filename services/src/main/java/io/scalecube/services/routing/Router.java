package io.scalecube.services.routing;

import io.scalecube.services.ServiceDefinition;
import io.scalecube.services.ServiceInstance;

public interface Router {

  ServiceInstance route(ServiceDefinition serviceDefinition);
  
}
