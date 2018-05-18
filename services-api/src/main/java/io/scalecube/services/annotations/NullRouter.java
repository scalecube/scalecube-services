package io.scalecube.services.annotations;

import io.scalecube.services.ServiceReference;
import io.scalecube.services.api.ServiceMessage;
import io.scalecube.services.registry.api.ServiceRegistry;
import io.scalecube.services.routing.Router;

import java.util.List;

@Null
class NullRouter implements Router {

  @Override
  public List<ServiceReference> routes(ServiceRegistry serviceRegistry, ServiceMessage request) {
    return null;
  }

}
