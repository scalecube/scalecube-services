package io.scalecube.services.routings.sut;

import io.scalecube.services.ServiceReference;
import io.scalecube.services.api.ServiceMessage;
import io.scalecube.services.registry.api.ServiceRegistry;
import io.scalecube.services.routing.Router;

import java.util.Optional;

public class DummyRouter implements Router {
  private Object empty;

  public DummyRouter() {
    if (this.empty == null) {
      throw new IllegalArgumentException();
    }
  }

  @Override
  public Optional<ServiceReference> route(ServiceRegistry serviceRegistry, ServiceMessage request) {
    return Optional.empty();
  }

}
