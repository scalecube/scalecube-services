package io.scalecube.services.routings.sut;

import static com.google.common.base.Preconditions.checkArgument;

import io.scalecube.services.ServiceReference;
import io.scalecube.services.api.ServiceMessage;
import io.scalecube.services.registry.api.ServiceRegistry;
import io.scalecube.services.routing.Router;

import java.util.Optional;

public class DummyRouter implements Router {
  private Object empty;

  public DummyRouter() {
    checkArgument(this.empty != null);
  }

  @Override
  public Optional<ServiceReference> route(ServiceRegistry serviceRegistry, ServiceMessage request) {
    return Optional.empty();
  }

}
