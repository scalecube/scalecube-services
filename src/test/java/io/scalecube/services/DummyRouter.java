package io.scalecube.services;

import static com.google.common.base.Preconditions.checkArgument;

import io.scalecube.services.routing.Router;
import io.scalecube.services.transport.ServiceMessage;

import java.util.Collection;
import java.util.Optional;

public class DummyRouter implements Router {
  private Object empty;

  public DummyRouter() {
    checkArgument(this.empty != null);
  }

  @Override
  public Optional<ServiceInstance> route(ServiceMessage request) {
    return null;
  }

  @Override
  public Collection<ServiceInstance> routes(ServiceMessage request) {
    // TODO Auto-generated method stub
    return null;
  }

}
