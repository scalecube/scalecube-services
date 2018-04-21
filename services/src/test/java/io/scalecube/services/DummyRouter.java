package io.scalecube.services;

import io.scalecube.services.api.ServiceMessage;
import io.scalecube.services.routing.Router;

import java.util.List;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;

public class DummyRouter implements Router {
  private Object empty;

  public DummyRouter() {
    checkArgument(this.empty != null);
  }

  @Override
  public Optional<ServiceReference> route(ServiceMessage request) {
    return null;
  }

  @Override
  public List<ServiceReference> routes(ServiceMessage request) {
    // TODO Auto-generated method stub
    return null;
  }

}
