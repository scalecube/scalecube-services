package io.scalecube.services;

import io.scalecube.services.api.ServiceMessage;
import io.scalecube.services.routing.Router;

import java.util.Collection;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;

public class DummyRouter implements Router {
  private Object empty;

  public DummyRouter() {
    checkArgument(this.empty != null);
  }

  @Override
  public Optional<ServiceEndpoint> route(ServiceMessage request) {
    return null;
  }

  @Override
  public Collection<ServiceEndpoint> routes(ServiceMessage request) {
    // TODO Auto-generated method stub
    return null;
  }

}
