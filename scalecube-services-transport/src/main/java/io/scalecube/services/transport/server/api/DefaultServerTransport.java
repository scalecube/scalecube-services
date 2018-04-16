package io.scalecube.services.transport.server.api;

import io.scalecube.services.ServiceInstance;
import io.scalecube.transport.Address;

import java.util.Collection;

public class DefaultServerTransport implements ServerTransport{

  private Collection<ServiceInstance> services;

  @Override
  public Address bindAwait() {
    return Address.create("localhost", 4801);
  }

  @Override
  public ServerTransport services(Collection<ServiceInstance> services) {
    this.services = services;
    
    return this;
  }

}
