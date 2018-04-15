package io.scalecube.services.transport.server.api;

import io.scalecube.services.ServiceInstance;
import io.scalecube.transport.Address;

import java.util.Collection;

public class DefaultServiceTransport implements ServiceTransport{

  private Collection<ServiceInstance> services;

  @Override
  public Address bindAwait() {
    return Address.create("localhost", 4801);
  }

  @Override
  public ServiceTransport services(Collection<ServiceInstance> services) {
    this.services = services;
    
    return this;
  }

}
