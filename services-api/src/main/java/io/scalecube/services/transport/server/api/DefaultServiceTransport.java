package io.scalecube.services.transport.server.api;

import io.scalecube.transport.Address;

public class DefaultServiceTransport implements ServiceTransport{

  private Object[] services;

  @Override
  public ServiceTransport services(Object... services) {
    this.services = services;
    return this;
  }

  @Override
  public Address bindAwait() {
    return Address.create("localhost", 4801);
  }

}
