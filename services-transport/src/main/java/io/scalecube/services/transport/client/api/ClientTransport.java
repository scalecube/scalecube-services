package io.scalecube.services.transport.client.api;

import io.scalecube.transport.Address;

public interface ClientTransport {

  public ClientChannel create(Address address);

}
