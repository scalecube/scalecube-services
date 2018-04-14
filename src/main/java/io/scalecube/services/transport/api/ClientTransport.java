package io.scalecube.services.transport.api;

import io.scalecube.transport.Address;

public interface ClientTransport {

  public ClientChannel create(Address address);

  public static ClientTransport newClient() {
    // TODO Auto-generated method stub
    return null;
  }

}
