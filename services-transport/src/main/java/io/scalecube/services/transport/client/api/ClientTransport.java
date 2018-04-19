package io.scalecube.services.transport.client.api;

import io.scalecube.transport.Address;

public interface ClientTransport {

  ClientChannel create(Address address);

}
