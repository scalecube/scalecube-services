package io.scalecube.services.transport.client.api;

import io.scalecube.services.ServiceMessageCodec;
import io.scalecube.transport.Address;

public interface ClientTransport {

  ClientChannel create(Address address);

  ServiceMessageCodec getMessageCodec();

}
