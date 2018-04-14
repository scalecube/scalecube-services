package io.scalecube.services.transport.server.api;

import io.scalecube.transport.Address;

public interface ServiceTransport {

  ServiceTransport services(Object... services);

  Address bindAwait();
  
}

