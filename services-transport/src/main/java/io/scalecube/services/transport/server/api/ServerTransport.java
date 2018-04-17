package io.scalecube.services.transport.server.api;

import io.scalecube.services.Services;
import io.scalecube.transport.Address;

public interface ServerTransport {

  ServerTransport services(Services services);

  Address bindAwait(int port);

  void stop();
  
}

