package io.scalecube.services.transport;

import io.scalecube.services.transport.client.api.ClientTransport;
import io.scalecube.services.transport.server.api.ServerTransport;

public interface ServiceTransport {

  ClientTransport getClientTransport();
  
  ServerTransport getServerTransport();
}
