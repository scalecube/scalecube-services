package io.scalecube.services.transport.server.api;

import io.scalecube.transport.Address;

public interface ServerTransport {

  ServerTransport accept(ServerMessageAcceptor acceptor);

  Address bindAwait(int port);

  void stop();
  
}

