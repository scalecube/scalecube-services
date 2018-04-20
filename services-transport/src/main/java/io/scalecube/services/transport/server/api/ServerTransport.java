package io.scalecube.services.transport.server.api;

import io.scalecube.services.ServiceMessageCodec;
import io.scalecube.transport.Address;

import java.util.Collection;

public interface ServerTransport {

  Collection<? extends ServiceMessageCodec> availableServiceMessageCodec();
  
  ServerTransport accept(ServerMessageAcceptor acceptor);

  Address bindAwait(int port);

  void stop();
  
}

