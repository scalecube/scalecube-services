package io.scalecube.services.transport.server.api;

import io.scalecube.transport.Address;

public interface ServiceTransport {

  static ServiceTransport newServer() {
    // TODO Auto-generated method stub
    return null;
  }

  Address bindAwait();
  
}

