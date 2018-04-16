package io.scalecube.services.transport;

import io.scalecube.services.transport.ServiceTransport;
import io.scalecube.services.transport.client.api.ClientTransport;
import io.scalecube.services.transport.server.api.DefatultClientTransport;
import io.scalecube.services.transport.server.api.DefaultServerTransport;
import io.scalecube.services.transport.server.api.ServerTransport;

public class DefaultServiceTransport implements ServiceTransport{

  @Override
  public ClientTransport getClientTransport() {
    return new DefatultClientTransport();
  }

  @Override
  public ServerTransport getServerTransport() {
    return new DefaultServerTransport();
  }
  
}
