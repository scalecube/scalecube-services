package io.scalecube.services.transport.rsocket;

import io.scalecube.services.transport.api.ClientTransportFactory;
import io.scalecube.services.transport.api.ServerTransport;
import io.scalecube.services.transport.api.ServiceTransportProvider;

public class RSocketServiceTransportProvider implements ServiceTransportProvider {

  private final RSocketScalecubeClientTransport clientTransport;
  private final RSocketScalecubeServerTransport serverTransport;

  public RSocketServiceTransportProvider(
      RSocketScalecubeClientTransport clientTransport,
      RSocketScalecubeServerTransport serverTransport) {
    this.clientTransport = clientTransport;
    this.serverTransport = serverTransport;
  }

  @Override
  public ClientTransportFactory provideClientTransportFactory() {
    return clientTransport;
  }

  @Override
  public ServerTransport provideServerTransport() {
    return serverTransport;
  }
}
