package io.scalecube.services.transport.rsocket.experimental;

import io.scalecube.services.transport.api.experimental.ClientTransportFactory;
import io.scalecube.services.transport.api.experimental.ServerTransport;
import io.scalecube.services.transport.api.experimental.ServiceTransportProvider;

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
