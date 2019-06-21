package io.scalecube.services.transport.api.experimental;

public interface ServiceTransportProvider {

  ClientTransportFactory provideClientTransportFactory();

  ServerTransport provideServerTransport();

}
