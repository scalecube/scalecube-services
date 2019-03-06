package io.scalecube.services.gateway;

import io.scalecube.services.Microservices.ServiceTransportBootstrap;
import io.scalecube.services.transport.rsocket.RSocketServiceTransport;
import io.scalecube.services.transport.rsocket.RSocketTransportResources;

public class ServiceTransports {

  private ServiceTransports() {
    // Do not instantiate
  }

  public static ServiceTransportBootstrap rsocketServiceTransport(ServiceTransportBootstrap opts) {
    return opts.resources(RSocketTransportResources::new)
        .client(RSocketServiceTransport.INSTANCE::clientTransport)
        .server(RSocketServiceTransport.INSTANCE::serverTransport);
  }
}
