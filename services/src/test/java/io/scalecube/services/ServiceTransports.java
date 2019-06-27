package io.scalecube.services;

import io.scalecube.services.Microservices.ServiceTransportBootstrap;
import io.scalecube.services.transport.rsocket.experimental.builder.RSocketByNettyTcp;

public class ServiceTransports {

  private ServiceTransports() {
    // Do not instantiate
  }

  public static ServiceTransportBootstrap rsocketServiceTransport(ServiceTransportBootstrap opts) {
    return opts.transportProvider(RSocketByNettyTcp.builder().build());
  }
}
