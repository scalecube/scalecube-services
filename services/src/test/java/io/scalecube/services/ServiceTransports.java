package io.scalecube.services;

import io.scalecube.services.Microservices.ServiceTransportBootstrap;
import io.scalecube.services.transport.rsocket.RSocketServiceTransport;

public class ServiceTransports {

  private ServiceTransports() {
    // Do not instantiate
  }

  public static ServiceTransportBootstrap rsocketServiceTransport(ServiceTransportBootstrap opts) {
    return opts.serviceTransport(RSocketServiceTransport::new);
  }
}
