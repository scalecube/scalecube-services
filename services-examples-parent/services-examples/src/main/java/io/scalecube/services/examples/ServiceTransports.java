package io.scalecube.services.examples;

import io.scalecube.services.Microservices.ServiceTransportBootstrap;
import io.scalecube.services.transport.rsocket.builder.RSocketByNettyTcp;

public class ServiceTransports {

  public static ServiceTransportBootstrap rsocketServiceTransport(ServiceTransportBootstrap opts) {
    return opts.transportProvider(RSocketByNettyTcp.builder().build());
  }
}
