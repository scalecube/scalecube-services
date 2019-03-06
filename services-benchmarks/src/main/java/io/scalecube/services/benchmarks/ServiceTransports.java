package io.scalecube.services.benchmarks;

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

  public static ServiceTransportBootstrap rsocketServiceTransport(
      ServiceTransportBootstrap opts, int numOfThreads) {
    return opts.resources(() -> new RSocketTransportResources(numOfThreads))
        .client(RSocketServiceTransport.INSTANCE::clientTransport)
        .server(RSocketServiceTransport.INSTANCE::serverTransport);
  }
}
