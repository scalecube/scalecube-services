package io.scalecube.services.benchmarks;

import io.scalecube.services.Microservices.ServiceTransportBootstrap;
import io.scalecube.services.transport.rsocket.RSocketServiceTransport;
import io.scalecube.services.transport.rsocket.RSocketTransportResources;

public class ServiceTransports {

  private ServiceTransports() {
    // Do not instantiate
  }

  /**
   * Returns new {@code ServiceTransportBootstrap} object.
   *
   * @param opts options
   * @return new {@code ServiceTransportBootstrap} object
   */
  public static ServiceTransportBootstrap rsocketServiceTransport(ServiceTransportBootstrap opts) {
    return opts.resources(RSocketTransportResources::new)
        .client(RSocketServiceTransport.INSTANCE::clientTransport)
        .server(RSocketServiceTransport.INSTANCE::serverTransport);
  }

  /**
   * Returns new {@code ServiceTransportBootstrap} object.
   *
   * @param opts options
   * @param numOfThreads number of threads in worker pool
   * @return new {@code ServiceTransportBootstrap} object
   */
  public static ServiceTransportBootstrap rsocketServiceTransport(
      ServiceTransportBootstrap opts, int numOfThreads) {
    return opts.resources(() -> new RSocketTransportResources(numOfThreads))
        .client(RSocketServiceTransport.INSTANCE::clientTransport)
        .server(RSocketServiceTransport.INSTANCE::serverTransport);
  }
}
