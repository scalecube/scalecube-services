package io.scalecube.services.examples;

import io.scalecube.services.Microservices.ServiceTransportBootstrap;
import io.scalecube.services.transport.rsocket.experimental.builder.RSocketNettyTcp;

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
    return opts.transportProvider(RSocketNettyTcp.builder().build());
  }
}
