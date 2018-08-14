package io.scalecube.services.gateway;

import io.scalecube.services.ServiceLoaderUtil;

import java.net.InetSocketAddress;

import reactor.core.publisher.Mono;

public interface Gateway {

  static Gateway getGateway(Class<? extends Gateway> gatewayClass) {
    return ServiceLoaderUtil
        .findFirst(Gateway.class, gateway -> gateway.getClass().isAssignableFrom(gatewayClass))
        .orElseThrow(() -> new IllegalStateException("Gateway is not found in classpath"));
  }

  /**
   * Starts the gateway with default configuration.
   *
   * @return IP socket address on which gateway is listening to requests
   */
  Mono<InetSocketAddress> start();

  /**
   * Starts the gateway with given configuration. In case some options are not overridden, default values will be used.
   * 
   * @param config gateway configuration
   * @return IP socket address on which gateway is listening to requests
   */
  Mono<InetSocketAddress> start(GatewayConfig config);

  /**
   * Stops the gateway.
   */
  Mono<Void> stop();

}
