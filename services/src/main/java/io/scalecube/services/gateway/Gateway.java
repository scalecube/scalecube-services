package io.scalecube.services.gateway;

import io.scalecube.services.ServiceCall;
import io.scalecube.services.ServiceLoaderUtil;
import io.scalecube.services.metrics.Metrics;
import reactor.core.publisher.Mono;

import java.net.InetSocketAddress;
import java.util.concurrent.ExecutorService;

public interface Gateway {

  static Gateway getGateway(Class<? extends Gateway> gatewayClass) {
    return ServiceLoaderUtil.findFirst(
            Gateway.class, gateway -> gateway.getClass().isAssignableFrom(gatewayClass))
        .orElseThrow(() -> new IllegalStateException("Gateway is not found in classpath"));
  }

  /**
   * Starts the gateway with given configuration. In case some options are not overridden, default
   * values will be used.
   *
   * @param config gateway configuration
   * @param executorService service transport executor service
   * @param call servica call definition
   * @param metrics @return IP socket address on which gateway is listening to requests
   */
  Mono<InetSocketAddress> start(
      GatewayConfig config,
      ExecutorService executorService,
      ServiceCall.Call call,
      Metrics metrics);

  /**
   * Stops the gateway.
   *
   * @return stop signal
   */
  Mono<Void> stop();
}
