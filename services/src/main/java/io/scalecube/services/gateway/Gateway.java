package io.scalecube.services.gateway;

import io.scalecube.services.ServiceCall;
import io.scalecube.services.ServiceLoaderUtil;
import io.scalecube.services.metrics.Metrics;
import java.net.InetSocketAddress;
import java.util.concurrent.ExecutorService;
import reactor.core.publisher.Mono;

public interface Gateway {

  /**
   * Get {@link Gateway} instance by type.
   *
   * @param gatewayClass - type of {@link Gateway} to be returned.
   * @return - instance of {@link Gateway} of given <code>gatewayClass</code>.
   */
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
   * @param selectorExecutor selector service transport executor service
   * @param workerExecutor worker service transport executor service
   * @param call service call definition
   * @param metrics @return IP socket address on which gateway is listening to requests
   */
  Mono<InetSocketAddress> start(
      GatewayConfig config,
      ExecutorService selectorExecutor,
      ExecutorService workerExecutor,
      ServiceCall.Call call,
      Metrics metrics);

  /**
   * Stops the gateway.
   *
   * @return stop signal
   */
  Mono<Void> stop();
}
