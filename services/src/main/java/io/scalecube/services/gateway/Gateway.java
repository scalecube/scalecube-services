package io.scalecube.services.gateway;

import io.scalecube.services.ServiceCall.Call;
import io.scalecube.services.ServiceLoaderUtil;
import io.scalecube.services.metrics.Metrics;
import java.net.InetSocketAddress;
import java.util.concurrent.Executor;
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
   * @param workerThreadPool worker service transport executor service
   * @param preferNative flag telling should native provider be preferred
   * @param call service call definition
   * @param metrics @return IP socket address on which gateway is listening to requests
   */
  Mono<Gateway> start(
      GatewayConfig config,
      Executor workerThreadPool,
      boolean preferNative,
      Call call,
      Metrics metrics);

  /**
   * Returns Gateway's address if it's started, {@code null} otherwise.
   *
   * @return Mono of listening address of gateway if it's started.
   */
  InetSocketAddress address();

  /**
   * Stops the gateway.
   *
   * @return stop signal
   */
  Mono<Void> stop();
}
