package io.scalecube.services.transport.api;

import io.scalecube.services.ServiceLoaderUtil;
import java.util.concurrent.ExecutorService;
import reactor.core.publisher.Mono;

/** Service transport interface. */
public interface ServiceTransport {

  /**
   * Loads service transport from classpath service loader mechanism.
   *
   * @return instance of service transport
   */
  static ServiceTransport getTransport() {
    return ServiceLoaderUtil.findFirst(ServiceTransport.class)
        .orElseThrow(() -> new IllegalStateException("ServiceTransport not configured"));
  }

  /**
   * Getting client transport.
   *
   * @param executorService transport executor service
   * @return client transport
   */
  ClientTransport getClientTransport(ExecutorService executorService);

  /**
   * Getting server transport.
   *
   * @param executorService transport executor service
   * @return server transport
   */
  ServerTransport getServerTransport(ExecutorService executorService);

  /**
   * Getting new service transport executor service.
   *
   * @return service transport executor service
   */
  ExecutorService getExecutorService();

  /**
   * Shutdowns service transport.
   *
   * @param executorService transport executor service
   * @return shutdown signal
   */
  Mono<Void> shutdown(ExecutorService executorService);
}
