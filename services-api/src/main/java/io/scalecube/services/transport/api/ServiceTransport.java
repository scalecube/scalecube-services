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
   * @return client transport
   * @param executorService transport executor service
   */
  ClientTransport getClientTransport(ExecutorService executorService);

  /**
   * Getting server transport.
   *
   * @return server transport
   * @param executorService transport executor service
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
   * @return shutdown signal
   * @param executorService transport executor service
   */
  Mono<Void> shutdown(ExecutorService executorService);
}
