package io.scalecube.services.transport.api;

import io.scalecube.services.ServiceLoaderUtil;
import reactor.core.publisher.Mono;

import java.util.concurrent.ExecutorService;

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
   */
  ClientTransport getClientTransport();

  /**
   * Getting server transport.
   *
   * @return server transport
   */
  ServerTransport getServerTransport();

  /**
   * Getting service transport executor service.
   *
   * @return service transport executor service
   */
  ExecutorService getExecutorService();

  /**
   * Shutdowns service transport.
   *
   * @return shutdown signal
   */
  Mono<Void> shutdown();
}
