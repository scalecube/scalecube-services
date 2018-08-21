package io.scalecube.services.transport.api;

import io.scalecube.services.ServiceLoaderUtil;
import java.util.concurrent.ExecutorService;
import reactor.core.publisher.Mono;

/**
 * Service transport interface.
 */
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
   * @param selectorExecutor transport executor service
   * @param workerExecutor transport executor service
   * @return client transport
   */
  ClientTransport getClientTransport(ExecutorService selectorExecutor,
      ExecutorService workerExecutor);

  /**
   * Getting server transport.
   *
   * @param selectorExecutor transport executor service
   * @param workerExecutor transport executor service
   * @return server transport
   */
  ServerTransport getServerTransport(ExecutorService selectorExecutor,
      ExecutorService workerExecutor);

  /**
   * Getting new selector service transport executor service.
   *
   * @return service transport executor service
   */
  ExecutorService getSelectorExecutor();

  /**
   * Getting new worker service transport executor service.
   *
   * @return service transport executor service
   */
  ExecutorService getWorkerExecutor();

  /**
   * Shutdowns service transport.
   *
   * @param selectorExecutor transport executor service
   * @param workerExecutor transport executor service
   * @return shutdown signal
   */
  Mono<Void> shutdown(ExecutorService selectorExecutor, ExecutorService workerExecutor);
}
