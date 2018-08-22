package io.scalecube.services.transport.api;

import io.scalecube.services.ServiceLoaderUtil;
import java.util.concurrent.Executor;
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
   * @param selectorThreadPool service transport selector thread pool
   * @param workerThreadPool service transport worker thread pool
   * @return client transport
   */
  ClientTransport getClientTransport(Executor selectorThreadPool, Executor workerThreadPool);

  /**
   * Getting server transport.
   *
   * @param selectorThreadPool service transport selector thread pool
   * @param workerThreadPool service transport worker thread pool
   * @return server transport
   */
  ServerTransport getServerTransport(Executor selectorThreadPool, Executor workerThreadPool);

  /**
   * Getting new service transport selector thread pool.
   *
   * @return executor
   */
  Executor getSelectorThreadPool();

  /**
   * Getting new service transport worker thread pool.
   *
   * @return executor
   */
  Executor getWorkerThreadPool(WorkerThreadChooser workerThreadChooser);

  /**
   * Shutdowns service transport.
   *
   * @param selectorThreadPool service transport selector thread pool
   * @param workerThreadPool service transport worker thread pool
   * @return shutdown signal
   */
  Mono<Void> shutdown(Executor selectorThreadPool, Executor workerThreadPool);
}
